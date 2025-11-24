#!/bin/bash
set -euo pipefail

SCRIPT_NAME=$(basename "$0")
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

usage() {
cat <<'EOF'
run_allocated.sh — launch MPI workloads inside an existing SLURM allocation with optional gdb/xterm debugging.

Typical workflow:
  1. Request nodes:   salloc -N 5 -n 600 --partition=milano --exclusive
  2. Log onto a node: ssh $(squeue --me -o "%N" -h | head -n1)
  3. Load env:        source <your setup script>   # e.g. setup_dev.sh
  4. Launch job:      DEBUG_MODE=1 tools/run_allocated.sh -n 120 -- dream --exp tmo100864625 --run 25

Usage:
  tools/run_allocated.sh -n <cores> [options] -- <command [args...]>

Options:
  -n, --np <cores>         Total MPI ranks to launch (required).
  -H, --hostfile <path>    Hostfile for mpirun; otherwise SLURM allocation is used.
  -l, --launcher <mode>    auto (default), mpirun, or srun.
  -e, --env-probe <path>   Python script to run before launching (prints environment info).
      --no-env-probe       Skip env probe even if --env-probe/ENV_PROBE is set.
  -x, --export <VAR>       Additional environment variable to export through mpirun (repeatable).
  -h, --help               Show this help.
  --                       Everything after -- is treated as the command to run.

Environment knobs:
  DEBUG_MODE=1             Pops an xterm with gdb attached to the first debug rank.
  DEBUG_RANKS="5 6"        Explicit ranks to debug (defaults to FIRST_WORKER_RANK or PS_EB_NODES or 0).
  FIRST_WORKER_RANK=120    Override automatic worker-rank detection.
  ENV_PROBE=/path/to.py    Default env probe script when -e/--env-probe is omitted.
  RUN_ALLOCATED_WRAP_DREAM  Set to 0 to disable auto-wrapping of the dream CLI.

You must already be inside a SLURM allocation (salloc/srun/queued job) so the launcher
can use the reserved nodes.  For remote allocations, ssh into one of the allocated hosts
before invoking this script.
EOF
}

CORES=""
HOSTFILE=""
LAUNCHER="auto"
ENV_PROBE="${ENV_PROBE:-}"
RUN_ENV_PROBE=1
EXTRA_EXPORTS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        -n|--np)
            CORES="$2"; shift 2 ;;
        -H|--hostfile)
            HOSTFILE="$2"; shift 2 ;;
        -l|--launcher)
            LAUNCHER="$2"; shift 2 ;;
        -e|--env-probe)
            ENV_PROBE="$2"; shift 2 ;;
        --no-env-probe)
            RUN_ENV_PROBE=0; shift ;;
        -x|--export)
            EXTRA_EXPORTS+=("$2"); shift 2 ;;
        -h|--help)
            usage; exit 0 ;;
        --)
            shift; break ;;
        -*)
            echo "Unknown option: $1" >&2
            usage
            exit 1 ;;
        *)
            break ;;
    esac
done

if [[ -z "$CORES" ]]; then
    echo "Error: --np/-n is required." >&2
    usage
    exit 1
fi

if [[ $# -eq 0 ]]; then
    echo "Error: specify a command to run after --." >&2
    usage
    exit 1
fi

if [[ -z "$ENV_PROBE" && -f "$SCRIPT_DIR/env_probe.py" ]]; then
    ENV_PROBE="$SCRIPT_DIR/env_probe.py"
fi

PYTHON_BIN="${PYTHON_BIN:-}"
if [[ -z "$PYTHON_BIN" && -n "${CONDA_PREFIX:-}" ]]; then
    PYTHON_BIN="$CONDA_PREFIX/bin/python"
fi
if [[ -z "$PYTHON_BIN" ]]; then
    PYTHON_BIN=$(command -v python || true)
fi

CMD=( "$@" )

maybe_wrap_dream() {
    local resolved cmd0
    cmd0="${CMD[0]}"
    resolved=$(command -v "$cmd0" 2>/dev/null || true)
    if [[ -z "$resolved" ]]; then
        return
    fi
    if [[ "${RUN_ALLOCATED_WRAP_DREAM:-1}" == "0" ]]; then
        return
    fi
    if [[ "$(basename "$resolved")" == "dream" ]]; then
        if [[ -z "$PYTHON_BIN" ]]; then
            echo "[run_allocated] Cannot wrap dream command; PYTHON_BIN not found." >&2
            return
        fi
        CMD=( "$PYTHON_BIN" -u -m mpi4py.run "$resolved" "${CMD[@]:1}" )
        echo "[run_allocated] Wrapped dream command with '$PYTHON_BIN -m mpi4py.run'."
    fi
}

maybe_wrap_dream

if [[ "$LAUNCHER" != "mpirun" && "$LAUNCHER" != "srun" && "$LAUNCHER" != "auto" ]]; then
    echo "Unsupported launcher: $LAUNCHER (use auto|mpirun|srun)" >&2
    exit 1
fi

AUTO_LAUNCHER="$LAUNCHER"
if [[ "$LAUNCHER" == "auto" ]]; then
    if [[ -n "$HOSTFILE" ]]; then
        AUTO_LAUNCHER="mpirun"
    elif [[ -n "${SLURM_JOBID:-}" ]]; then
        AUTO_LAUNCHER="srun"
    else
        AUTO_LAUNCHER="mpirun"
    fi
fi

MPI_EXPORT_VARS=(PATH LD_LIBRARY_PATH PYTHONPATH CONDA_PREFIX CONFIGDIR PS_EB_NODES PS_SRV_NODES MONA_DREAM_DIR FIRST_WORKER_RANK)
MPI_EXPORTS=()
for var in "${MPI_EXPORT_VARS[@]}"; do
    if [[ -n "${!var:-}" ]]; then
        MPI_EXPORTS+=(-x "$var")
    fi
done
for var in "${EXTRA_EXPORTS[@]}"; do
    MPI_EXPORTS+=(-x "$var")
done

if [[ "$AUTO_LAUNCHER" == "mpirun" ]]; then
    MPI_LAUNCHER=(mpirun -np "$CORES" "${MPI_EXPORTS[@]}")
    if [[ -n "$HOSTFILE" ]]; then
        MPI_LAUNCHER+=(--hostfile "$HOSTFILE")
    fi
elif [[ "$AUTO_LAUNCHER" == "srun" ]]; then
    MPI_LAUNCHER=(srun --mpi=pmix_v3 -n "$CORES")
else
    echo "Internal error: unknown launcher $AUTO_LAUNCHER" >&2
    exit 1
fi

if [[ "$RUN_ENV_PROBE" -eq 1 && -n "$ENV_PROBE" ]]; then
    if [[ -z "$PYTHON_BIN" ]]; then
        echo "[run_allocated] Cannot run env probe; no python interpreter found." >&2
    else
        echo "=== Running env probe before MPI launch ==="
        echo "Python: $PYTHON_BIN"
        echo "Probe:  $ENV_PROBE"
        "$PYTHON_BIN" -u "$ENV_PROBE"
        echo "=== End env probe ==="
    fi
fi

DEBUG_MODE="${DEBUG_MODE:-0}"

run_launcher() {
    if [[ "$DEBUG_MODE" == "1" ]]; then
        DEFAULT_DEBUG_RANK="${FIRST_WORKER_RANK:-${PS_EB_NODES:-0}}"
        DEBUG_RANKS="${DEBUG_RANKS:-$DEFAULT_DEBUG_RANK}"
        if [[ -z "$DEBUG_RANKS" ]]; then
            echo "[run_allocated] DEBUG_MODE=1 but DEBUG_RANKS is empty; falling back to rank 0."
            DEBUG_RANKS=0
        fi
        wrapper_script=$(mktemp "${TMPDIR:-/tmp}/run_allocated_wrap.XXXXXX")
        cat <<'EOF' > "$wrapper_script"
#!/bin/bash
RANK=${OMPI_COMM_WORLD_RANK:-${PMIX_RANK:-0}}
IFS=' ' read -r -a DEBUG_LIST <<< "${RUN_ALLOCATED_DEBUG_RANKS:-}"
CMD=( "$@" )
for dbg in "${DEBUG_LIST[@]}"; do
    if [[ "$RANK" = "$dbg" ]]; then
        export RUN_ALLOCATED_DEBUG_CMD="${CMD[*]}"
        exec xterm -hold -e bash -lc 'set -euo pipefail; CMD=($RUN_ALLOCATED_DEBUG_CMD); gdb -ex run --args "${CMD[@]}"; status=$?; echo "gdb exited with $status"; read -p "Press Enter to close"; exit 0'
    fi
done
exec "${CMD[@]}"
EOF
        chmod +x "$wrapper_script"
        trap 'rm -f "$wrapper_script"' EXIT
        export RUN_ALLOCATED_DEBUG_RANKS="$DEBUG_RANKS"
        echo "[run_allocated] DEBUG_MODE enabled for ranks: $DEBUG_RANKS"
        "${MPI_LAUNCHER[@]}" "$wrapper_script" "${CMD[@]}"
        trap - EXIT
        rm -f "$wrapper_script"
    else
        "${MPI_LAUNCHER[@]}" "${CMD[@]}"
    fi
}

echo "[run_allocated] Launcher: ${MPI_LAUNCHER[*]}"
echo "[run_allocated] Command:  ${CMD[*]}"
run_launcher
