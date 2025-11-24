#!/bin/bash
set -euo pipefail

usage() {
cat <<'EOF'
Usage: preproc_dev.sh [options] <experiment> <run>

Options:
  -N, --nodes <num>            Number of nodes to request (default: 1)
  -t, --tasks-per-node <num>   Tasks per node (default: 120)
  -e, --ps-eb-nodes <num>      Value for PS_EB_NODES (default: 1)
  -s, --ps-srv-nodes <num>     Value for PS_SRV_NODES (default: 1)
  -h, --help                   Show this help and exit

Examples:
  ./preproc_dev.sh tmo100864625 25
  ./preproc_dev.sh -N 5 -t 120 -e 15 -s 5 tmo100864625 25
EOF
}

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
if [[ -z "${CONFIGDIR:-}" ]]; then
    CONFIGDIR_RESOLVED=$(readlink -f "$SCRIPT_DIR/../config_test")
    export CONFIGDIR="${CONFIGDIR_RESOLVED%/}/"
else
    export CONFIGDIR="${CONFIGDIR%/}/"
fi
SETUP_SCRIPT="$(readlink -f "$SCRIPT_DIR/setup_dev.sh")"
if [[ ! -f "$SETUP_SCRIPT" ]]; then
    echo "[preproc_dev] Missing setup script at $SETUP_SCRIPT" >&2
    exit 1
fi

NODES=1
TASKS_PER_NODE=120
PS_EB_NODES_VAL=1
PS_SRV_NODES_VAL=1

while [[ $# -gt 0 ]]; do
    case "$1" in
        -N|--nodes)
            NODES="$2"; shift 2 ;;
        -t|--tasks-per-node)
            TASKS_PER_NODE="$2"; shift 2 ;;
        -e|--ps-eb-nodes)
            PS_EB_NODES_VAL="$2"; shift 2 ;;
        -s|--ps-srv-nodes)
            PS_SRV_NODES_VAL="$2"; shift 2 ;;
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

if [[ $# -lt 2 ]]; then
    echo "Error: experiment and run are required." >&2
    usage
    exit 1
fi

EXP="$1"
RUN="$2"

export PS_EB_NODES="$PS_EB_NODES_VAL"
export PS_SRV_NODES="$PS_SRV_NODES_VAL"

ENV_PROBE="${ENV_PROBE:-$SCRIPT_DIR/env_probe.py}"

config_file="$CONFIGDIR/dream/offline.yaml"
log_path1=$(awk '/^log:/{flag=1;next}/^[^[:space:]]/{flag=0}flag && /path1:/{print $2}' "$config_file")
log_path2=$(awk '/^log:/{flag=1;next}/^[^[:space:]]/{flag=0}flag && /path2:/{print $2}' "$config_file")
log_dir="${log_path1%/}/$EXP/${log_path2#/}"

sbatch <<EOT
#!/bin/bash
#SBATCH --output="dream_%j.out"
#SBATCH --error="dream_%j.err"
#SBATCH --partition=milano
#SBATCH --account=lcls:data
#SBATCH --nodes=$NODES
#SBATCH --ntasks-per-node=$TASKS_PER_NODE
#SBATCH --exclusive
##SBATCH --mail-user=monarin@slac.stanford.edu
##SBATCH --mail-type=FAIL

source "$SETUP_SCRIPT"

echo "=== Running env probe before mpirun ==="
echo "Python:   \$CONDA_PREFIX/bin/python"
echo "Probe:    $ENV_PROBE"
\$CONDA_PREFIX/bin/python -u $ENV_PROBE
echo "=== End env probe ==="

mpirun \$CONDA_PREFIX/bin/python -u -m mpi4py.run \$(which dream) --exp=$EXP --run=$RUN
EOT
