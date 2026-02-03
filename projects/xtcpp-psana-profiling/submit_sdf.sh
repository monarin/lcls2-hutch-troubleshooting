#!/bin/bash
#SBATCH --partition=milano
#SBATCH --account=lcls:data
#SBATCH --job-name=test-psana2
#SBATCH --output=output-%j.txt
#SBATCH --exclusive
#SBATCH --time=10:00

usage() {
  echo "Usage: $0 [sbatch opts] -e <exp> -r <runnum> [--max_events <n>] [--log_level <level>]" >&2
  echo "" >&2
  echo "Sbatch options (script flags):" >&2
  echo "  -N, --nodes <count>          Nodes to request (default: 1 or SLURM allocation)" >&2
  echo "  --task-per-node <count>      Tasks per node (default: 120)" >&2
  echo "  -c, --cores <count>          Total MPI ranks (overrides nodes*task-per-node)" >&2
  echo "" >&2
  echo "ds_count_events options:" >&2
  echo "  -e, --exp <expcode>          Experiment (required)" >&2
  echo "  -r, --runnum <run>           Run number (required)" >&2
  echo "  --max_events <n>             Max events (default: 0)" >&2
  echo "  --log_level <level>          Log level (default: INFO)" >&2
  echo "" >&2
  echo "Example: sbatch -N 2 $0 -e mfx101344525 -r 84 --max_events 2000 --log_level DEBUG" >&2
}

t_start=$(date +%s)

NODES=1
NODES_SET=0
CORES=""
EXP=""
RUNNUM=""
MAX_EVENTS=0
LOG_LEVEL=INFO
DEBUG_DETECTOR=""
PRINT_INTERVAL=50
CALIB=0

while [ $# -gt 0 ]; do
  case "$1" in
    -N|--nodes)
      NODES=$2
      NODES_SET=1
      shift 2
      ;;
    -c|--cores)
      CORES=$2
      shift 2
      ;;
    -e|--exp)
      EXP=$2
      shift 2
      ;;
    -r|--runnum|--run|--runnumber)
      RUNNUM=$2
      shift 2
      ;;
    --max_events)
      MAX_EVENTS=$2
      shift 2
      ;;
    --log_level)
      LOG_LEVEL=$2
      shift 2
      ;;
    --debug_detector)
      DEBUG_DETECTOR=$2
      shift 2
      ;;
    --print_interval)
      PRINT_INTERVAL=$2
      shift 2
      ;;
    --calib)
      CALIB=1
      shift 1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [ $NODES_SET -eq 0 ]; then
  if [ -n "${SLURM_JOB_NUM_NODES:-}" ]; then
    NODES=$SLURM_JOB_NUM_NODES
  elif [ -n "${SLURM_NNODES:-}" ]; then
    NODES=$SLURM_NNODES
  fi
fi

if [ -z "$EXP" ] || [ -z "$RUNNUM" ]; then
  echo "Missing -e/--exp or -r/--runnum." >&2
  usage
  exit 1
fi

if [ -n "${SLURM_JOB_NUM_NODES:-}" ] && [ "$SLURM_JOB_NUM_NODES" -ne "$NODES" ]; then
  echo "Warning: --nodes $NODES differs from SLURM_JOB_NUM_NODES $SLURM_JOB_NUM_NODES" >&2
fi

SLURM_NODES=""
if [ -n "${SLURM_JOB_NODELIST:-}" ]; then
  if command -v scontrol >/dev/null 2>&1; then
    SLURM_NODES=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | paste -sd, -)
  else
    SLURM_NODES=$SLURM_JOB_NODELIST
  fi
fi

if [ -n "$CORES" ]; then
  TOTAL_RANKS=$CORES
else
  if [ -n "${SLURM_NTASKS_PER_NODE:-}" ]; then
    TOTAL_RANKS=$((SLURM_NTASKS_PER_NODE * NODES))
  else
    echo "Missing --cores and SLURM_NTASKS_PER_NODE is not set." >&2
    exit 1
  fi
fi

echo "Running ds_count_events with nodes=${NODES} ranks=${TOTAL_RANKS} exp=${EXP} run=${RUNNUM} max_events=${MAX_EVENTS} log_level=${LOG_LEVEL} debug_detector=${DEBUG_DETECTOR} print_interval=${PRINT_INTERVAL} calib=${CALIB}"

export PS_JF_SHARE_DERIVED=1

mpirun -n "${TOTAL_RANKS}" python -m psana.debugtools.ds_count_events \
  --exp "${EXP}" \
  --run "${RUNNUM}" \
  --max_events "${MAX_EVENTS}" \
  --log_level "${LOG_LEVEL}" \
  --print_interval "${PRINT_INTERVAL}" \
  ${CALIB:+--calib} \
  ${DEBUG_DETECTOR:+--debug_detector "${DEBUG_DETECTOR}"}

t_end=$(date +%s)
elapsed=$((t_end - t_start))
if [ -n "$SLURM_NODES" ]; then
  echo "PSJobCompleted TotalElapsed ${elapsed}s Nodes: ${SLURM_NODES}"
else
  echo "PSJobCompleted TotalElapsed ${elapsed}s"
fi
