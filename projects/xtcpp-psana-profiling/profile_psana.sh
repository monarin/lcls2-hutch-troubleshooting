#!/usr/bin/env bash
set -euo pipefail

if [ $# -lt 1 ]; then
  echo "Usage: $0 <mode> [num_ranks] [log_dir]" >&2
  echo "  mode=1 -> profile iterate_psana.py (requires num_ranks)" >&2
  echo "  mode=2 -> run psana.debugtools.ds_count_events (default num_ranks=3)" >&2
  exit 1
fi

RUN_MODE=$1
shift

case "$RUN_MODE" in
  1)
    if [ $# -lt 1 ]; then
      echo "Mode 1 requires <num_ranks> [log_dir]" >&2
      exit 1
    fi
    N_RANKS=$1
    shift
    ;;
  2)
    N_RANKS=${1:-3}
    if [ $# -gt 0 ]; then
      shift
    fi
    ;;
  *)
    echo "Invalid mode: $RUN_MODE (expected 1 or 2)" >&2
    exit 1
    ;;
esac

LOG_DIR=${1:-log_run}
shift || true
mkdir -p "$LOG_DIR"
export LOG_DIR
OUT_STEM=profile

set +u
source ~/lcls2/setup_env.sh
set -u
cd ~/sw/xtcpp
export OMPI_MCA_osc=sm

case "$RUN_MODE" in
  1)
    ITER_CMD='python -m cProfile -o "$out_prof" \
      examples/iterate_psana.py \
      -e mfx101344525 -r 70 --max_events 1000 \
      --use_calib_cache --batch_size 10'
    export ITER_CMD

    mpirun -n "$N_RANKS" bash -c '
    rank=${OMPI_COMM_WORLD_RANK:-0}
    out_log="'"$LOG_DIR"'/'"$OUT_STEM"'.rank${rank}.log"
    out_prof="'"$LOG_DIR"'/'"$OUT_STEM"'.rank${rank}.prof"
    eval "$ITER_CMD"
    '
    ;;
  2)
    DS_COUNT_SCRIPT=$(python - <<'PY'
import inspect
import os
import psana.debugtools.ds_count_events as mod

path = inspect.getsourcefile(mod) or mod.__file__
print(os.path.abspath(path))
PY
)
    ITER_CMD='python -m cProfile -o "$out_prof" '"$DS_COUNT_SCRIPT"' \
      --exp mfx101344525 --run 70 --batch_size 1 \
      --use_calib_cache --max_events 0 --detectors jungfrau'
    export ITER_CMD

    mpirun -n "$N_RANKS" bash -c '
    rank=${OMPI_COMM_WORLD_RANK:-0}
    out_log="'"$LOG_DIR"'/'"$OUT_STEM"'.rank${rank}.log"
    out_prof="'"$LOG_DIR"'/'"$OUT_STEM"'.rank${rank}.prof"
    eval "$ITER_CMD"
    '
    ;;
esac
