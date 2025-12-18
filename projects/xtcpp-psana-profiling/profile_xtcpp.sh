#!/usr/bin/env bash
set -euo pipefail
PROFILE_OUT=${PROFILE_OUT:-/tmp/xtcpp_calib.prof}
export PROFILE_OUT
#ARGS=("-e" "mfx101344525" "-r" "70" "--calib" "--max_events" "100")
ARGS=("-e" "mfx101344525" "-r" "70")
if [ "$#" -gt 0 ]; then
  ARGS=("$@")
fi
set +u
source ~/lcls2/setup_env.sh
[ -f "$HOME/goodstuffs/bashrc" ] && . "$HOME/goodstuffs/bashrc"
set -u
activate_xtcpp
cd ~/sw/xtcpp
mpirun -n 2 python -m cProfile -o "$PROFILE_OUT" examples/test_pytiming.py "${ARGS[@]}"
python3 - <<'PY'
import os, pstats
path = os.environ.get('PROFILE_OUT', '/tmp/xtcpp_calib.prof')
print(f"\nTop cumulative functions for {path}:\n")
pstats.Stats(path).sort_stats('cumulative').print_stats(20)
PY
