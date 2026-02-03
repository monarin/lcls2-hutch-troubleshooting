#!/bin/bash
#SBATCH -J marching_ds
#SBATCH -A lcls:data
#SBATCH -p milano
#SBATCH --ntasks-per-node=120
#SBATCH --exclusive

# Cannot enable `set -u` because the conda activate/deactivate hooks referenced by
# setup_env.sh expect some env vars to be undefined.
set -eo pipefail

if [ "$#" -ne 4 ]; then
    echo "Usage: $0 <nodes> <exp> <run> <max_events>"
    echo "Example: sbatch -N 2 $0 2 mfx101344525 84 2000"
    exit 1
fi

NODES=$1
EXPERIMENT=$2
RUNNUM=$3
MAX_EVENTS=$4

if [ -n "${SLURM_NNODES:-}" ] && [ "$SLURM_NNODES" -ne "$NODES" ]; then
    echo "Warning: requested nodes ($NODES) differ from allocated nodes ($SLURM_NNODES)" >&2
fi

export PS_SMD_N_EVENTS=20000
export PS_MARCH_SLOTS=10
export PS_MARCH_MAX_EVENTS=25000
export PS_MARCH_EVENTS_PER_GRANT=4
export PS_MARCHING_READ=1
export PS_BD_CHUNKSIZE=32000000

source ~/lcls2/setup_env.sh

TOTAL_RANKS=$((120 * NODES))

mpirun -n "${TOTAL_RANKS}" python -m psana.debugtools.ds_count_events \
    --exp "${EXPERIMENT}" \
    --run "${RUNNUM}" \
    --max_events "${MAX_EVENTS}" \
    --detectors jungfrau
