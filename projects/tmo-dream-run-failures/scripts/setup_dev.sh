unset LD_LIBRARY_PATH
unset PYTHONPATH

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CONFIGDIR_PATH=$(readlink -f "$SCRIPT_DIR/../config_test")
export CONFIGDIR="${CONFIGDIR_PATH%/}/"

if [ -d "/sdf/group/lcls/" ]
then
    # for s3df
    source /sdf/group/lcls/ds/ana/sw/conda2-v4/inst/etc/profile.d/conda.sh
    export CONDA_ENVS_DIRS="/sdf/group/lcls/ds/tools/conda_envs"
    export DIR_PSDM=/sdf/group/lcls/ds/ana/
    export SIT_PSDM_DATA=/sdf/data/lcls/ds/
    # choose specific psana release here:
    export LCLS2_RELEASE=/sdf/group/lcls/ds/ana/sw/conda2/rel/lcls2_112025

    PATH_PS=/sdf/group/lcls/ds/ana/sw/conda2/inst/envs/ps_20241122
    conda activate dream

    # Inject chosen LCLS2 release into PATH + PYTHONPATH
    export PATH=$LCLS2_RELEASE/install/bin:${PATH}
    pyver=$(python -c "import sys; print(str(sys.version_info.major)+'.'+str(sys.version_info.minor))")
    export PYTHONPATH=$LCLS2_RELEASE/install/lib/python$pyver/site-packages
    SITE_A=$PATH_PS/lib/python3.9/site-packages
    export PYTHONPATH="$SITE_A${PYTHONPATH:+:$PYTHONPATH}"
else
    # for psana
    source /cds/sw/ds/ana/conda2-v4/inst/etc/profile.d/conda.sh
    export CONDA_ENVS_DIRS=/cds/sw/ds/ana/conda2/inst/envs/
    export DIR_PSDM=/cds/group/psdm
    export SIT_PSDM_DATA=/cds/data/psdm
    export SUBMODULEDIR=/cds/sw/ds/ana/conda2/rel/lcls2_submodules_03172025
    conda activate daq_20250402
fi

AUTH_FILE=$DIR_PSDM"/sw/conda2/auth.sh"
if [ -f "$AUTH_FILE" ]; then
    source $AUTH_FILE
else
  echo "$AUTH_FILE file is missing"
fi

export CUDA_ROOT=/usr/local/cuda
if [ -h "$CUDA_ROOT" ]; then
    export PATH=${CUDA_ROOT}/bin${PATH:+:${PATH}}
    #export LD_LIBRARY_PATH=${CUDA_ROOT}/lib64${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}
    export MANPATH=${CUDA_ROOT}/man${MANPATH:+:${MANPATH}}
    export NVCC_PREPEND_FLAGS='-ccbin '${CC} # Ensure the correct host compiler is used with nvcc
fi

export PATH=${PATH}:$PATH_PS/epics/bin/linux-x86_64:$PATH_PS/bin
RELDIR="$( cd "$( dirname "$(readlink -f "/sdf/group/lcls/ds/ana/sw/conda2/manage/bin/psconda.sh")" )" && pwd )"
export PATH=$RELDIR/install/bin:${PATH}
pyver=$(python -c "import sys; print(str(sys.version_info.major)+'.'+str(sys.version_info.minor))")
#export PYTHONPATH=$RELDIR/install/lib/python$pyver/site-packages
#SITE_A=$PATH_PS/lib/python3.9/site-packages
#export PYTHONPATH="$SITE_A${PYTHONPATH:+:$PYTHONPATH}"
#SITE_A=$PATH_PS/lib/python3.9/site-packages
#export PYTHONPATH="$SITE_A${PYTHONPATH:+:$PYTHONPATH}"

# --- Mona's local DREAM override ---------------------------------------
export MONA_DREAM_DIR=/sdf/scratch/users/m/monarin/dream
export PYTHONPATH=$MONA_DREAM_DIR${PYTHONPATH:+:$PYTHONPATH}
echo "[setup_dev] Using Mona's DREAM from $MONA_DREAM_DIR"
# ------------------------------------------------------------------------

# for procmgr
export TESTRELDIR=$RELDIR/install

export PROCMGR_EXPORT=RDMAV_FORK_SAFE=1,RDMAV_HUGEPAGES_SAFE=1  # See fi_verbs man page regarding fork()
export PROCMGR_EXPORT=$PROCMGR_EXPORT,OPENBLAS_NUM_THREADS=1,PS_PARALLEL='none'

# for daqbatch
export DAQMGR_EXPORT=RDMAV_FORK_SAFE=1,RDMAV_HUGEPAGES_SAFE=1  # See fi_verbs man page regarding fork()
export DAQMGR_EXPORT=$DAQMGR_EXPORT,OPENBLAS_NUM_THREADS=1,PS_PARALLEL='none'

# cpo: seems that in more recent versions blas is creating many threads
export OPENBLAS_NUM_THREADS=1
# cpo: getting intermittent file-locking issue on ffb, so try this
export HDF5_USE_FILE_LOCKING=FALSE
# for libfabric. decreases performance a little, but allows forking
export RDMAV_FORK_SAFE=1
export RDMAV_HUGEPAGES_SAFE=1

# needed by JupyterLab
export JUPYTERLAB_WORKSPACES_DIR=${HOME}

# cpo: workaround a qt bug which may no longer be there (dec 5, 2022)
if [ ! -d /usr/share/X11/xkb ]; then
    export QT_XKB_CONFIG_ROOT=${CONDA_PREFIX}/lib
fi

# needed by Ric to get correct libfabric man pages
export MANPATH=$CONDA_PREFIX/share/man${MANPATH:+:${MANPATH}}
