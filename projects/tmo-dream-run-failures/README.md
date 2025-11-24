# TMO DREAM run failures (tmo100864625)

Tracking the failed TMO DREAM production runs so that we can replay them locally, patch DREAM, and record what resolved each issue.

## Files in this project
- `scripts/` &mdash; exact copy of `/sdf/group/lcls/ds/tools/conda_envs/dream/sh_test` minus the `.err/.out` artifacts.  `setup_dev.sh` now resolves `CONFIGDIR` relative to this repository and injects Mona's local DREAM checkout (`/sdf/scratch/users/m/monarin/dream`) via `MONA_DREAM_DIR`.
- `config_test/` &mdash; snapshot of `/sdf/group/lcls/ds/tools/conda_envs/dream/config_test`.  `ENV_PROBE` defaults to the local `env_probe.py` so we capture the environment that actually ran.
- Large smalldata helpers such as `smd_r0030_s003.txt` stay on SDF (`/sdf/group/lcls/ds/tools/conda_envs/dream/sh_test/`) to keep the GitHub repo lightweight; bring them over manually if you need to mirror a specific shard.

Source logs remain on SDF under `/sdf/group/lcls/ds/tools/conda_envs/dream/sh_test/dream_*.err` so we do not version large `.err/.out` files here.

## Known run status
- **Run 25** (job `dream_15926238.err`): aborts inside `XtcData::XtcIterator::iterate` when iterating over the smalldata chunks.  Still reproducible using the commands below.
- **Run 30** (job `dream_15919146.err` and related attempts): segfaults in `numpy.core._multiarray_umath` when DREAM tries to build oversized arrays.  DREAM developer reproduced and mitigated it by reducing numpy array sizes in the processing modules.
- **Run 40** (job `dream_15924094.err`): completes successfully; `.err` file is empty.

## Reproduction recipe
```bash
cd ~/lcls2-hutch-troubleshooting/projects/tmo-dream-run-failures/scripts  # mirrors sh_test
source setup_dev.sh
./preproc_dev.sh tmo100864625 25
```
- `setup_dev.sh` activates the shared `dream` conda env, injects the chosen LCLS2 release, and prepends Mona's local DREAM checkout for hot fixes.
- `preproc_dev.sh` reads logging directories from `config_test/dream/offline.yaml`, runs `env_probe.py` for visibility, and submits the batch job (`dream_<jobid>.err/.out` land in the `scripts/` directory on SDF).
- For interactive debugging on an existing SLURM allocation, use `../../tools/run_allocated.sh`:
  ```bash
  salloc -N5 -n600 --partition=milano --exclusive
  ssh $FIRST_NODE_FROM_ALLOCATION
  source setup_dev.sh
  DEBUG_MODE=1 ../../tools/run_allocated.sh -n 120 -- dream --exp tmo100864625 --run 25
  ```
  It auto-detects whether to use `mpirun` or `srun`, auto-wraps the `dream` CLI with `python -m mpi4py.run` so gdb can attach to the interpreter, can run an env probe (`-e env_probe.py`), and pops an xterm with gdb on the first worker rank when `DEBUG_MODE=1`.

## Next steps / notes
- When tweaking DREAM locally, keep edits in `/sdf/scratch/users/m/monarin/dream` so the override block in `setup_dev.sh` picks them up automatically.
- If run 25 is still failing in `XtcIterator`, retest after pulling the latest LCLS2 release or instrumenting `XtcIterator` locally via the same override.
- Run 30 crash signatures are preserved in the referenced `.err` files should we need to validate the numpy fix; rerunning with smaller array allocations should now succeed.
