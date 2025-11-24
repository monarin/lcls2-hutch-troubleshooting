# LCLS-II Hutch Troubleshooting

This repository captures troubleshooting work for LCLS-II hutch analysis stacks (TMO, RIX, XPP, MFX, ...).  Each project keeps the exact scripts that were used on the shared systems together with configuration snapshots and human-readable notes so we can rerun and iterate on fixes without reverse engineering old jobs.

## Repository layout
- `projects/<hutch>-<topic>/scripts/` &mdash; runnable copies of the shell helpers that were used in the field (mirrors of `/sdf/group/lcls/ds/tools/conda_envs/<module>/sh_test`).
- `projects/<hutch>-<topic>/config_test/` &mdash; configuration snapshot matched to those scripts.
- `projects/<hutch>-<topic>/README.md` &mdash; findings, reproduction steps, links to logs and status of each run that was investigated.
- `tools/` &mdash; shared utilities that apply to multiple hutches (e.g. the generalized `run_allocated.sh` MPI launcher/debugger).

## Adding a new troubleshooting record
1. Create a new `projects/<hutch>-<topic>` folder.
2. Mirror the relevant scripts/configs into `scripts/` and `config_test/` (keep logs in the shared filesystem; `.err`/`.out` files are ignored here).
3. Update `scripts/setup_dev.sh` so that `CONFIGDIR` is resolved relative to the repository copy.  All scripts should rely on that `CONFIGDIR` rather than hard-coded shared paths.
4. Document the test recipe in `README.md`.  Include:
   - Original log locations (e.g. `/sdf/group/.../dream_*.err`).
   - Exact commands that reproduce the failure/success.
   - Notes about fixes or environment overrides (e.g. DREAM overrides via `MONA_DREAM_DIR`).
5. Commit the changes so future investigations can reuse the same baseline.

The `scripts/setup_dev.sh` template already contains Mona's DREAM override block so that local edits in `/sdf/scratch/users/m/monarin/dream` automatically shadow the conda environment.  Keep that block intact when copying to new projects.
