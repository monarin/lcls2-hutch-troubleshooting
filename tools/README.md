# Tools

Shared scripts that apply across troubleshooting projects.

## `run_allocated.sh`
- Launch MPI workloads inside a SLURM allocation (obtained via `salloc`/`srun` or a queued job) and optionally attach gdb/xterm to specific ranks (`DEBUG_MODE=1`).
- General purpose: pass any command after `--` (e.g. `dream --exp ...`, `python mytest.py`, etc.).
- Provides helpers for:
  - Auto-picking `mpirun` vs `srun` (or force via `--launcher`).
  - Running a Python environment probe prior to MPI launch (`-e env_probe.py`).
  - Exporting extra env vars across MPI ranks (`-x VAR`).
  - Debug ranks configured via `DEBUG_RANKS`/`FIRST_WORKER_RANK`.
  - Automatically wrapping the `dream` CLI with `python -m mpi4py.run` so gdb attaches to the interpreter (disable via `RUN_ALLOCATED_WRAP_DREAM=0` if undesired).

Quick example:
```bash
salloc -N4 -n480 --partition=milano --exclusive
ssh $(squeue --me -o "%N" -h | head -n1)
source <your setup_dev.sh>
DEBUG_MODE=1 tools/run_allocated.sh -n 120 -- dream --exp tmo100864625 --run 25
```

See `tools/run_allocated.sh --help` for the full option list and instructions.
