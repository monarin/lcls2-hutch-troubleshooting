#!/usr/bin/env python3
"""
shared_offsets_demo.py
Run with: mpirun -n <num_ranks> python shared_offsets_demo.py
"""

import numpy as np
from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

# Example configuration
batch_size = 10  # number of offsets in this marching batch

def create_shared_window(comm, n_entries):
    """EB allocates offset table, BD ranks map the same window."""
    entry_dtype = np.int64  # store offsets
    bytes_per_entry = entry_dtype().nbytes
    win = MPI.Win.Allocate(bytes_per_entry * n_entries,
                           bytes_per_entry,
                           comm=comm)
    buf = np.ndarray((n_entries,), dtype=entry_dtype, buffer=win.tomemory())
    return win, buf

def eb_fill_offsets(offset_buf):
    """Only EB rank fills offsets and sets header fields."""
    for i in range(len(offset_buf)):
        offset_buf[i] = 1000 + i * 50  # replace with real offsets

def bd_march(win, offset_buf, comm):
    claimed = np.zeros(1, dtype='i')
    one = np.array(1, dtype='i')
    n_entries = offset_buf.shape[0]
    while True:
        win.Lock(rank=0)  # EB is rank 0
        win.Fetch_and_op(one, claimed, target_rank=0, op=MPI.SUM)
        win.Unlock(0)
        idx = int(claimed[0])
        if idx >= n_entries:
            break
        offset = offset_buf[idx]
        print(f"[Rank {rank}] marching index {idx} offset {offset}")
        # ... do pread using offset here ...

if rank == 0:
    # EB node: allocate shared table and fill it
    win, offsets = create_shared_window(comm, batch_size)
    eb_fill_offsets(offsets)
else:
    # BD nodes: attach to the same window (Allocate already mapped it)
    win, offsets = create_shared_window(comm, batch_size)

comm.Barrier()  # ensure offsets ready
if rank != 0:
    bd_march(win, offsets, comm)

win.Free()
comm.Barrier()
if rank == 0:
    print("All BD ranks finished marching")

