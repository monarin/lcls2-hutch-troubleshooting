#!/usr/bin/env python3
"""
marching_pread.py

Prototype that:
  * loads offsets from offsets_mfx101344525_r0070.pkl
  * keeps only Jungfrau streams (s006–s010)
  * assigns event indices atomically via MPI_Fetch_and_op
  * each rank preads the offsets for every stream for its assigned event

Usage:
    mpirun -n 4 python marching_pread.py \
        --offsets offsets_mfx101344525_r0070.pkl \
        --xtc-dir /sdf/data/lcls/ds/mfx/mfx101344525/xtc
"""

import argparse
import os
import pickle
import time
from pathlib import Path

import numpy as np
from mpi4py import MPI

from parallel_pread import ParallelPreader


def load_offsets(offset_file: Path):
    with offset_file.open("rb") as f:
        payload = pickle.load(f)
    target_streams = {"006", "007", "008", "009", "010"}
    jungfrau_streams = []
    for fname in payload["files"]:
        parts = fname.split("-")
        if len(parts) < 3:
            continue
        stream_part = parts[2]
        if stream_part.startswith("s") and stream_part[1:] in target_streams:
            jungfrau_streams.append(fname)
    jungfrau_streams.sort()
    if not jungfrau_streams:
        raise ValueError("No Jungfrau streams (s006-s010) found in offsets file")
    arrays = [payload["files"][fname] for fname in jungfrau_streams]
    # Sanity: assume every stream has same #events
    n_events = arrays[0].shape[0]
    for arr in arrays[1:]:
        if arr.shape[0] != n_events:
            raise ValueError("Streams have different lengths; cannot march one-to-one.")
    return jungfrau_streams, arrays, n_events


def atomic_counter_init(comm: MPI.Comm):
    """Create an MPI window holding a single integer counter."""
    int_size = MPI.INT.Get_size()
    win = MPI.Win.Allocate(int_size, int_size, comm=comm)
    buf = np.ndarray(shape=(1,), dtype="i", buffer=win.tomemory())
    buf[0] = 0
    comm.Barrier()
    return win, buf


def fetch_next_index(win: MPI.Win, comm: MPI.Comm) -> int:
    one = np.array([1], dtype="i")
    result = np.zeros(1, dtype="i")
    win.Lock(rank=0)
    win.Fetch_and_op(one, result, target_rank=0, op=MPI.SUM)
    win.Unlock(0)
    return int(result[0])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--offsets", required=True)
    parser.add_argument("--xtc-dir", required=True, help="Directory holding the actual XTC2 big-data files")
    parser.add_argument("--max-events", type=int, default=0, help="Optional limit")
    args = parser.parse_args()

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    if rank == 0:
        streams, arrays, n_events = load_offsets(Path(args.offsets))
        stacked = np.stack(arrays, axis=0).astype(np.int64)
    else:
        streams = None
        n_events = None
        stacked = None

    streams = comm.bcast(streams, root=0)
    n_events = comm.bcast(n_events, root=0)
    if args.max_events:
        n_events = min(n_events, args.max_events)

    num_streams = len(streams)
    stacked_shape = stacked.shape if rank == 0 else None
    stacked_shape = comm.bcast(stacked_shape, root=0)

    # Create shared windows per node and broadcast stacked offsets to node leaders.
    shm_comm = comm.Split_type(MPI.COMM_TYPE_SHARED, rank, MPI.INFO_NULL)
    node_rank = shm_comm.Get_rank()
    is_node_leader = node_rank == 0
    leader_comm = comm.Split(0 if is_node_leader else MPI.UNDEFINED, rank)
    leader_rank = leader_comm.Get_rank() if leader_comm else None
    num_nodes = leader_comm.Get_size() if leader_comm else 1

    if leader_comm:
        node_start = (leader_rank * n_events) // num_nodes
        node_end = ((leader_rank + 1) * n_events) // num_nodes
    else:
        node_start = 0
        node_end = n_events
    local_bounds = np.array([node_start, node_end], dtype=np.int64)
    shm_comm.Bcast(local_bounds, root=0)
    node_start, node_end = map(int, local_bounds)
    local_count = node_end - node_start
    local_shape = (num_streams, local_count, stacked.shape[2]) if rank == 0 else None
    local_shape = comm.bcast(local_shape, root=0)
    print(f'[Rank {rank}] node_rank={node_rank} is_node_leader={is_node_leader} local_count={local_count} ')

    if rank == 0:
        slices = [stacked[:, (i * n_events) // num_nodes: ((i + 1) * n_events) // num_nodes, :]
                  for i in range(num_nodes)]
    else:
        slices = None

    if leader_comm:
        if rank == 0:
            leader_comm.scatter(slices, root=0)
            local_offsets = slices[leader_rank]
        else:
            local_offsets = leader_comm.scatter(None, root=0)
    else:
        local_offsets = stacked

    itemsize = np.int64().nbytes
    local_bytes = int(np.prod(local_shape) * itemsize) if is_node_leader else 0
    offsets_win = MPI.Win.Allocate_shared(local_bytes,
                                          itemsize if local_bytes else itemsize,
                                          comm=shm_comm)
    buf, _ = offsets_win.Shared_query(0)
    shared_offsets = np.ndarray(buffer=buf,
                                dtype=np.int64,
                                shape=local_shape)

    if is_node_leader:
        shared_offsets[:] = local_offsets
    shm_comm.Barrier()

    arrays = [shared_offsets[i] for i in range(num_streams)]

    # Pre-open all Jungfrau streams for pread
    xtc_dir = Path(args.xtc_dir)
    fds = []
    for fname in streams:
        path = xtc_dir / fname
        fd = os.open(path, os.O_RDONLY)
        fds.append(fd)
        if rank == 0:
            print(f"[Rank 0] Opened {path}")
    fds_arr = np.asarray(fds, dtype=np.intc)
    max_sizes = [int(arr[:, 1].max()) for arr in arrays]
    preader = ParallelPreader(len(streams), max_sizes)
    offsets_arr = np.zeros(len(streams), dtype=np.int64)
    sizes_arr = np.zeros(len(streams), dtype=np.intp)

    win, counter_buf = atomic_counter_init(shm_comm)

    processed = 0
    interval = 1000
    loop_start = time.time()
    interval_start = loop_start
    while True:
        idx_local = fetch_next_index(win, shm_comm)
        if idx_local >= local_count:
            break
        for i, arr in enumerate(arrays):
            offsets_arr[i] = arr[idx_local, 0]
            sizes_arr[i] = arr[idx_local, 1]
        preader.read(fds_arr, offsets_arr, sizes_arr)
        processed += 1
        global_idx = idx_local + node_start
        if processed and processed % interval == 0:
            now = time.time()
            interval_time = now - interval_start
            rate = interval / interval_time if interval_time > 0 else 0.0
            print(
                f"[Rank {rank}] processed {processed} events (last global idx {global_idx}) "
                f"Interval={interval_time:.2f}s Rate={rate:.1f} Hz"
            )
            interval_start = now

    for fd in fds:
        os.close(fd)
    loop_elapsed = time.time() - loop_start
    win.Free()
    comm.Barrier()
    total = comm.reduce(processed, op=MPI.SUM, root=0)
    total_loop = comm.reduce(loop_elapsed, op=MPI.MAX, root=0)
    offsets_win.Free()
    if rank == 0:
        rate = total / total_loop if total_loop > 0 else 0.0
        print(f"Total processed events = {total} "
              f"Elapsed={total_loop:.2f}s Rate={rate:.1f} Hz")


if __name__ == "__main__":
    main()
