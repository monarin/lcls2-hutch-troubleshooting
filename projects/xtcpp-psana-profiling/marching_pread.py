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


def load_offsets(offset_file: Path, include_streams=None):
    if include_streams:
        include_set = set(include_streams)
    else:
        include_set = {"006", "007", "008", "009", "010"}
    with offset_file.open("rb") as f:
        payload = pickle.load(f)
    jungfrau_streams = []
    for fname in payload["files"]:
        parts = fname.split("-")
        if len(parts) < 3:
            continue
        stream_part = parts[2]
        if stream_part.startswith("s") and stream_part[1:] in include_set:
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


def reserve_event_block(win: MPI.Win, comm: MPI.Comm, grant_size: int, limit: int):
    """Atomically reserve a block of events and return (start, end) bounds."""
    one = np.array([grant_size], dtype="i")
    result = np.zeros(1, dtype="i")
    win.Lock(rank=0)
    win.Fetch_and_op(one, result, target_rank=0, op=MPI.SUM)
    win.Unlock(0)
    start = int(result[0])
    if start >= limit:
        return None
    end = min(start + grant_size, limit)
    return start, end


def _stream_chunk_length(stream_offsets: np.ndarray, start_idx: int, end_idx: int) -> int:
    """Compute contiguous byte length required to cover [start_idx, end_idx) events."""
    if start_idx >= end_idx:
        return 0
    first_offset = int(stream_offsets[start_idx, 0])
    last_offset = int(stream_offsets[end_idx - 1, 0])
    last_size = int(stream_offsets[end_idx - 1, 1])
    return (last_offset + last_size) - first_offset


def estimate_chunk_capacities(stream_arrays, events_per_chunk: int):
    """Return per-stream max bytes needed for the configured chunk size."""
    capacities = []
    step = max(1, events_per_chunk)
    for arr in stream_arrays:
        event_count = arr.shape[0]
        max_len = 1
        if event_count > 0:
            cursor = 0
            while cursor < event_count:
                chunk_end = min(cursor + step, event_count)
                chunk_len = _stream_chunk_length(arr, cursor, chunk_end)
                if chunk_len > max_len:
                    max_len = chunk_len
                cursor += step
        capacities.append(max_len)
    return capacities


def populate_chunk(arrays, start_idx: int, end_idx: int, offsets_out, sizes_out) -> int:
    """Fill offsets/sizes for the chunk and return total bytes across all streams."""
    total = 0
    for idx, arr in enumerate(arrays):
        length = _stream_chunk_length(arr, start_idx, end_idx)
        offsets_out[idx] = arr[start_idx, 0] if start_idx < end_idx else 0
        sizes_out[idx] = length
        total += length
    return int(total)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--offsets", required=True)
    parser.add_argument("--xtc-dir", required=True, help="Directory holding the actual XTC2 big-data files")
    parser.add_argument("--max-events", type=int, default=0, help="Optional limit")
    parser.add_argument(
        "--streams",
        nargs="+",
        help="Space-separated stream IDs to include (e.g. 006 007 008). Defaults to Jungfrau s006-s010.",
    )
    parser.add_argument(
        "--events-per-grant",
        type=int,
        default=32,
        help="How many events each MPI Fetch_and_op should reserve (must be >= 1).",
    )
    parser.add_argument(
        "--report-interval",
        type=int,
        default=1000,
        help="Print progress after processing this many events (0 disables).",
    )
    args = parser.parse_args()

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    events_per_grant = max(1, args.events_per_grant)
    report_interval = max(0, args.report_interval)
    if rank == 0:
        print(
            "[Rank 0] marching_pread parameters:\n"
            f"  offsets        : {args.offsets}\n"
            f"  xtc-dir        : {args.xtc_dir}\n"
            f"  max-events     : {args.max_events or 'all'}\n"
            f"  streams        : {', '.join(args.streams) if args.streams else 'default (006-010)'}\n"
            f"  events-per-grant: {events_per_grant}\n"
            f"  report-interval: {report_interval}"
        )

    include_streams = args.streams if args.streams else None
    if rank == 0:
        streams, arrays, n_events = load_offsets(Path(args.offsets), include_streams=include_streams)
        if include_streams:
            print(f"[Rank 0] Reading requested streams: {', '.join(include_streams)}")
        else:
            print("[Rank 0] Reading default streams: 006 007 008 009 010")
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
    if rank == 0:
        print(f"[Rank 0] Planned global events: {n_events} across {num_streams} streams")

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
    if is_node_leader:
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

    chunk_caps = np.ones(num_streams, dtype=np.int64)
    if is_node_leader and num_streams > 0:
        chunk_caps[:] = estimate_chunk_capacities(arrays, events_per_grant)
    shm_comm.Bcast(chunk_caps, root=0)

    # Pre-open all Jungfrau streams for pread
    xtc_dir = Path(args.xtc_dir)
    fds = []
    for fname in streams:
        path = xtc_dir / fname
        fd = os.open(path, os.O_DIRECT)
        fds.append(fd)
        if rank == 0:
            print(f"[Rank 0] Opened {path}")
    fds_arr = np.asarray(fds, dtype=np.intc)
    max_sizes = chunk_caps.tolist()
    preader = ParallelPreader(len(streams), max_sizes)
    offsets_arr = np.zeros(len(streams), dtype=np.int64)
    sizes_arr = np.zeros(len(streams), dtype=np.intp)

    win, _ = atomic_counter_init(shm_comm)

    processed_events = 0
    events_since_report = 0
    loop_start = time.time()
    interval_start = loop_start
    total_bytes = 0
    interval_bytes = 0
    grants_logged = 0
    node_label = f"leader {leader_rank}" if leader_comm else "single-node"
    while True:
        block = reserve_event_block(win, shm_comm, events_per_grant, local_count)
        if block is None:
            break
        block_start, block_end = block
        if block_end <= block_start:
            continue
        block_bytes = populate_chunk(arrays, block_start, block_end, offsets_arr, sizes_arr)
        if grants_logged < 3 and is_node_leader:
            desc = ", ".join(
                f"{streams[i]}:{sizes_arr[i]/(1024*1024):.2f} MiB"
                for i in range(len(streams))
            )
            print(
                f"[Node {node_label} rank {rank}] Grant {grants_logged + 1} "
                f"events[{node_start + block_start},{node_start + block_end}) "
                f"bytes per stream: {desc}"
            )
            grants_logged += 1
        preader.read(fds_arr, offsets_arr, sizes_arr)
        events_in_block = block_end - block_start
        processed_events += events_in_block
        events_since_report += events_in_block
        total_bytes += block_bytes
        interval_bytes += block_bytes
        global_idx = node_start + block_end - 1
        if report_interval > 0 and events_since_report >= report_interval:
            now = time.time()
            interval_time = now - interval_start
            rate = events_since_report / interval_time if interval_time > 0 else 0.0
            io_rate = (interval_bytes / interval_time) / (1024 * 1024) if interval_time > 0 else 0.0
            print(
                f"[Rank {rank}] processed {processed_events} events (last global idx {global_idx}) "
                f"Interval={interval_time:.2f}s Rate={rate:.1f} Hz "
                f"IO={io_rate:.1f} MiB/s"
            )
            interval_start = now
            events_since_report = 0
            interval_bytes = 0

    for fd in fds:
        os.close(fd)
    loop_elapsed = time.time() - loop_start
    win.Free()
    comm.Barrier()
    total = comm.reduce(processed_events, op=MPI.SUM, root=0)
    total_loop = comm.reduce(loop_elapsed, op=MPI.MAX, root=0)
    total_io_bytes = comm.reduce(total_bytes, op=MPI.SUM, root=0)
    offsets_win.Free()
    if rank == 0:
        rate = total / total_loop if total_loop > 0 else 0.0
        io_rate = (total_io_bytes / total_loop) / (1024 * 1024) if total_loop > 0 else 0.0
        print(f"Total processed events = {total} "
              f"Elapsed={total_loop:.2f}s Rate={rate:.1f} Hz "
              f"IO={io_rate:.1f} MiB/s")


if __name__ == "__main__":
    main()
