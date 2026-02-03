#!/usr/bin/env python3
"""
MPI driver that wires the range planner and node fetcher together using
shared-memory buffers so only node leaders touch the filesystem.
"""

import argparse
import time
from pathlib import Path

import numpy as np
from mpi4py import MPI

from node_fetcher import NodeFetcher
from range_planner import assign_windows_to_nodes, build_event_windows, load_offsets
from shared_slot_buffer import SharedSlotManager


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="MPI driver for staged xtc reads.")
    parser.add_argument("--offsets", required=True, help="Offsets pickle path.")
    parser.add_argument("--xtc-dir", required=True, help="Directory holding xtc files.")
    parser.add_argument("--nodes", type=int, required=True, help="Total node count to plan for.")
    parser.add_argument("--max-stream-bytes", type=int, default=32 * 1024 * 1024)
    parser.add_argument("--max-events", type=int, default=0, help="Max events per window (0 disables).")
    parser.add_argument("--strategy", choices=["round-robin", "contiguous"], default="contiguous")
    parser.add_argument("--num-slots", type=int, default=64, help="Slots in the shared ring per node.")
    parser.add_argument(
        "--sample-records",
        type=int,
        default=5,
        help="Number of records each non-leader rank should print (0 disables).",
    )
    parser.add_argument(
        "--include-stream",
        action="append",
        help="Restrict to selected stream IDs (defaults to Jungfrau s006-s010).",
    )
    return parser.parse_args()


def drain_consumer(
    rank: int,
    consumer,
    sample_records: int,
    expected_windows: int,
    primary_stream: str,
) -> int:
    printed = 0
    consumed_events = 0
    interval_target = 1000
    last_report = time.time()
    last_milestone = 0
    while True:
        record = consumer.consume_next()
        if record is None:
            break
        if primary_stream is None or record.stream == primary_stream:
            event_span = record.end_event - record.start_event
            consumed_events += event_span
            if consumed_events // interval_target > last_milestone:
                now = time.time()
                interval = now - last_report
                events_since_last = consumed_events - (last_milestone * interval_target)
                rate = events_since_last / interval if interval > 0 else 0.0
                print(
                    f"[Rank {rank}] Consumed {consumed_events} events "
                    f"(interval {interval:.2f}s Rate={rate:.1f} Hz)"
                )
                last_report = now
                last_milestone = consumed_events // interval_target
        if sample_records and printed < sample_records:
            print(
                f"[Rank {rank}] stream={record.stream} window={record.window_index} "
                f"events[{record.start_event},{record.end_event}) offset={record.offset} len={record.length}"
            )
            printed += 1
    if sample_records and printed == 0 and expected_windows == 0:
        print(f"[Rank {rank}] no work assigned on this node.")
    return consumed_events


def main() -> None:
    start_time = time.time()
    args = parse_args()
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    shm_comm = comm.Split_type(MPI.COMM_TYPE_SHARED, 0, MPI.INFO_NULL)
    node_rank = shm_comm.Get_rank()
    is_node_leader = node_rank == 0
    leader_comm = comm.Split(0 if is_node_leader else MPI.UNDEFINED, 0)
    num_leaders = comm.allreduce(1 if is_node_leader else 0, op=MPI.SUM)
    if rank == 0:
        plan_nodes = args.nodes
        if plan_nodes != num_leaders:
            print(
                f"[Rank 0] Requested --nodes={plan_nodes} but detected {num_leaders} node leaders; "
                f"planning for {num_leaders}."
            )
            plan_nodes = num_leaders
    else:
        plan_nodes = None
    plan_nodes = comm.bcast(plan_nodes, root=0)

    include = args.include_stream or ["007", "008", "009", "010", "011"]
    if rank == 0:
        streams, stacked = load_offsets(Path(args.offsets), include)
        max_events = args.max_events if args.max_events > 0 else None
        windows = build_event_windows(streams, stacked, args.max_stream_bytes, max_events=max_events)
        assignments = assign_windows_to_nodes(windows, plan_nodes, args.strategy)
        windows_payload = [assignment.windows for assignment in assignments]
    else:
        streams = None
        windows_payload = None
    # Broadcast stream names to everyone.
    streams = comm.bcast(streams, root=0)

    node_windows = None
    if is_node_leader:
        node_windows = leader_comm.scatter(windows_payload, root=0)
    # Share workload size with intra-node ranks.
    workload_counts = np.array([len(node_windows) if node_windows else 0], dtype=np.int64)
    shm_comm.Bcast(workload_counts, root=0)
    print(f'[Rank {rank}] assigned {workload_counts[0]} windows')

    manager = SharedSlotManager(
        shm_comm=shm_comm,
        stream_names=streams,
        max_payload=args.max_stream_bytes,
        num_slots=args.num_slots,
    )

    consumer = manager.create_consumer()
    print(f'[Rank {rank}] created consumer with {manager.num_slots} slots')

    local_consumed = 0
    local_bytes = 0
    window_stats = np.zeros(2, dtype=np.float64)
    if node_windows:
        window_stats[0] = sum(
            chunk.length for window in node_windows for chunk in window.stream_chunks
        )
        window_stats[1] = sum(window.end_event - window.start_event for window in node_windows)
    # Share window bytes/events info within the node.
    shm_comm.Bcast(window_stats, root=0)
    if is_node_leader:
        producer = manager.create_producer()
        fetcher = NodeFetcher(Path(args.xtc_dir), node_windows, buffer_bytes=args.max_stream_bytes, buffer=producer)
        print(f'[Rank {rank}] created producer and fetcher with {len(node_windows)} windows')
        fetcher.start()
        fetcher.wait_for_completion()
        local_bytes = window_stats[0]
    else:
        produced = workload_counts[0]
        print(f'[Rank {rank}] waiting for {produced} windows to be produced')
        primary_stream = streams[0] if streams else None
        local_consumed = drain_consumer(rank, consumer, args.sample_records, produced, primary_stream)
        if window_stats[1] > 0:
            bytes_per_event = window_stats[0] / window_stats[1]
            local_bytes = local_consumed * bytes_per_event
    comm.Barrier()

    manager.close()
    comm.Barrier()

    elapsed_local = time.time() - start_time
    total_events = comm.reduce(local_consumed, op=MPI.SUM, root=0)
    total_bytes = comm.reduce(local_bytes, op=MPI.SUM, root=0)
    max_elapsed = comm.reduce(elapsed_local, op=MPI.MAX, root=0)
    if rank == 0:
        overall_rate = total_events / max_elapsed if max_elapsed > 0 else 0.0
        io_rate = (total_bytes / max_elapsed) / (1024 * 1024) if max_elapsed > 0 else 0.0
        print(
            f"[Prefetch Summary] Total events={total_events} "
            f"Elapsed={max_elapsed:.2f}s Rate={overall_rate:.1f} Hz "
            f"IO={io_rate:.1f} MiB/s"
        )


if __name__ == "__main__":
    main()
