#!/usr/bin/env python3
"""
Prototype range planner for sequential Weka reads.

Given a pickle of offsets (same format used by marching_pread.py), this script
groups events into contiguous windows so that each xtc file can be read in large
sequential chunks.  The resulting windows are assigned to logical nodes, which
will later be translated into actual MPI ranks or hosts.
"""

import argparse
import math
import pickle
from pathlib import Path
from typing import Iterable, List, NamedTuple, Optional, Sequence, Tuple

import numpy as np


class StreamChunk(NamedTuple):
    """Description of the contiguous bytes that must be read from one stream."""

    stream: str
    offset: int
    length: int


class EventWindow(NamedTuple):
    """Events that can be served after one sequential read per stream."""

    start_event: int
    end_event: int  # exclusive
    stream_chunks: Sequence[StreamChunk]

    @property
    def event_count(self) -> int:
        return self.end_event - self.start_event

    @property
    def max_stream_bytes(self) -> int:
        return max(chunk.length for chunk in self.stream_chunks)

    @property
    def total_bytes(self) -> int:
        return sum(chunk.length for chunk in self.stream_chunks)


class NodeAssignment(object):
    def __init__(self, node_id: int):
        self.node_id = node_id
        self.windows: List[EventWindow] = []

    @property
    def total_events(self) -> int:
        return sum(window.event_count for window in self.windows)

    @property
    def total_bytes(self) -> int:
        return sum(window.total_bytes for window in self.windows)

    @property
    def peak_stream_bytes(self) -> int:
        peaks = [window.max_stream_bytes for window in self.windows]
        return max(peaks) if peaks else 0


def load_offsets(offset_file: Path, include_streams: Iterable[str] = None) -> Tuple[List[str], np.ndarray]:
    """Return the selected stream names and stacked offsets array."""
    include_set = {stream for stream in include_streams} if include_streams else None
    with offset_file.open("rb") as handle:
        payload = pickle.load(handle)
    jungfrau_streams: List[str] = []
    for fname in sorted(payload["files"]):
        parts = fname.split("-")
        if len(parts) < 3:
            continue
        stream_part = parts[2]
        if not stream_part.startswith("s"):
            continue
        stream_id = stream_part[1:]
        if include_set is None or stream_id in include_set:
            jungfrau_streams.append(fname)
    if not jungfrau_streams:
        raise ValueError("No streams matched the provided filters.")
    arrays = [payload["files"][fname] for fname in jungfrau_streams]
    n_events = arrays[0].shape[0]
    for arr in arrays[1:]:
        if arr.shape[0] != n_events:
            raise ValueError("Streams have mismatched lengths; cannot build windows.")
    stacked = np.stack(arrays, axis=0).astype(np.int64)
    return jungfrau_streams, stacked


def _chunk_from_stream(stream: str, arr: np.ndarray, start_evt: int, end_evt: int) -> StreamChunk:
    start_offset = int(arr[start_evt, 0])
    last_offset = int(arr[end_evt, 0])
    last_size = int(arr[end_evt, 1])
    length = last_offset + last_size - start_offset
    return StreamChunk(stream=stream, offset=start_offset, length=length)


def build_event_windows(
    streams: Sequence[str],
    stacked_offsets: np.ndarray,
    max_stream_bytes: int,
    max_events: Optional[int] = None,
) -> List[EventWindow]:
    """
    Group events into windows bounded by per-stream byte limits.

    Each window spans the same start/end events across all streams so that the
    consumer side can keep the Jungfrau packets aligned.
    """
    num_streams, n_events, _ = stacked_offsets.shape
    windows: List[EventWindow] = []
    start = 0
    while start < n_events:
        end = start
        accepted_chunks: Optional[Sequence[StreamChunk]] = None
        while end < n_events:
            candidate_end = end + 1
            candidate_chunks = []
            exceeds_limit = False
            for stream_idx in range(num_streams):
                chunk = _chunk_from_stream(
                    streams[stream_idx], stacked_offsets[stream_idx], start, candidate_end - 1
                )
                candidate_chunks.append(chunk)
                if max_stream_bytes and chunk.length > max_stream_bytes:
                    exceeds_limit = True
            if max_events and (candidate_end - start) > max_events:
                exceeds_limit = True
            if exceeds_limit and accepted_chunks is not None:
                break
            accepted_chunks = candidate_chunks
            end = candidate_end
            if exceeds_limit:
                break
        if accepted_chunks is None:
            raise RuntimeError(
                f"Unable to build a window starting at event {start}; "
                "consider increasing --max-stream-bytes."
            )
        windows.append(EventWindow(start_event=start, end_event=end, stream_chunks=accepted_chunks))
        start = end
    return windows


def assign_windows_to_nodes(
    windows: Sequence[EventWindow],
    num_nodes: int,
    strategy: str = "round-robin",
) -> List[NodeAssignment]:
    if num_nodes <= 0:
        raise ValueError("num_nodes must be positive.")
    assignments = [NodeAssignment(node_id=i) for i in range(num_nodes)]
    if strategy == "round-robin":
        for idx, window in enumerate(windows):
            assignments[idx % num_nodes].windows.append(window)
    elif strategy == "contiguous":
        chunk = math.ceil(len(windows) / num_nodes)
        cursor = 0
        for assignment in assignments:
            assignment.windows.extend(windows[cursor : cursor + chunk])
            cursor += chunk
    else:
        raise ValueError(f"Unknown strategy '{strategy}'.")
    return assignments


def human_readable_bytes(value: int) -> str:
    suffixes = ["B", "KiB", "MiB", "GiB", "TiB"]
    val = float(value)
    for suffix in suffixes:
        if val < 1024.0 or suffix == suffixes[-1]:
            return f"{val:.1f} {suffix}"
        val /= 1024.0
    return f"{value} B"


def summarize_plan(assignments: Sequence[NodeAssignment], verbose: bool = False) -> None:
    total_events = sum(a.total_events for a in assignments)
    total_bytes = sum(a.total_bytes for a in assignments)
    print("=== Range Planner Summary ===")
    print(f"Nodes: {len(assignments)}")
    print(f"Total windows: {sum(len(a.windows) for a in assignments)}")
    print(f"Total events: {total_events}")
    print(f"Total bytes: {human_readable_bytes(total_bytes)}")
    for assignment in assignments:
        print(
            f"- Node {assignment.node_id}: {len(assignment.windows)} windows, "
            f"{assignment.total_events} events, "
            f"{human_readable_bytes(assignment.total_bytes)} total, "
            f"peak chunk {human_readable_bytes(assignment.peak_stream_bytes)}"
        )
        if verbose:
            for window in assignment.windows:
                chunk_desc = ", ".join(
                    f"{chunk.stream}@{chunk.offset}:{human_readable_bytes(chunk.length)}"
                    for chunk in window.stream_chunks
                )
                print(
                    f"    window events [{window.start_event}, {window.end_event}) "
                    f"({window.event_count} events) -> {chunk_desc}"
                )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prototype sequential range planner for xtc files.")
    parser.add_argument("--offsets", required=True, help="Path to offsets pickle.")
    parser.add_argument("--nodes", type=int, default=4, help="Number of logical nodes to assign.")
    parser.add_argument(
        "--max-stream-bytes",
        type=int,
        default=64 * 1024 * 1024,
        help="Maximum bytes per stream chunk within a window.",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=2000,
        help="Optional cap on events per window (0 disables the limit).",
    )
    parser.add_argument(
        "--include-stream",
        action="append",
        help="Stream IDs (e.g. 006) to include. Defaults to Jungfrau s006-s010.",
    )
    parser.add_argument(
        "--strategy",
        choices=["round-robin", "contiguous"],
        default="round-robin",
        help="How to map windows onto nodes.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print every window assignment instead of only the summary.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    streams_filter = args.include_stream or ["006", "007", "008", "009", "010"]
    streams, stacked = load_offsets(Path(args.offsets), streams_filter)
    max_events = args.max_events if args.max_events > 0 else None
    windows = build_event_windows(streams, stacked, args.max_stream_bytes, max_events=max_events)
    assignments = assign_windows_to_nodes(windows, args.nodes, args.strategy)
    summarize_plan(assignments, verbose=args.verbose)


if __name__ == "__main__":
    main()
