#!/usr/bin/env python3
"""
Prototype node-side fetcher that consumes range_planner assignments and
pre-fetches large sequential windows per stream into a local ring buffer.

Usage example:
    python3 node_fetcher.py --offsets offsets_mfx101344525_r0070.pkl \
        --xtc-dir /sdf/data/lcls/ds/mfx/mfx101344525/xtc --nodes 4 --node-id 0 \
        --strategy contiguous --max-stream-bytes 33554432 \
        --buffer-bytes 536870912 --sample-consumer 10
"""

import argparse
import os
import threading
import time
from collections import deque
from pathlib import Path
from typing import Deque, Dict, Iterable, Iterator, List, NamedTuple, Optional, Sequence

import numpy as np

from range_planner import (
    EventWindow,
    StreamChunk,
    assign_windows_to_nodes,
    build_event_windows,
    load_offsets,
)


class StreamWorkItem(NamedTuple):
    window_index: int
    start_event: int
    end_event: int
    chunk: StreamChunk


class BufferRecord(NamedTuple):
    stream: str
    window_index: int
    start_event: int
    end_event: int
    offset: int
    length: int
    payload: bytes


class RingBuffer(object):
    """Simple blocking ring buffer for byte payloads + metadata."""

    def __init__(self, capacity_bytes: int):
        self._capacity = capacity_bytes
        self._current = 0
        self._queue: Deque[BufferRecord] = deque()
        self._cv = threading.Condition()
        self._active_producers = 0
        self._closed = False

    def set_producer_count(self, count: int) -> None:
        with self._cv:
            self._active_producers = count

    def produce(self, record: BufferRecord) -> None:
        size = len(record.payload)
        with self._cv:
            while not self._closed and (self._current + size) > self._capacity:
                self._cv.wait()
            if self._closed:
                raise RuntimeError("RingBuffer closed")
            self._queue.append(record)
            self._current += size
            self._cv.notify_all()

    def consume(self) -> Optional[BufferRecord]:
        with self._cv:
            while not self._queue and self._active_producers > 0 and not self._closed:
                self._cv.wait()
            if not self._queue:
                return None
            record = self._queue.popleft()
            self._current -= len(record.payload)
            self._cv.notify_all()
            return record

    def producer_done(self) -> None:
        with self._cv:
            self._active_producers -= 1
            if self._active_producers <= 0:
                self._cv.notify_all()

    def close(self) -> None:
        with self._cv:
            self._closed = True
            self._cv.notify_all()


class StreamReader(threading.Thread):
    """Dedicated reader for a single xtc stream."""

    def __init__(
        self,
        stream_name: str,
        xtc_dir: Path,
        work_items: Sequence[StreamWorkItem],
        buffer: RingBuffer,
    ):
        super(StreamReader, self).__init__(name=f"reader-{stream_name}", daemon=True)
        self.stream_name = stream_name
        self.xtc_path = xtc_dir / stream_name
        self.work_items = work_items
        self.buffer = buffer
        self._stop_evt = threading.Event()
        self._bytes_read = 0
        self._io_time = 0.0

    def stop(self) -> None:
        self._stop_evt.set()

    def run(self) -> None:
        try:
            fd = os.open(self.xtc_path, os.O_RDONLY)
        except OSError as exc:
            print(f"[{self.name}] failed to open {self.xtc_path}: {exc}")
            self.buffer.producer_done()
            return
        try:
            for item in self.work_items:
                if self._stop_evt.is_set():
                    break
                start = time.perf_counter()
                data = os.pread(fd, item.chunk.length, item.chunk.offset)
                self._io_time += time.perf_counter() - start
                self._bytes_read += len(data)
                record = BufferRecord(
                    stream=self.stream_name,
                    window_index=item.window_index,
                    start_event=item.start_event,
                    end_event=item.end_event,
                    offset=item.chunk.offset,
                    length=item.chunk.length,
                    payload=data,
                )
                self.buffer.produce(record)
        finally:
            os.close(fd)
            if self._io_time > 0 and self._bytes_read > 0:
                mib = self._bytes_read / (1024 * 1024)
                rate = mib / self._io_time if self._io_time > 0 else 0.0
                print(
                    f"[{self.name}] Disk read {mib:.1f} MiB in {self._io_time:.2f}s "
                    f"Rate={rate:.1f} MiB/s"
                )
            self.buffer.producer_done()


class NodeFetcher(object):
    """Coordinates per-stream readers and exposes a consumer iterator."""

    def __init__(
        self,
        xtc_dir: Path,
        node_windows: Sequence[EventWindow],
        buffer_bytes: int,
        buffer: RingBuffer = None,
    ):
        self.xtc_dir = xtc_dir
        self.node_windows = node_windows
        if buffer is None:
            self.buffer = RingBuffer(buffer_bytes)
            self._owns_buffer = True
        else:
            self.buffer = buffer
            self._owns_buffer = False
        self.readers: List[StreamReader] = []
        self._started = False

    def _build_stream_map(self) -> Dict[str, List[StreamWorkItem]]:
        mapping: Dict[str, List[StreamWorkItem]] = {}
        for idx, window in enumerate(self.node_windows):
            for chunk in window.stream_chunks:
                mapping.setdefault(chunk.stream, []).append(
                    StreamWorkItem(
                        window_index=idx,
                        start_event=window.start_event,
                        end_event=window.end_event,
                        chunk=chunk,
                    )
                )
        return mapping

    def start(self) -> None:
        if self._started:
            return
        stream_map = self._build_stream_map()
        self.buffer.set_producer_count(len(stream_map))
        for stream, work in stream_map.items():
            reader = StreamReader(stream, self.xtc_dir, work, self.buffer)
            reader.start()
            self.readers.append(reader)
        self._started = True

    def stop(self) -> None:
        for reader in self.readers:
            reader.stop()
        for reader in self.readers:
            reader.join()
        if self._owns_buffer:
            self.buffer.close()

    def wait_for_completion(self) -> None:
        """Block until all reader threads finish."""
        for reader in self.readers:
            reader.join()

    def consume(self) -> Iterator[BufferRecord]:
        while True:
            record = self.buffer.consume()
            if record is None:
                return
            yield record


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prototype node fetcher for sequential xtc windows.")
    parser.add_argument("--offsets", required=True, help="Offsets pickle path.")
    parser.add_argument("--xtc-dir", required=True, help="Directory with xtc files.")
    parser.add_argument("--nodes", type=int, default=4, help="Total node count.")
    parser.add_argument("--node-id", type=int, default=0, help="Zero-based node identifier to simulate.")
    parser.add_argument(
        "--max-stream-bytes",
        type=int,
        default=32 * 1024 * 1024,
        help="Upper bound for each stream chunk within a window.",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=2000,
        help="Optional cap on events per window (0 disables).",
    )
    parser.add_argument(
        "--strategy",
        choices=["round-robin", "contiguous"],
        default="contiguous",
        help="Window assignment strategy.",
    )
    parser.add_argument(
        "--buffer-bytes",
        type=int,
        default=512 * 1024 * 1024,
        help="Ring buffer capacity per node.",
    )
    parser.add_argument(
        "--sample-consumer",
        type=int,
        default=0,
        help="Read this many buffer records and print a brief summary (0 disables).",
    )
    parser.add_argument(
        "--include-stream",
        action="append",
        help="Restrict to selected stream IDs (ex: 006). Defaults to s006-s010.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    include = args.include_stream or ["006", "007", "008", "009", "010"]
    streams, stacked = load_offsets(Path(args.offsets), include)
    max_events = args.max_events if args.max_events > 0 else None
    windows = build_event_windows(streams, stacked, args.max_stream_bytes, max_events=max_events)
    assignments = assign_windows_to_nodes(windows, args.nodes, args.strategy)
    if args.node_id < 0 or args.node_id >= len(assignments):
        raise SystemExit(f"node-id must be within [0, {len(assignments) - 1}]")
    node_assignment = assignments[int(args.node_id)]
    print(
        f"[NodeFetcher] node {args.node_id} -> {len(node_assignment.windows)} windows, "
        f"{node_assignment.total_events} events"
    )
    fetcher = NodeFetcher(Path(args.xtc_dir), node_assignment.windows, args.buffer_bytes)
    fetcher.start()
    try:
        if args.sample_consumer > 0:
            for idx, record in enumerate(fetcher.consume()):
                print(
                    f"chunk {idx}: stream={record.stream} window={record.window_index} "
                    f"events[{record.start_event},{record.end_event}) "
                    f"offset={record.offset} len={record.length}"
                )
                if idx + 1 >= args.sample_consumer:
                    break
    finally:
        fetcher.stop()


if __name__ == "__main__":
    main()
