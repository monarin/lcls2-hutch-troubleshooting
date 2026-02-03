#!/usr/bin/env python3
"""
Simple multithreaded pread benchmark that mimics the node_fetcher streaming pattern.

Reads a list of xtc files sequentially using one thread per file. Each thread issues
fixed-size `os.pread` calls until reaching EOF. At the end, the script prints the per-file
throughput and aggregate throughput.
"""

import argparse
import os
import threading
import time
from pathlib import Path
from typing import List


class FileReader(threading.Thread):
    def __init__(self, path: Path, chunk_size: int, max_bytes: int):
        super().__init__(name=f"reader-{path.name}", daemon=True)
        self.path = path
        self.chunk_size = chunk_size
        self.max_bytes = max_bytes
        self.bytes_read = 0
        self.elapsed = 0.0
        self._stop_evt = threading.Event()

    def stop(self) -> None:
        self._stop_evt.set()

    def run(self) -> None:
        start = time.time()
        try:
            fd = os.open(self.path, os.O_RDONLY)
        except OSError as exc:
            print(f"[{self.name}] failed to open {self.path}: {exc}")
            return
        offset = 0
        try:
            while not self._stop_evt.is_set():
                to_read = self.chunk_size
                if self.max_bytes and (self.bytes_read + to_read) > self.max_bytes:
                    to_read = self.max_bytes - self.bytes_read
                if to_read <= 0:
                    break
                data = os.pread(fd, to_read, offset)
                if not data:
                    break
                self.bytes_read += len(data)
                offset += len(data)
        finally:
            os.close(fd)
            self.elapsed = time.time() - start
            mib = self.bytes_read / (1024 * 1024)
            rate = mib / self.elapsed if self.elapsed > 0 else 0.0
            print(
                f"[{self.name}] Read {mib:.1f} MiB in {self.elapsed:.2f}s "
                f"({rate:.1f} MiB/s)"
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Multithreaded pread benchmark for xtc files.")
    parser.add_argument(
        "--xtc-dir",
        required=True,
        help="Directory containing the xtc files.",
    )
    parser.add_argument(
        "--files",
        nargs="+",
        required=True,
        help="List of xtc filenames to read (relative to --xtc-dir).",
    )
    parser.add_argument(
        "--chunk-size-mb",
        type=int,
        choices=[8, 16, 32],
        default=32,
        help="Size of each pread chunk in MiB.",
    )
    parser.add_argument(
        "--max-read-gb",
        type=int,
        default=0,
        help="Optional cap on bytes to read per file in GiB (0 means read entire file).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    xtc_dir = Path(args.xtc_dir)
    chunk_size = args.chunk_size_mb * 1024 * 1024
    max_bytes = args.max_read_gb * 1024 * 1024 * 1024 if args.max_read_gb > 0 else 0
    files: List[Path] = [(xtc_dir / fname) for fname in args.files]
    readers = [FileReader(path, chunk_size, max_bytes) for path in files]
    start = time.time()
    for reader in readers:
        reader.start()
    for reader in readers:
        reader.join()
    elapsed = time.time() - start
    total_bytes = sum(reader.bytes_read for reader in readers)
    total_mib = total_bytes / (1024 * 1024)
    aggregate_rate = total_mib / elapsed if elapsed > 0 else 0.0
    print(
        f"Aggregate read {total_mib:.1f} MiB in {elapsed:.2f}s "
        f"Rate={aggregate_rate:.1f} MiB/s"
    )


if __name__ == "__main__":
    main()
