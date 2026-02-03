#!/usr/bin/env python3
"""
Parse ds_count_events rank output and summarize statistics.

Expected line format (one per rank):
[INFO] Rank 223 processed 173 events Rate=0.9 Hz Load time=27.78s Loop time=185.04s |
  IO bytes=5536.45 MiB time=181.21s rate=30.55 MiB/s calls=173

Usage: python parse_rank_stats.py path/to/logfile
"""

from __future__ import annotations

import argparse
import re
import statistics as stats
from pathlib import Path
from typing import Dict, Iterable, List


LINE_RE = re.compile(
    r"processed (?P<events>\d+) events "
    r"Rate=(?P<rate>[0-9.]+) Hz "
    r"Load time=(?P<load>[0-9.]+)s "
    r"Loop time=(?P<loop>[0-9.]+)s "
    r"\| IO bytes=(?P<bytes>[0-9.]+) MiB "
    r"time=(?P<iot>[0-9.]+)s "
    r"rate=(?P<ior>[0-9.]+) MiB/s "
    r"calls=(?P<calls>\d+)"
)

FIELDS = [
    ("events", "Processed events"),
    ("rate", "Rate (Hz)"),
    ("load", "Load time (s)"),
    ("loop", "Loop time (s)"),
    ("bytes", "IO bytes (MiB)"),
    ("iot", "IO time (s)"),
    ("ior", "IO rate (MiB/s)"),
    ("calls", "Calls"),
]


def parse_file(path: Path) -> List[Dict[str, float]]:
    rows: List[Dict[str, float]] = []
    for line in path.read_text().splitlines():
        match = LINE_RE.search(line)
        if match:
            rows.append({k: float(v) for k, v in match.groupdict().items()})
    return rows


def summarize(values: Iterable[float]) -> Dict[str, float]:
    vals = list(values)
    count = len(vals)
    if count == 0:
        raise ValueError("No data points were parsed; check the input file format.")
    return {
        "count": count,
        "avg": sum(vals) / count,
        "min": min(vals),
        "max": max(vals),
        "std": stats.pstdev(vals) if count > 1 else 0.0,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Summarize ds_count_events per-rank metrics."
    )
    parser.add_argument(
        "logfile",
        type=Path,
        help="Path to the log file containing per-rank output.",
    )
    args = parser.parse_args()

    rows = parse_file(args.logfile)
    if not rows:
        raise SystemExit("No matching lines found in the log file.")

    print(f"Parsed {len(rows)} rank entries from {args.logfile}")
    print(f"{'Field':25s} {'Count':>5s} {'Avg':>10s} {'Min':>10s} {'Max':>10s} {'Std':>10s}")
    for field, label in FIELDS:
        summary = summarize(row[field] for row in rows)
        print(
            f"{label:25s} "
            f"{summary['count']:5d} "
            f"{summary['avg']:10.4f} "
            f"{summary['min']:10.4f} "
            f"{summary['max']:10.4f} "
            f"{summary['std']:10.4f}"
        )


if __name__ == "__main__":
    main()
