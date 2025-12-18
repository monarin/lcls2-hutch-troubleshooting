import pathlib
import re
import statistics

files = {
    "1bd": pathlib.Path("/sdf/home/m/monarin/sw/xtcpp/calib_jf_t_per_evt_1bd.out"),
    "16bd": pathlib.Path("/sdf/home/m/monarin/sw/xtcpp/calib_jf_t_per_evt_16bd.out"),
    "16bd_stage01": pathlib.Path("/sdf/home/m/monarin/sw/xtcpp/calib_jf_t_per_evt_16bd_stage01.out"),
    "16bd_stage02": pathlib.Path("/sdf/home/m/monarin/sw/xtcpp/calib_jf_t_per_evt_16bd_stage02.out"),
    "16bd_stage03": pathlib.Path("/sdf/home/m/monarin/sw/xtcpp/calib_jf_t_per_evt_16bd_stage03.out"),
    "16bd_stage04": pathlib.Path("/sdf/home/m/monarin/sw/xtcpp/calib_jf_t_per_evt_16bd_stage04.out"),
}
pattern = re.compile(r"\[Rank\s+(?P<rank>\d+)\].*calib_jungfrau time.*: (?P<time>[0-9.]+)")

for label, path in files.items():
    try:
        lines = path.read_text().splitlines()
    except FileNotFoundError:
        print(f"{label}: missing {path}")
        continue

    samples = []
    per_rank_counts = {}
    for idx, line in enumerate(lines, start=1):
        match = pattern.search(line)
        if match:
            rank = int(match.group("rank"))
            per_rank_counts[rank] = per_rank_counts.get(rank, 0) + 1
            samples.append(
                (
                    float(match.group("time")),
                    rank,
                    per_rank_counts[rank],
                    idx,
                )
            )

    if not samples:
        print(f"{label}: no samples in {path}")
        continue

    vals = [s[0] for s in samples]
    n = len(vals)
    min_v = min(vals)
    max_v = max(vals)
    avg = statistics.fmean(vals)
    median = statistics.median(vals)
    std = statistics.pstdev(vals) if n > 1 else 0.0
    threshold = avg + 2 * std
    outliers = [
        (rank, evt_rank_idx, val, line_idx)
        for val, rank, evt_rank_idx, line_idx in samples
        if val > threshold
    ]
    percent = (len(outliers) / n) * 100.0

    print(
        f"{label}: samples={n} "
        f"min={min_v:.6f}s max={max_v:.6f}s avg={avg:.6f}s median={median:.6f}s std={std:.6f}s "
        f">{avg:.6f}+2*std ({threshold:.6f}s): {len(outliers)} events ({percent:.2f}%)"
    )
    if outliers:
        print("  Outliers (rank, rank_event_index, file_line, time):")
        for rank, evt_rank_idx, val, line_idx in outliers:
            print(
                f"    Rank {rank:>3} event {evt_rank_idx:>5} (line {line_idx:>6}): {val:.6f}s"
            )
