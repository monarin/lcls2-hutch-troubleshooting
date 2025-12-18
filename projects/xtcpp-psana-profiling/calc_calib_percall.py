#!/usr/bin/env python3
import argparse
import glob
import os
import pstats

def extract(statfile, key):
    stats = pstats.Stats(statfile)
    entry = stats.stats.get(key)
    if not entry:
        return None
    cc, nc, tt, ct, callers = entry
    return (tt / nc if nc else 0.0, ct / nc if nc else 0.0, nc)

def main():
    parser = argparse.ArgumentParser(description="Compute per-call times for calib_jungfrau")
    parser.add_argument("log_dir", help="Directory containing jf_calib.rank*.prof")
    args = parser.parse_args()

    pattern = os.path.join(args.log_dir, "jf_calib.rank*.prof")
    files = sorted(glob.glob(pattern))
    if not files:
        raise SystemExit(f"No profiles found under {pattern}")

    target = ("/sdf/home/m/monarin/lcls2/psana/psana/detector/UtilsJungfrau.py", 302, "calib_jungfrau")
    print("rank\tcall_count\ttime_per_call(cum)")
    for path in files:
        result = extract(path, target)
        rank = os.path.basename(path).split("rank")[-1].split(".")[0]
        if result:
            _, cum_per_call, calls = result
            print(f"{rank}\t{calls}\t{cum_per_call:.6f}")
        else:
            print(f"{rank}\tN/A\tN/A")

if __name__ == "__main__":
    main()
