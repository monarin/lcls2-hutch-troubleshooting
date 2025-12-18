#!/usr/bin/env python3
import argparse
import glob
import os
import pstats


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize cProfile outputs per rank")
    parser.add_argument("log_dir", help="Directory containing profile.rank*.prof files")
    parser.add_argument(
        "-n", "--num", type=int, default=20, help="Number of lines to print per profile"
    )
    args = parser.parse_args()

    pattern = os.path.join(args.log_dir, "profile.rank*.prof")
    profiles = sorted(glob.glob(pattern))
    if not profiles:
        raise SystemExit(f"No profile files found matching {pattern}")

    for prof in profiles:
        print(f"\nTop cumulative functions for {prof}\n")
        try:
            pstats.Stats(prof).sort_stats("cumulative").print_stats(args.num)
        except FileNotFoundError:
            print(f"Missing profile: {prof}")
        except Exception as exc:
            print(f"Failed to read {prof}: {exc}")


if __name__ == "__main__":
    main()
