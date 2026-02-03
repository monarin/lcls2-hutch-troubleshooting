#!/bin/bash
jobid=17999417
for src in /dev/shm/calibconst.pkl /dev/shm/jungfrau_calibc_cache.pkl; do
    dst="/dev/shm/$(basename "$src")"
    echo "Staging $src -> $dst on job $jobid"
    sbcast --job="$jobid" --compress "$src" "$dst"
done

