import os, glob
exp = 'mfx101262725'
base = f'/sdf/data/lcls/ds/mfx/{exp}/xtc'
MB7 = 7 * 1024 * 1024
runs = {}
for f in glob.glob(os.path.join(base, f'{exp}-r*-s*-c000.xtc2')):
    run = os.path.basename(f).split('-')[1]
    runs.setdefault(run, []).append(os.path.getsize(f))
rows = []
for run, sizes in runs.items():
    jungfrau = sorted(sizes, reverse=True)[:5]
    if len(jungfrau) == 5:
        rows.append((run, sum(jungfrau)/len(jungfrau)/MB7))
rows.sort(key=lambda x: x[1], reverse=True)
for r,e in rows:
    print(f'{r}\t{e:9.0f}')
print('Total runs with Jungfrau data:', len(rows))

