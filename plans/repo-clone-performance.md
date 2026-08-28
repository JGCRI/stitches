# Why Cloning `stitches` Is Slow — Diagnosis and Solutions

The checked-out working tree is modest (a dozen Python modules, a handful of small CSVs, a Sphinx site). The clone cost is therefore **almost entirely in `.git`**: object history, not the current snapshot.

---

## 0. Measured Results (2026-08-28, branch `release/v1`)

```
working tree   ~2 MB
.git           288 MB      (size-pack 274.44 MiB, 5702 objects in 1 pack)
total blobs    784.1 MB uncompressed across all history
```

Blob bytes grouped by category:

| Uncompressed | Category | In HEAD? |
|---:|---|---|
| 346.2 MB | `stitches/data/**` package data | No — `.gitignore`d |
| 188.3 MB | `notebooks/quickstart-ncs/*.nc` | No — deleted |
| 127.2 MB | `notebooks/stitches_dev/**` (HTML + CSV) | No — deleted |
| 82.9 MB | `*.ipynb` with embedded outputs | Partly |
| 20.2 MB | images / PDF | Partly |
| 16.3 MB | everything else | Mostly |
| 2.9 MB | HTML | No |

**722.6 MB of 784.1 MB (92%) is blobs for paths that no longer exist in `HEAD`.**

Worst individual offenders:

| Size | Revs | Path |
|---:|---:|---|
| 94.16 MB | 1 | `notebooks/quickstart-ncs/stitched_CanESM5_tas_ssp245~r1i1p1f1~1.nc` |
| 94.16 MB | 1 | `notebooks/quickstart-ncs/stitched_CanESM5_pr_ssp245~r1i1p1f1~1.nc` |
| 77.10 MB | 1 | `stitches/data/pangeo_comparison_table.csv` |
| 56.47 MB | 5 | `stitches/data/pangeo_table.csv` |
| 49.89 MB | 3 | `stitches/data/matching_archive_staggered.csv` |
| 41.93 MB | 1 | `notebooks/stitches_dev/inputs/main_raw_pasted_tgav_anomaly_all_pangeo_list_models.csv` |
| 38.52 MB | **31** | `notebooks/stitches-quickstart.ipynb` |
| 28.33 MB | 12 | `notebooks/stitches_dev/Notebook6_throwout_Duplicates-across-ensemble-members.html` |
| 21.63 MB | 8 | `stitches/data/matching_archive.csv` |
| 20.53 MB | 2 | `stitches/data/created_data/main_tgav_all_pangeo_list_models.csv` |
| 11.51 MB | 3 | `stitches/data/tas-data/ACCESS-ESM1-5_tas.csv` |
| 10.86 MB | 1 | `notebooks/figs/Tutorial_2023_tgavex.tiff` |
| 10.14 MB | 8 | `notebooks/GCAM_AnnualMeeting2023.ipynb` |
| 7.85 MB | 2 | `stitches/data/tas_values.pkl` |

Reproduce with:

```bash
git rev-list --objects --all \
  | git cat-file --batch-check='%(objecttype) %(objectname) %(objectsize) %(rest)' \
  | awk '$1=="blob" {print $3, $4}' | sort -k2 \
  | awk '{s[$2]+=$1; n[$2]++} END {for (p in s) printf "%12.2f MB  %4d rev  %s\n", s[p]/1048576, n[p], p}' \
  | sort -rn | head -30
```

**Conclusion: a history rewrite (§3.4) is clearly justified.** Removing the deleted-path blobs alone should reduce the clone by roughly an order of magnitude. Notebook output stripping (§3.2) is still required to stop the problem recurring — `stitches-quickstart.ipynb` alone has accumulated 38.5 MB across 31 revisions of an ~1 MB file.

---

## 1. Root Causes

### Cause A — Notebooks committed with embedded base64 outputs (primary)

Four notebooks contain inline `image/png` payloads:

| Notebook | Embedded PNG outputs found |
|---|---|
| [`notebooks/stitches-quickstart.ipynb`](../notebooks/stitches-quickstart.ipynb) | 6 |
| [`notebooks/stitches_takehome_GCAMAnnualMeeting2023.ipynb`](../notebooks/stitches_takehome_GCAMAnnualMeeting2023.ipynb) | 10 |
| [`notebooks/stitches_training_GCAMAnnualMeeting2023.ipynb`](../notebooks/stitches_training_GCAMAnnualMeeting2023.ipynb) | 6 |
| [`notebooks/preparing-input-data.ipynb`](../notebooks/preparing-input-data.ipynb) | 1 |

Why this is disproportionately expensive:

- Base64 inflates binary PNGs by ~33%.
- The payloads sit on **single enormous JSON lines**, so git's line-oriented delta compression is ineffective.
- Re-executing a notebook changes every image byte-for-byte (matplotlib metadata, font hinting, timestamps), plus `execution_count` and cell ids. Each commit that touches a notebook therefore stores an essentially **new full copy** of every figure in it.
- The training/takehome notebooks were produced with matplotlib 3.5.1 and the quickstart with 3.8.2, which is direct evidence of multiple re-execution generations living in history.

Net effect: history accumulates N copies of every figure for N notebook commits.

### Cause B — Package data and generated artifacts committed and later deleted (largest total)

Confirmed by §0: 346.2 MB of `stitches/data/**`, 188.3 MB of stitched NetCDF outputs under `notebooks/quickstart-ncs/`, and 127.2 MB of rendered development notebooks under `notebooks/stitches_dev/`. None of these paths exist in `HEAD`.

[`.gitignore`](../.gitignore:1) begins with a block of *external data* exclusions:

```
stitches/data/tas-data/*.*
stitches/data/temp-data/*.*
stitches/data/*.nc
stitches/data/matching_archive.csv
stitches/data/matching_archive_staggered.csv
stitches/data/pangeo_comparison_table.csv
stitches/data/pangeo_table.csv
stitches/data/*.csv
```

Rules that specific are typically written *after* the files caused a problem. If any of those CSV/NetCDF files (the archive is Zenodo record `8367628`, a multi-hundred-MB `data.zip`) were ever committed, `.gitignore` removes them from the *working tree* only — the blobs stay in history forever and are still transferred on clone.

### Cause C — Duplicated and rendered binary assets

- `stitches_diagram.jpg` exists three times: [`docs/source/getting-started/stitches_diagram.jpg`](../docs/source/getting-started/stitches_diagram.jpg), [`notebooks/figs/stitches_diagram.jpg`](../notebooks/figs/stitches_diagram.jpg), [`paper/stitches_diagram.jpg`](../paper/stitches_diagram.jpg).
- Committed Sphinx renders: `docs/source/getting-started/output_13_1.png`, `output_16_0.png`, `output_22_0.png`, `output_25_0.png`, `output_40_0.png`, `output_40_1.png` — these are *generated* artifacts that should be produced at docs-build time.
- Additional JPEGs under [`notebooks/figs/`](../notebooks/figs/) (`Tutorial_2023_T_dT_example.jpg`, `Tutorial_2023_tgavex.jpg`) and [`docs/source/images/`](../docs/source/images/).

Binary assets never delta-compress well, and every revision of them is a full new blob.

---

## 2. Measure Before You Act

Run these and record the numbers in the PR that implements the fix.

```bash
# Total repo size and pack breakdown
du -sh .git
git count-objects -vH

# 20 largest blobs actually reachable in history, with their paths
git rev-list --objects --all \
  | git cat-file --batch-check='%(objecttype) %(objectname) %(objectsize) %(rest)' \
  | awk '$1=="blob"' \
  | sort -k3 -n -r \
  | head -20

# Size contribution grouped by path
git rev-list --objects --all \
  | git cat-file --batch-check='%(objecttype) %(objectname) %(objectsize) %(rest)' \
  | awk '$1=="blob" {print $3, $4}' \
  | sort -k2 \
  | awk '{s[$2]+=$1} END {for (p in s) print s[p], p}' \
  | sort -n -r | head -30

# Clone wall-clock baseline (cold)
time git clone --no-local <remote-url> /tmp/stitches-clonetest
```

`git-sizer` and `git filter-repo --analyze` both produce a nicer report:

```bash
brew install git-sizer git-filter-repo
git-sizer --verbose
git filter-repo --analyze   # writes .git/filter-repo/analysis/*.txt
```

Note: `git filter-repo --analyze` refuses to run in a repo with uncommitted changes and, like all `filter-repo` invocations, expects a fresh clone. Run it against a throwaway `--mirror` clone.

The `analysis/path-all-sizes.txt` output directly confirms or refutes Causes A/B/C with hard numbers.

---

## 3. Solutions

### 3.1 Immediate relief for users (non-destructive, zero coordination)

Document these in [`README.md`](../README.md) and the contributor guide:

```bash
# Blobless clone — full history graph, blobs fetched on demand. Usually the best default.
git clone --filter=blob:none https://github.com/JGCRI/stitches.git

# Shallow clone — for CI or read-only use
git clone --depth 1 https://github.com/JGCRI/stitches.git

# Single branch, shallow — smallest
git clone --depth 1 --single-branch --branch main https://github.com/JGCRI/stitches.git
```

Also apply `--filter=blob:none` in CI checkouts:

```yaml
- uses: actions/checkout@v4
  with:
    filter: blob:none
    fetch-depth: 1
```

This mitigates the symptom immediately without touching history.

### 3.2 Stop the bleeding (required, do this first)

1. **Strip notebook outputs on commit.** Add to [`.pre-commit-config.yaml`](../.pre-commit-config.yaml):

   ```yaml
   - repo: https://github.com/kynan/nbstripout
     rev: 0.7.1
     hooks:
       - id: nbstripout
   ```

   Optionally add a `.gitattributes` filter as a second line of defense:

   ```
   *.ipynb filter=nbstripout
   ```

2. **Guard against large files.** Add `check-added-large-files` to the existing `pre-commit-hooks` block:

   ```yaml
   - id: check-added-large-files
     args: ['--maxkb=512']
   ```

3. **Stop committing generated docs images.** Delete `docs/source/getting-started/output_*.png` and let `nbsphinx` execute the notebook during the docs build (`nbsphinx_execute = "always"` with cached data), or generate them into `docs/_build/`.

4. **Deduplicate `stitches_diagram.jpg`** to one canonical location (e.g. `docs/source/images/`) and reference it from the notebook and paper.

Trade-off to accept: stripped notebooks show no output on GitHub's static renderer. Mitigate by publishing executed notebooks in the hosted docs (already wired via `nbsphinx`) and linking to them from the README.

### 3.3 Structural options for the notebooks

| Option | Clone cost | GitHub preview | Effort |
|---|---|---|---|
| `nbstripout` (outputs removed) | Low | No outputs shown | Low |
| `jupytext` paired `.py:percent` + generated `.ipynb` untracked | Lowest | No notebook in repo | Medium |
| Keep outputs but move figures to external files referenced by path | Medium | Outputs shown | Medium |
| Keep as-is | High | Outputs shown | None |

Recommendation: `nbstripout` now; consider `jupytext` pairing for the two GCAM-meeting notebooks, which are historical training artifacts and arguably belong in a tagged release asset rather than `main`.

### 3.4 History rewrite (optional, destructive, highest payoff)

Only worth doing if §2 shows that historical blobs dominate. This is the only way to actually shrink an existing clone.

```bash
# Work on a fresh mirror, never your working clone
git clone --mirror https://github.com/JGCRI/stitches.git stitches-mirror
cd stitches-mirror

# Drop historical package data, stitched outputs, and dev notebook renders.
# Paths below are the confirmed offenders from section 0; none exist in HEAD,
# so removing them cannot affect the current tree.
git filter-repo \
  --path 'notebooks/quickstart-ncs' \
  --path 'notebooks/stitches_dev' \
  --path 'stitches/data/created_data' \
  --path-glob 'stitches/data/tas-data/*' \
  --path-glob 'stitches/data/temp-data/*' \
  --path-glob 'stitches/data/*.nc' \
  --path-glob 'stitches/data/*.pkl' \
  --path-glob 'stitches/data/matching_archive*.csv' \
  --path-glob 'stitches/data/pangeo_*table.csv' \
  --path-glob '*.tiff' \
  --path-glob 'docs/source/getting-started/output_*.png' \
  --invert-paths

# Verify nothing in HEAD was removed
git diff --stat <pre-rewrite-head-sha> HEAD   # expect: empty

# Strip notebook outputs from every historical revision
git filter-repo --force \
  --path-glob '*.ipynb' \
  --blob-callback '
import json
try:
    nb = json.loads(blob.data)
except Exception:
    pass
else:
    if isinstance(nb, dict) and "cells" in nb:
        for c in nb["cells"]:
            c["outputs"] = []
            c["execution_count"] = None
        blob.data = json.dumps(nb, indent=1).encode()
'

git reflog expire --expire=now --all
git gc --prune=now --aggressive
```

Consequences that must be planned for:

- **Every commit SHA changes.** All open PRs must be rebased or recreated; all forks must re-clone or re-base.
- Zenodo DOI archives and the JOSS paper reference specific commits/tags — verify tags survive the rewrite (`git filter-repo` preserves them but re-points them) and keep a **read-only archived mirror** of the pre-rewrite history for citation integrity.
- Requires a force-push to a protected branch: temporarily lift protection, announce a freeze window, then restore.

Pre-flight checklist:

- [ ] §2 measurements recorded, showing the expected savings
- [ ] Archived mirror pushed to a `stitches-history-archive` repository
- [ ] All maintainers notified with a freeze window and re-clone instructions
- [ ] Open PR inventory captured
- [ ] Tags and releases verified post-rewrite on the mirror before force-push
- [ ] Post-rewrite clone time re-measured and recorded

### 3.5 If large files must live in the repo

Prefer *not* to. The package already downloads science data from Zenodo at runtime via [`stitches/install_pkgdata.py`](../stitches/install_pkgdata.py), which is the correct pattern. If any binary must be versioned, use Git LFS or a GitHub Release asset rather than a normal blob.

---

## 4. Recommended Plan of Record

1. Measure (§2) and publish the numbers.
2. Land the preventive hooks and asset cleanup (§3.2) — safe, immediate, no coordination.
3. Document `--filter=blob:none` for users and switch CI to filtered/shallow checkout (§3.1).
4. Decide on the rewrite (§3.4) based on the §2 evidence; if the historical blobs are under ~50 MB, skip it and rely on partial clone.
5. Re-measure clone time and record the before/after in `CHANGELOG.md`.
