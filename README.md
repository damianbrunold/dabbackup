# dabbak

A small, dependency-free Python backup tool that keeps an always-current mirror of your files plus dated incremental snapshots, so you can restore both "the latest version" and "this file as it was last Tuesday."

Single file, stdlib only. Runs on Linux, macOS, and Windows.

## What it does

Each time you run `dabbak backup`, it walks your configured source directories and produces two outputs:

- **A full mirror** at `destination.directory_full` — always reflects the current state of your sources. One file in, one file out. Restoring "the latest version" of anything is just a normal file copy from here.
- **A dated partial snapshot** at `destination.directory_partial/YYYY-MM-DD/` — contains only the files that were *new or changed* on that run. Together with all prior snapshots, this lets you reconstruct your sources as they were on any past date.

A JSON state file remembers each file's size + mtime between runs, so dabbak knows what changed without re-reading every byte.

## Quick start

```bash
# 1. Drop dabbak.py somewhere convenient
# 2. Create a config file next to it
python dabbak.py init

# 3. Edit backup-config.json to point at your real sources/destinations
# 4. Run a backup
python dabbak.py backup
```

## Requirements

- Python 3.9+ (3.12+ recommended for `os.path.isjunction` support on Windows; older versions silently skip the check)
- No third-party packages

## Setup

1. **Place `dabbak.py`** somewhere convenient (e.g. `~/bin/`, `/opt/dabbak/`, or your home directory).

2. **Generate a config template:**

   ```bash
   python dabbak.py init
   ```

   This creates `backup-config.json` next to `dabbak.py`. You can name it anything with `--name foo.json`.

3. **Edit the config** to point at your real sources and destinations:

   ```json
   {
     "source": {
       "directories": ["/home/me/Documents", "/home/me/projects"],
       "excludes": ["__pycache__", "*.pyc", "node_modules", ".git"]
     },
     "destination": {
       "directory_full": "/mnt/backup/full",
       "directory_partial": "/mnt/backup/partial"
     },
     "full_state_file": "/mnt/backup/state.json",
     "packaging_state_file": "packaging-state.json"
   }
   ```

4. **Verify the config is picked up:**

   ```bash
   python dabbak.py config
   ```

5. **First backup** (this initial run copies *everything* — subsequent runs only handle changes):

   ```bash
   python dabbak.py backup
   ```

### Using multiple configs

dabbak reads `backup-config.json` by default. To use a different file, set the `DABBAK_CONFIG` env var:

```bash
DABBAK_CONFIG=backup-config-laptop.json python dabbak.py backup
```

## Configuration reference

| Field | Type | Description |
|---|---|---|
| `source.directories` | list[str] | Source directories to back up. Paths ending in `*` are expanded one level (e.g. `/home/users/*` becomes each child directory). |
| `source.excludes` | list[str] | Patterns to skip. See [Exclude syntax](#exclude-syntax). |
| `source.is-windows` | bool, optional | Set true when reading a Windows-formatted state file on POSIX (rare; for cross-platform restore). |
| `destination.directory_full` | str | Where the always-current mirror lives. |
| `destination.directory_partial` | str | Where dated snapshots live (one folder per day they ran). |
| `full_state_file` | str | Path to the JSON state file. dabbak writes to it atomically. |
| `packaging_state_file` | str | Relative path (resolved next to `dabbak.py`) where `package --full` records its last full-package timestamp. |

### Exclude syntax

Excludes are gitignore-flavored:

| Form | Example | Meaning |
|---|---|---|
| No slash | `__pycache__`, `*.pyc`, `node_modules` | Match against the **basename** anywhere in the tree. Globs (`*`, `?`, `[…]`) supported. |
| Slash + glob | `**/build/*`, `*/.cache/*` | Match against the **full path** via `fnmatch`. |
| Slash, no glob | `/home/me/.cache` | Exact **absolute path** match (legacy form, still supported). |

Matching is case-insensitive on Windows to match the filesystem.

## Commands

All commands are subcommands of `dabbak.py`. Every subcommand supports `--help`.

### `init`

Create a config template next to `dabbak.py`.

```
dabbak init [--name FILE] [--force]
```

| Flag | Description |
|---|---|
| `--name FILE` | Filename to write (default: `backup-config.json`). |
| `--force` | Overwrite an existing config. |

---

### `backup`

Run an incremental backup. Copies new/changed files into both `directory_full` and today's `directory_partial/YYYY-MM-DD/` snapshot. Deletes files from the mirror that no longer exist at the source.

```
dabbak backup [--dry-run] [-q|--quiet] [--json]
```

| Flag | Description |
|---|---|
| `--dry-run` | Walk and diff, but write nothing — no file copies, no state update, no snapshot folder. |
| `--quiet`, `-q` | Suppress per-file output; show warnings + final summary only. |
| `--json` | Suppress normal output; emit a JSON summary on stdout. Good for cron/monitoring. |

**Per-file markers in the log:**
- `++ /path/to/file` — new file
- `** /path/to/file` — changed file
- `-- /path/to/file` — file deleted at source (and from mirror)

**Exit codes:** 0 on success (including with copy errors logged); 1 if another dabbak run holds the lock; 2 on argument errors.

**Failure handling:** if dabbak is interrupted mid-run (Ctrl-C, USB unplug, network share drop), state is *merged* rather than truncated — entries for paths not yet visited are preserved from the previous state. The deletion pass is skipped. Today's snapshot folder gets an `__incomplete` marker so `restore` and `package` ignore it. Re-running `backup` resumes naturally.

---

### `restore`

Copy files out of the backup as they existed at a given date.

```
dabbak restore <dest_dir> [-t YYYY-MM-DD] [pattern ...] [--dry-run] [--force]
```

| Argument / Flag | Description |
|---|---|
| `dest_dir` | Where restored files go. Must not exist (override with `--force`). |
| `pattern ...` | Optional path filters. Each pattern is either a **glob** (contains `*`, `?`, or `[`) matched against the full source path, or a **prefix** otherwise. Omit to restore everything in the snapshot. |
| `-t`, `--timestamp YYYY-MM-DD` | Snapshot date (default: today). dabbak picks the newest snapshot at or before this date. |
| `--dry-run` | Print "DRY <dest> ← <snapshot>/<relpath>" lines, copy nothing. |
| `--force` | Allow restoring into an existing directory. |

**Back-compat:** the legacy positional form `dabbak restore /tmp/out 2024-03-15 /home/me/Docs` still works — a first positional matching `YYYY-MM-DD` is interpreted as a timestamp.

**How dates are resolved:** dabbak loads `__state.json` from the newest *valid* snapshot at or before the requested date (snapshots with an `__incomplete` marker are skipped) and uses it as the file manifest. For each listed file, it walks the snapshot history newest-first and copies the first version it finds.

---

### `list`

Show the partial snapshots that exist.

```
dabbak list [--json]
```

Prints a table of `date / files / size / status` (where `status` is `ok` or `incomplete`). `--json` emits a machine-readable array.

---

### `prune`

Delete old partial snapshots according to a retention policy.

```
dabbak prune [--keep-last N] [--keep-days N] [--force] [--json]
```

| Flag | Description |
|---|---|
| `--keep-last N` | Keep the N most recent snapshots. |
| `--keep-days N` | Keep snapshots dated within the last N days. |
| `--force` | Actually delete. Without this, prune is a **dry-run** that prints what it would do. |
| `--json` | JSON output. |

A snapshot is kept if it satisfies **either** policy (union, not intersection). Today's snapshot is always kept regardless. Pruning deletes the dated snapshot folder *and* its `backup-partial-YYYY-MM-DD.log` together.

You must specify at least one of `--keep-last` or `--keep-days`.

---

### `package`

Build size-chunked offline archives from the partial history. Useful for burning to DVDs, splitting across external drives, etc.

```
dabbak package <dest_dir> <max_size> [<timestamp>] [--full] [--force]
```

| Argument / Flag | Description |
|---|---|
| `dest_dir` | Where to write `backup-<timestamp>-part-N/` folders. |
| `max_size` | Per-part size limit. Accepts suffixes `k`, `m`, `g` (e.g. `4g`). |
| `timestamp` | Snapshot date (default: today). |
| `--full` | Ignore the packaging-state cutoff and include every snapshot up to `timestamp`. Updates `packaging_state_file` afterward. |
| `--force` | Proceed even if `dest_dir` already exists. |

Without `--full`, package includes only snapshots newer than the last `--full` run (incremental packaging — successive packages combine to cover the same range as one `--full`).

---

### `refresh-state`

Rebuild the state file by walking `destination.directory_full`. Use this if the state file is lost or corrupted — the mirror itself becomes the source of truth.

```
dabbak refresh-state
```

After this, the next `backup` will diff against the rebuilt state and proceed normally.

---

### `config`

Print the effective config as JSON. Useful for verifying what dabbak is reading (especially with `DABBAK_CONFIG` set).

```
dabbak config
```

## Common use cases

### Daily backup via cron

```cron
# Backup every night at 02:00, log everything to a sibling file
0 2 * * *  cd /opt/dabbak && python dabbak.py backup --quiet >> /var/log/dabbak.log 2>&1

# Once a week, prune snapshots older than 90 days
0 3 * * 0  cd /opt/dabbak && python dabbak.py prune --keep-days 90 --force --json >> /var/log/dabbak.log
```

The per-config lockfile guarantees a manual `dabbak backup` and the cron job can't run at the same time.

### Restore a single file as of last Tuesday

```bash
python dabbak.py restore /tmp/recovered -t 2026-05-12 /home/me/Documents/report.docx
```

`/tmp/recovered/home/me/Documents/report.docx` will appear with the version that existed on or before May 12.

### Restore everything in a folder

```bash
python dabbak.py restore /tmp/recovered "/home/me/Projects/myapp/*"
```

(Quote the glob so the shell doesn't expand it locally.)

### Restore a whole subtree as it was a week ago

```bash
python dabbak.py restore /tmp/recovered -t 2026-05-07 /home/me/Documents
```

### Preview a restore without copying

```bash
python dabbak.py restore /tmp/recovered -t 2026-05-12 "*.docx" --dry-run
```

### See what changed in last night's run

```bash
grep -E '^(\+\+|\*\*|--)' /mnt/backup/partial/backup-partial-2026-05-13.log | head
```

Each snapshot folder has its own log; the always-appended `backup-full.log` next to `dabbak.py` is the long-term audit trail (auto-rotated at 10 MB).

### Inspect snapshot history

```bash
python dabbak.py list
```

```
date         files       size  status
2026-05-14   12,431    8.2 GB  ok
2026-05-13   12,429    8.2 GB  ok
2026-05-12   12,428    8.2 GB  incomplete
2026-05-11   12,425    8.2 GB  ok
```

`incomplete` means the run that produced this snapshot was interrupted; `restore` and `package` skip it automatically.

### Recover after a lost state file

```bash
python dabbak.py refresh-state
python dabbak.py backup
```

The first command rebuilds state from the mirror; the second resumes normal incremental operation.

### Build an offline archive for a 4 GB DVD set

```bash
python dabbak.py package /tmp/dvd-set 4g --full
```

Produces `/tmp/dvd-set/backup-<date>-part-1/`, `part-2/`, etc., each ≤ 4 GB. The `--full` flag also records this run as the new packaging cutoff, so the next `package` run (without `--full`) will start from here.

### Multiple backup sets on one machine

```bash
# Backup the laptop config
DABBAK_CONFIG=backup-laptop.json python dabbak.py backup

# Backup the photos config (uses its own state file, its own lockfile)
DABBAK_CONFIG=backup-photos.json python dabbak.py backup
```

Each config has its own state file → its own lock → the two can run concurrently.

## How it works (one-page tour)

- **State** (`full_state_file`) is a JSON dict `{absolute_path: [size, mtime]}`. A file is "changed" if size differs, or if mtime differs by ≥ 2 seconds (the tolerance absorbs FAT/exFAT/SMB rounding).
- **Backup pass** walks every configured source. For each file: if missing from state → `++` copy to mirror *and* today's snapshot; if state mismatch → `**` copy to both; otherwise unchanged. After the walk, files present in state but not on disk → `--` deleted from mirror.
- **Mirror layout**: a source path `/home/me/Docs/x.txt` is stored under `directory_full/Docs/x.txt`. The last path component of the source directory becomes the top-level folder in the backup (this is why two sources `/home/a/Docs` and `/home/b/Docs` would collide — give them distinct trailing names).
- **Snapshot layout**: same shape as the mirror, but only files that changed on that day. Plus a `__state.json` file capturing the manifest for that point in time.
- **Restore** loads `__state.json` from the requested date's snapshot as the manifest, then for each listed file searches the snapshot history newest-first for the most recent copy.
- **Failure semantics**: any exception (or Ctrl-C) during the walk skips the deletion pass and merges state instead of overwriting it, so the next run resumes cleanly. The interrupted snapshot folder is marked `__incomplete` and ignored by restore/package.
- **Concurrency**: every write command acquires a per-config kernel lock (`fcntl.flock` / `msvcrt.locking`) that auto-releases on process death.
- **Windows long paths**: every filesystem syscall is routed through wrappers that prepend `\\?\` to bypass MAX_PATH=260, so >260-char paths work natively.

## Files dabbak writes

- `<full_state_file>` — the JSON state map.
- `<full_state_file>.tmp` — exists only briefly during atomic state write.
- `<full_state_file>.lock` — the per-config exclusive lock; contains pid + timestamp for diagnostics.
- `destination.directory_full/...` — the always-current mirror.
- `destination.directory_partial/YYYY-MM-DD/` — dated snapshots, each containing changed-files-only + `__state.json` (+ `__incomplete` if the run failed).
- `destination.directory_partial/backup-partial-YYYY-MM-DD.log` — per-day log.
- `backup-full.log` next to `dabbak.py` — append-only audit log, auto-rotates to `.log.1` at 10 MB.
- `<base_dir>/<packaging_state_file>` — last `package --full` timestamp.

## Copying the backup off-host (preserving mtimes)

`dabbak.py backup` and `dabbak.py restore` both use `shutil.copy2`, which preserves file modification times. So the mtimes on files in `directory_full` and inside each dated snapshot match the source mtimes as of the last time those files were copied.

If you ever copy `directory_full` manually — e.g. to bring a backup over to a new machine, or to clone it to a second drive — **make sure your copy tool preserves mtimes**. If it doesn't, every file gets the copy time as its new mtime. That's not data loss, but if you later turn the copied tree back into a dabbak source, the very next backup will see every file as "changed" (mtime differs from state) and re-copy every byte unnecessarily.

| Platform | Use | Avoid |
|---|---|---|
| Linux / macOS | `rsync -a SRC/ DST/`  (preserves mtimes, perms, ownership) | plain `cp` (resets mtime) |
| Linux / macOS | `cp -a SRC DST`  or  `cp -p` | `cp` without flags |
| Linux / macOS, over SSH | `scp -p SRC user@host:DST`  or  `rsync -a -e ssh SRC/ user@host:DST/` | `scp` without `-p` |
| Linux / macOS, archive form | `tar c SRC \| ssh host 'tar x -C DST'` | — |
| Windows | `robocopy SRC DST /E /COPY:DAT /DCOPY:T`  (preserves file and directory timestamps; the defaults are usually enough but the explicit flags make it bulletproof) | `xcopy` without `/D /Y`; Explorer drag-drop across volumes |
| Windows | `xcopy SRC DST /E /H /K /Y` *with* `/D` for incremental copies | plain `xcopy` |
| Windows ↔ Linux | `rsync -a` (via WSL or Cygwin) | network drag-drop |

Tip: a quick sanity check after copying — `ls -l --time-style=full-iso` on one or two known-old files at both the source and destination should show identical mtimes. If they show today's date at the destination, the copy didn't preserve them.

If you've already done a copy that lost mtimes and you want to bring the new location back into a dabbak workflow:

1. Point dabbak at the new location and run `refresh-state` to rebuild state from the mtime-reset tree (state now reflects "now").
2. Run `backup`. The state-derived mtimes match the on-disk mtimes, so dabbak won't trigger spurious re-copies on this first run.

The cost is one full re-copy step lost — the new mirror won't carry the original source mtimes, but everything from this point forward will be tracked correctly.

## Limitations

- **Symlinks and junctions are skipped** silently. If you have meaningful symlinks in your backup tree, they won't be preserved.
- **Empty directories aren't preserved**. State tracks files only.
- **File metadata**: `shutil.copy2` is used, which preserves mtime and basic permissions. ACLs, xattrs, ownership (uid/gid), Windows ADS streams, and resource forks are *not* preserved.
- **No compression, encryption, or block-level dedup**. A small change in a large file means another full copy in that day's snapshot.
- **No verify command** to check the mirror against the source byte-for-byte. The state-file diff catches size + mtime changes but won't detect silent bit-rot on the backup drive.
- **Two sources with the same last path component collide** in the mirror (`/home/a/Docs` and `/home/b/Docs` both map to `Docs/...`). Rename or restructure to give each source a distinct top-level name.

## Tests

```bash
python -m unittest discover -s tests
```

The test suite is stdlib-only and runs identically on Linux, macOS, and Windows.

## License

See repository for license terms.
