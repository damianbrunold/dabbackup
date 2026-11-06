# dabbak

A small, dependency-free Scheme backup tool that keeps an always-current mirror of your files plus dated incremental snapshots, so you can restore both "the latest version" and "this file as it was last Tuesday."

Single self-contained file, runs under the [dabscm](https://github.com/dab/dabscm) interpreter (`scm`, or `scmj` for the Java build) using only its standard libraries. Runs on Linux, macOS, and Windows. Originally a Python tool; the Scheme port is drop-in compatible with it (same config, same JSON state format, same snapshot layout). The Python version — including its Tkinter GUI — lives on the `legacy-python` branch.

## What it does

Each time you run `dabbak backup`, it walks your configured source directories and produces two outputs:

- **A full mirror** at `destination.directory_full` — always reflects the current state of your sources. One file in, one file out. Restoring "the latest version" of anything is just a normal file copy from here.
- **A dated partial snapshot** at `destination.directory_partial/YYYY-MM-DD/` — contains only the files that were *new or changed* on that run. Together with all prior snapshots, this lets you reconstruct your sources as they were on any past date.

A JSON state file remembers each file's size + mtime between runs, so dabbak knows what changed without re-reading every byte.

## Quick start

```bash
# 1. Drop dabbak.scm somewhere convenient
# 2. Create a config file next to it
scm dabbak.scm init

# 3. Edit backup-config.json to point at your real sources/destinations
# 4. Run a backup
scm dabbak.scm backup
```

## Requirements

- The dabscm interpreter on your `PATH` as `scm` (C# build) or `scmj` (Java build).
- No third-party packages; only the dabscm standard libraries are used.

## Setup

1. **Place `dabbak.scm`** somewhere convenient (e.g. `~/bin/`, `/opt/dabbak/`, or your home directory).

2. **Generate a config template:**

   ```bash
   scm dabbak.scm init
   ```

   This creates `backup-config.json` next to `dabbak.scm`. You can name it anything with `--name foo.json`.

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
   scm dabbak.scm config
   ```

5. **First backup** (this initial run copies *everything* — subsequent runs only handle changes):

   ```bash
   scm dabbak.scm backup
   ```

### Using multiple configs

dabbak reads `backup-config.json` by default. To use a different file, set the `DABBAK_CONFIG` env var:

```bash
DABBAK_CONFIG=backup-config-laptop.json scm dabbak.scm backup
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
| `packaging_state_file` | str | Relative path (resolved next to `dabbak.scm`) where `package --full` records its last full-package timestamp. |

### Exclude syntax

Excludes are gitignore-flavored:

| Form | Example | Meaning |
|---|---|---|
| No slash | `__pycache__`, `*.pyc`, `node_modules` | Match against the **basename** anywhere in the tree. Globs (`*`, `?`, `[…]`) supported. |
| Slash + glob | `**/build/*`, `*/.cache/*` | Match against the **full path** via `fnmatch`. |
| Slash, no glob | `/home/me/.cache` | Exact **absolute path** match (legacy form, still supported). |

Matching is case-insensitive on Windows to match the filesystem.

## Commands

All commands are subcommands of `dabbak.scm`. Every subcommand supports `--help`.

### `init`

Create a config template next to `dabbak.scm`.

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

**Failure handling:** if dabbak is interrupted mid-run (Ctrl-C, USB unplug, network share drop), state is *merged* rather than truncated — entries for paths not yet visited are preserved from the previous state. The deletion pass is skipped. Today's snapshot folder gets an `__incomplete` marker so `restore` and `package` never use it as a restore point (the files it did copy are still found by later restores). Re-running `backup` resumes naturally.

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

**How dates are resolved:** dabbak loads `__state.json` from the newest *complete* snapshot at or before the requested date and uses it as the file manifest. Snapshots with an `__incomplete` or `__squashed` marker are never used as the manifest. For each listed file, it walks the snapshot history newest-first, from that snapshot back, and copies the first version it finds. Incomplete and squash-source snapshots are included in that search, because the files they hold are versions the later manifest refers to.

---

### `list`

Show the partial snapshots that exist.

```
dabbak list [--json]
```

Prints a table of `date / files / size / status` (where `status` is `ok`, `incomplete`, or `squash pending (source|target)` after an interrupted squash). `--json` emits a machine-readable array.

---

### `prune`

Thin out old partial snapshots according to a retention policy.

```
dabbak prune [--keep-last N] [--keep-days N] [--keep-monthly N] [--keep-yearly N] [--force] [--json]
```

| Flag | Description |
|---|---|
| `--keep-last N` | Keep the N most recent snapshots. |
| `--keep-days N` | Keep snapshots dated within the last N days. |
| `--keep-monthly N` | Keep the last snapshot of each of the N most recent months (`all` = every month). |
| `--keep-yearly N` | Keep the last snapshot of each of the N most recent years (`all` = every year). |
| `--force` | Actually prune. Without this, prune is a **dry-run** that prints what it would do. |
| `--json` | JSON output (`kept`, `deleted`, `would_delete`, `squashed_into`, `bytes_freed`, `force`). |

A snapshot is kept if it satisfies **any** policy (union, not intersection). Today's snapshot and the newest complete snapshot are always kept. Only complete snapshots count as kept: incomplete ones are always folded into the next kept snapshot.

Snapshots that aren't kept are **squashed into the next kept snapshot** (see `squash` below), not simply deleted. A partial snapshot holds only the files that changed that day, so deleting one would lose the only copy of every file that hasn't changed since. Restore at every kept date gives the same result before and after pruning. The removed snapshot's `backup-partial-YYYY-MM-DD.log` is prepended to the kept snapshot's log under a `== squashed from YYYY-MM-DD ==` header.

You must specify at least one `--keep-*` option. A typical policy is "daily for 3 months, then monthly for 2 years, then yearly forever":

```
dabbak prune --keep-days 90 --keep-monthly 24 --keep-yearly all --force
```

---

### `squash`

Merge every snapshot in a date range into the newest complete snapshot in that range. Example: compact all of 2025 into one snapshot, dated at the last backup of 2025.

```
dabbak squash --from YYYY-MM-DD --to YYYY-MM-DD [--force] [--json]
```

For each file in the merged snapshots, processed newest-first:

- if the target already has a copy (one it had, or a newer one already moved in), the older copy is **dropped**;
- if the target's `__state.json` still lists the file, it is **moved** into the target (a rename, so no data is copied);
- otherwise the file was deleted from the source before the target date, nothing at or after the target can need it, and it is **dropped**.

Restore points inside the range are gone afterwards. Points before the range, and points at or after the target, give exactly the same result as before. The space you get back is every intermediate version within the range plus every file deleted within it. Data that changes a lot shrinks a lot. Data that only ever grows (e.g. a photo archive) shrinks very little, because the latest version of every live file has to stay somewhere.

Incomplete snapshots inside the range are folded in like any other. Incomplete snapshots newer than the target are left alone. Without `--force` it is a dry-run that reports what would be moved, dropped and freed.

**Crash safety:** before anything moves, the target gets a `__squash.json` job file and each source gets a `__squashed` marker. Sources then stop being restore points, but restore still searches them for copies, so a restore at the target is correct at every intermediate step. If a squash is interrupted, `list` shows `squash pending`, and the next `squash --force` or `prune --force` finishes the job first.

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

---

### GUI

The Scheme port is **CLI-only** — the dabscm runtime ships no windowing toolkit. The Python version's Tkinter GUI (Backup / Restore / Settings tabs) is preserved on the `legacy-python` branch and shares the same config, state and lockfile as this CLI, so you can run it on the same backup destination if you need a graphical front-end.

## Common use cases

### Daily backup via cron

```cron
# Backup every night at 02:00, log everything to a sibling file
0 2 * * *  cd /opt/dabbak && scm dabbak.scm backup --quiet >> /var/log/dabbak.log 2>&1

# Once a week: keep 90 days of daily snapshots, then one per month for two
# years, then one per year (older snapshots are squashed, not just deleted)
0 3 * * 0  cd /opt/dabbak && scm dabbak.scm prune --keep-days 90 --keep-monthly 24 --keep-yearly all --force --json >> /var/log/dabbak.log
```

The per-config lockfile guarantees a manual `dabbak backup` and the cron job can't run at the same time.

### Restore a single file as of last Tuesday

```bash
scm dabbak.scm restore /tmp/recovered -t 2026-05-12 /home/me/Documents/report.docx
```

`/tmp/recovered/home/me/Documents/report.docx` will appear with the version that existed on or before May 12.

### Restore everything in a folder

```bash
scm dabbak.scm restore /tmp/recovered "/home/me/Projects/myapp/*"
```

(Quote the glob so the shell doesn't expand it locally.)

### Restore a whole subtree as it was a week ago

```bash
scm dabbak.scm restore /tmp/recovered -t 2026-05-07 /home/me/Documents
```

### Preview a restore without copying

```bash
scm dabbak.scm restore /tmp/recovered -t 2026-05-12 "*.docx" --dry-run
```

### See what changed in last night's run

```bash
grep -E '^(\+\+|\*\*|--)' /mnt/backup/partial/backup-partial-2026-05-13.log | head
```

Each snapshot folder has its own log; the always-appended `backup-full.log` next to `dabbak.scm` is the long-term audit trail (auto-rotated at 10 MB).

### Inspect snapshot history

```bash
scm dabbak.scm list
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
scm dabbak.scm refresh-state
scm dabbak.scm backup
```

The first command rebuilds state from the mirror; the second resumes normal incremental operation.

### Build an offline archive for a 4 GB DVD set

```bash
scm dabbak.scm package /tmp/dvd-set 4g --full
```

Produces `/tmp/dvd-set/backup-<date>-part-1/`, `part-2/`, etc., each ≤ 4 GB. The `--full` flag also records this run as the new packaging cutoff, so the next `package` run (without `--full`) will start from here.

### Multiple backup sets on one machine

```bash
# Backup the laptop config
DABBAK_CONFIG=backup-laptop.json scm dabbak.scm backup

# Backup the photos config (uses its own state file, its own lockfile)
DABBAK_CONFIG=backup-photos.json scm dabbak.scm backup
```

Each config has its own state file → its own lock → the two can run concurrently.

### Keeping the history on external media

A common setup is to copy the partial history to an external drive with [dabsync](https://github.com/dab/dabsync):

```bash
scm dabsync.scm copy /backup/partial /media/usb/partial
```

`dabsync copy` only adds and updates files and **never deletes**. So pruning or squashing locally does **not** remove anything from the external copy:

- The pruned snapshot folders and their logs stay on the media.
- The kept snapshot also gets the files that prune moved into it locally, plus its updated log. Each moved file then exists twice on the media.
- The media keeps the full history and stays consistent: a restore against it gives the same result at every date as before. It just doesn't get any smaller.

Before copying, make sure no squash is pending (`dabbak list` must not show `squash pending`). If one is, run `prune --force` or `squash --force` first so it finishes. Otherwise `copy` would carry the temporary `__squashed` / `__squash.json` markers to the media and never remove them.

To shrink the media as well, choose one of two ways:

1. **Mirror instead of copy.** `scm dabsync.scm sync /backup/partial /media/usb/partial` makes the media match local exactly, pruned folders included. Only use this if the media should never hold more history than the local disk.
2. **Prune the media with its own retention** (recommended when the media is the long-term archive). Make a second config that is identical to the normal one except that `destination.directory_partial` points at the media. Prune only needs each snapshot's `__state.json` and the configured source directories:

   ```bash
   # local: 90 days of dailies; media: monthly for 2 years, then yearly forever
   scm dabbak.scm prune --keep-days 90 --force
   scm dabsync.scm copy /backup/partial /media/usb/partial
   DABBAK_CONFIG=backup-usb.json scm dabbak.scm prune --keep-days 90 --keep-monthly 24 --keep-yearly all --force
   ```

   Make the media policy keep at least what the local policy keeps (here `--keep-days 90` on both). Otherwise the next `copy` brings back a snapshot the media just pruned but local still has, and every cycle copies and squashes it again.

   The same config also restores from the media (`DABBAK_CONFIG=backup-usb.json scm dabbak.scm restore ...`). It shares `full_state_file` with the normal config, and with it the lock, so a media prune and a local backup can never run at the same time. Don't run `backup` with this config, because that would write a new snapshot to the media.

## How it works (one-page tour)

- **State** (`full_state_file`) is a JSON dict `{absolute_path: [size, mtime]}`. A file is "changed" if size differs, or if mtime differs by ≥ 2 seconds (the tolerance absorbs FAT/exFAT/SMB rounding).
- **Backup pass** walks every configured source. For each file: if missing from state → `++` copy to mirror *and* today's snapshot; if state mismatch → `**` copy to both; otherwise unchanged. After the walk, files present in state but not on disk → `--` deleted from mirror.
- **Mirror layout**: a source path `/home/me/Docs/x.txt` is stored under `directory_full/Docs/x.txt`. The last path component of the source directory becomes the top-level folder in the backup (this is why two sources `/home/a/Docs` and `/home/b/Docs` would collide — give them distinct trailing names).
- **Snapshot layout**: same shape as the mirror, but only files that changed on that day. Plus a `__state.json` file capturing the manifest for that point in time.
- **Restore** loads `__state.json` from the requested date's snapshot as the manifest, then for each listed file searches the snapshot history newest-first for the most recent copy.
- **Failure semantics**: any exception (or Ctrl-C) during the walk skips the deletion pass and merges state instead of overwriting it, so the next run resumes cleanly. The interrupted snapshot folder is marked `__incomplete`. It is never a restore point, but its files are still found by later restores.
- **Squash / prune** merge old snapshots forward into a later kept one: the newest copy of each file the kept snapshot's manifest still lists is moved in, and everything else is dropped. Every restore point at or after the kept snapshot stays the same.
- **Concurrency**: every write command (`backup`, `package`, `refresh-state`, `prune --force`, `squash --force`) acquires a per-config kernel advisory lock at `<full_state_file>.lock` (via dabscm's `file-lock` primitive). A second run sees a clear error instead of racing, and the OS releases the lock automatically when the process exits — so a crash never leaves a stale lock (the leftover empty `.lock` file is harmless).
- **Windows long paths**: handled inside the `(scm fs)` primitives, so >260-char paths work natively with no `\\?\` plumbing in the tool itself.

## Files dabbak writes

- `<full_state_file>` — the JSON state map.
- `<full_state_file>.tmp` — exists only briefly during atomic state write.
- `<full_state_file>.lock` — the per-config exclusive lock; contains pid + timestamp for diagnostics.
- `destination.directory_full/...` — the always-current mirror.
- `destination.directory_partial/YYYY-MM-DD/` — dated snapshots, each containing changed-files-only + `__state.json` (+ `__incomplete` if the run failed; `__squash.json` / `__squashed` only while a squash is in progress or was interrupted).
- `destination.directory_partial/backup-partial-YYYY-MM-DD.log` — per-day log.
- `backup-full.log` next to `dabbak.scm` — append-only audit log, auto-rotates to `.log.1` at 10 MB.
- `<base_dir>/<packaging_state_file>` — last `package --full` timestamp.

## Copying the backup off-host (preserving mtimes)

`dabbak backup` and `dabbak restore` both copy with `copy-file`, which preserves file modification times. So the mtimes on files in `directory_full` and inside each dated snapshot match the source mtimes as of the last time those files were copied.

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
- **File metadata**: `copy-file` preserves mtime and file contents. ACLs, xattrs, ownership (uid/gid), Windows ADS streams, and resource forks are *not* preserved.
- **No compression, encryption, or block-level dedup**. A small change in a large file means another full copy in that day's snapshot.
- **No verify command** to check the mirror against the source byte-for-byte. The state-file diff catches size + mtime changes but won't detect silent bit-rot on the backup drive.
- **Two sources with the same last path component collide** in the mirror (`/home/a/Docs` and `/home/b/Docs` both map to `Docs/...`). Rename or restructure to give each source a distinct top-level name.

## Tests

The suite is black-box: it invokes `dabbak.scm` as a subprocess and asserts on the resulting filesystem, exit codes and output (it does not import the script's internals).

```bash
scm  test-dabbak.scm
scmj test-dabbak.scm   # Java build
```

It runs identically under both dabscm interpreters on Linux, macOS, and Windows.

## License

See repository for license terms.
