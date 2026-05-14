# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

`dabbak` is a single-file Python backup tool (`dabbak.py`, stdlib only, no dependencies). It maintains one always-current "full" mirror plus dated "partial" snapshots of incremental changes, allowing point-in-time restore.

## Commands

Run from the repository root (where `dabbak.py` and the config live):

```bash
python dabbak.py init                                     # write a config template
python dabbak.py backup [--dry-run] [--quiet] [--json]
python dabbak.py restore <dest-dir> [-t <yyyy-mm-dd>] [pattern ...] [--dry-run] [--force]
python dabbak.py list [--json]
python dabbak.py prune --keep-last N | --keep-days N [--force] [--json]
python dabbak.py package <dest-dir> <max-size> [<yyyy-mm-dd>] [--full] [--force]
python dabbak.py refresh-state
python dabbak.py config        # dump effective config
python -m unittest discover -s tests   # run test suite (stdlib only)
```

- `max-size` accepts suffixes `k`, `m`, `g` (e.g. `4g`).
- Config is selected by the `DABBAK_CONFIG` env var (default: `backup-config.json`), resolved relative to the directory of `dabbak.py`. Multiple config files coexist in the repo (`backup-config-damian.json`, `backup-config-rahel.json`, template).
- CLI uses `argparse`; `--help` works per subcommand.

## Architecture

**State-driven incremental backup.** A single JSON file (`full_state_file` in config) maps each source `filepath -> [size, mtime]`. A file is considered changed when size differs or when mtime differs by ≥ `MTIME_TOLERANCE_SECONDS` (2s, to absorb FAT/exFAT/SMB rounding). `make_backup` walks all configured sources, diffs against state, writes changed/new files to **both** `dest_full` (mirror) and `dest_partial/<today>/` (dated snapshot), then deletes files missing from the source from both destinations. State is rewritten at the end and copied into the partial snapshot as `__state.json`.

**Path layout.** Files are stored under destinations using `filepath[prefixlen:]`, where `prefix = os.path.dirname(sourcedir)`. So the last path component of each source directory is preserved as the top-level folder in the backup. The `prefixlen` calculation is centralized in `compute_prefixlen()` and used by `make_backup`, `restore`, and `refresh_state`. `find_source_prefix(config, fullpath)` maps a state-key path back to its prefix (used by restore/package); it handles wildcard sources and Windows-formatted state paths read on POSIX.

**Failure semantics.** `make_backup` tracks a `completed` flag. On success: deletion pass runs, state is rewritten, `__state.json` is copied into the dated snapshot. On any exception (including `KeyboardInterrupt`): the deletion pass is **skipped**, state is **merged** (old entries for unvisited paths preserved, new/updated entries from `new_state` win), and the snapshot folder gets an `__incomplete` marker so `restore`/`package` ignore it. State is always written atomically via tmp + fsync + `os.replace`.

**Windows long paths.** All filesystem syscalls go through `fs_*` wrappers that prepend `\\?\` (or `\\?\UNC\` for UNC) on Windows via `_long()`. Bare paths remain as dictionary keys in state — only the syscall site sees the prefixed form. When adding any new fs operation, use the `fs_*` wrapper, not raw `os.*` / `shutil.*`.

**Stats / output.** `make_backup` accumulates a `stats` dict (new/changed/deleted/unchanged/failed counts + bytes_copied + elapsed_seconds + completed + dry_run) and returns it. Output is gated by three flags:
- default: per-file `++ ** --` lines on stdout + summary
- `--quiet`: warnings + summary only
- `--json`: nothing on stdout except a JSON dump of `stats` at the end

Live progress goes to stderr (so it never mixes with `--json` on stdout). `Progress` uses `len(prev_state)` and the sum of recorded sizes as denominator estimates — no pre-scan. First-ever run shows just a running tally.

**Retention.** `cmd_prune` deletes whole snapshot folders (and their `backup-partial-<date>.log`) based on `--keep-last N` and/or `--keep-days N` (union of policies). Today's snapshot is always kept. Dry-run unless `--force`.

**Reliability invariant for state.** A file's entry in `new_state` is updated ONLY when both partial and full copies succeeded. On copy failure for a *changed* file, the OLD `[size, mtime]` is carried over so the next run still sees a diff and retries. New files that fail to copy stay out of state entirely so the next run treats them as new again. This is what makes failed files self-healing across runs.

**Source expansion.** A source path ending in `*` is expanded one level: e.g. `/home/users/*` becomes each immediate child directory. Excludes are absolute normalized paths and short-circuit `walk()`. Symlinks and junctions are skipped.

**Restore.** Given a target date, lists `dest_partial` directories in reverse order, filters to those `<= timestamp`, loads `__state.json` from the most recent one as the file manifest, then for each file walks the history newest-first and copies the first matching version found. `source_path` argument filters which files to restore by prefix.

**Package.** Builds chunked archives (size-limited folder sets named `backup-<ts>-part-N`) for offline storage. Without `--full`, only snapshots **after** the timestamp recorded in `packaging_state_file` are included (incremental packaging); `--full` ignores the cutoff and updates that state file afterward.

**refresh-state.** Rebuilds `full_state_file` by walking `dest_full` instead of the live sources — used to recover state if the state file is lost or corrupted, mapping mirror paths back to their original source paths.

**Logging.** Two parallel logs: `backup-full.log` (appended forever, in repo root) and `backup-partial-<date>.log` (in `dest_partial_base`). The `plog()` helper routes each line to "full", "partial", or both. Prefixes in logs: `++` new, `**` changed, `--` deleted.

**Windows interop.** When `source.is-windows` is true and running on POSIX (running the tool against a mounted Windows backup), source dirs may use `\` separators; `find_source_prefix` handles the mixed case. Be careful preserving this when touching path handling.

## Config shape

```json
{
  "source": { "directories": [...], "excludes": [...], "is-windows": bool },
  "destination": { "directory_full": "...", "directory_partial": "..." },
  "full_state_file": "...",
  "packaging_state_file": "..."
}
```
