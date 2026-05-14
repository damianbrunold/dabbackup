# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

`dabbak` is a single-file Python backup tool (`dabbak.py`, stdlib only, no dependencies). It maintains one always-current "full" mirror plus dated "partial" snapshots of incremental changes, allowing point-in-time restore.

## Commands

Run from the repository root (where `dabbak.py` and the config live):

```bash
python dabbak.py backup [--dry-run]
python dabbak.py restore <dest-dir> [<yyyy-mm-dd> [<source-path>]]
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
