# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

`dabbak` is a single-file Scheme utility (`dabbak.scm`) for incremental backups: it maintains one always-current "full" mirror plus dated "partial" snapshots of incremental changes, allowing point-in-time restore. It runs under the dabscm interpreter (`scm`, or `scmj` for the Java build) and uses only that runtime's standard libraries. Tests live in `test-dabbak.scm`.

The tool was originally a Python script with a Tkinter GUI. The Scheme port replaced it on `main`; the last Python state (including the GUI) is preserved on the `legacy-python` branch. The Scheme port is **drop-in compatible** with the Python version: same config file, same JSON state format, same snapshot layout, same CLI surface — you can point it at a backup destination the Python tool produced and vice versa.

## Running

```
scm dabbak.scm <command> [options]
```

Commands: `init`, `backup`, `restore`, `list`, `prune`, `package`, `refresh-state`, `config`. Config is selected by the `DABBAK_CONFIG` env var (default `backup-config.json`), resolved relative to the directory of `dabbak.scm` (`base-dir`). The script runs `main` on load via `(main (cdr (command-line)))`.

There is **no `gui` command** — the dabscm runtime ships no windowing toolkit. Use the `legacy-python` branch for the Tkinter GUI.

## Tests

The suite is **black-box** (like `test-dabsync.scm` in the sibling dabsync project): it copies `dabbak.scm` into a fresh temp directory (so the script's own folder becomes the config/`base-dir`, as in a real install), writes a config there, invokes the script as a subprocess (via `run-program/capture` from `(scm system)`) and asserts on the resulting filesystem, exit codes and captured output. It does not import dabbak's internals. The interpreter is chosen to match the one running the tests (`sys-scm-technology`), overridable with `DABBAK_SCM` / `DABBAK_SCRIPT`.

```
scm  test-dabbak.scm
scmj test-dabbak.scm
```

## Architecture

`dabbak.scm` is self-contained, organized top-to-bottom as: small utilities, fnmatch/excludes, walk, config/state, path-prefix logic, locking, logging, the backup engine, restore/package/list/prune/refresh-state/init/config, then the CLI.

### JSON

dabscm's built-in `(scm json)` reader returns objects as opaque `JsonObject`s queried by *known* attribute name — it cannot enumerate an object's keys, which is fatal for a state file `{path: [size, mtime]}` whose path keys are dynamic. dabbak therefore uses **`(scm json simple)`** — a high-level codec added to dabscm for exactly this: `json-parse` (text → sexp), `json-write`/`json-write-pretty` (sexp → text), and `json-ref` for object lookup. Objects map to alists with string keys, arrays to vectors, null to the symbol `'null`; `json-write-pretty` is byte-compatible with Python's `json.dump(indent=2)`. The state alist is loaded into a SRFI-69 hashtable for O(1) lookups during the walk and written back key-sorted. (Originally this library was carried inline in `dabbak.scm`; it was extracted into the runtime.)

Representation (the answer to "hashtables or alists for objects?"):
- JSON **object → alist** `(("k" . v) ...)`, source order preserved (`'()` = `{}`)
- JSON **array → vector** `#(v ...)` — keeps objects and arrays unambiguous (a list is always an object, a vector always an array)
- string → string; integral number → exact integer; fractional/exponent → inexact real; `true`/`false` → `#t`/`#f`; `null` → the symbol `'null`

`json-write-pretty` produces 2-space-indent output byte-compatible with Python's `json.dump(indent=2)`. The **backup engine** loads the state alist into a **SRFI-69 hashtable** for O(1) lookups during the walk (an alist would be O(n²) over large trees) and writes it back as a key-sorted alist. So: alists at the data-interchange boundary, a hashtable as the working index.

### State and change detection

State (`full_state_file`) is a JSON object `{absolute_path: [size, seconds]}`. **mtime is stored in seconds** (`file-modification-timestamp` returns milliseconds in dabscm, so the engine divides by 1000) to match the Python format byte-for-byte — this is what makes the state files interoperable. A file is "changed" if size differs or mtime differs by ≥ 2 seconds (`mtime-changed?`, absorbing FAT/exFAT/SMB rounding). `copy-file` preserves mtime so runs are idempotent.

### Backup engine (`make-backup`)

Walks the union of configured sources, diffs each file against state, copies new/changed files into **both** `dest_full` (mirror) and `dest_partial/<today>/` (dated snapshot), then — only if the walk completed — deletes from both destinations any file in state but missing from the source. The walk runs inside a `guard`; any error (the runtime's nearest analogue to Ctrl-C) leaves `completed = #f`, which **skips the deletion pass and merges state** (old entries for unvisited paths preserved) and drops an `__incomplete` marker into the snapshot so `restore`/`package` ignore it. On success, the state is copied into the snapshot as `__state.json`. State is written atomically via tmp + `move-file` (rename). A changed file whose copy fails carries its OLD `[size, mtime]` forward so the next run retries; a failed new file stays out of state entirely.

Output via `plog`: `++` new, `**` changed, `--` deleted, routed to `backup-full.log` (rotated at 10 MB) and `backup-partial-<date>.log` always; stdout is gated by `--quiet` (suppresses info/file lines) and `--json` (suppresses everything but the final stats JSON). A lightweight `Progress` writes a throttled tally to stderr (so it never mixes with `--json` on stdout).

### Path layout

Files are stored under destinations using `filepath[prefixlen:]` where `prefix = dirname(sourcedir)`, so the last component of each source directory becomes the top-level folder in the backup. `compute-prefixlen` centralizes this; `find-source-prefix` maps a state-key path back to its prefix for restore/package (handling `*` wildcard sources and Windows-formatted state paths read on POSIX via the `is-windows` flag). `expand-source-dirs` expands a trailing-`*` source one level into its child directories (skipping symlinks).

### Excludes (gitignore-flavored)

`compile-excludes` classifies each entry: no slash → match basename anywhere; slash + glob → match the full path; slash, no glob → exact absolute-path match. Globbing is done by `fnmatch->regex` over `string-matches` (not `(scm glob)`), so `*` matches path separators exactly as Python's `fnmatch` does; matching is case-insensitive on Windows.

### Locking

The per-config lock (`<full_state_file>.lock`, used by `backup`/`package`/`refresh-state`/`prune --force`) uses the **`(scm fs)` `file-lock`/`file-unlock`** primitives — a kernel-managed advisory lock (FileShare.None on .NET, `FileChannel.tryLock` on the JVM). `with-lock` acquires it (or exits 1 if another process holds it), runs the thunk, and releases it. Because the OS frees the lock when the holding process exits, a crashed run leaves **no stale lock** — the leftover empty `.lock` file is not the signal and is harmless. (This replaced an earlier best-effort existence lock that could go stale; `make-directory` is already recursive in dabscm, so dabbak calls it directly rather than a hand-rolled `mkdir -p`.)

### Filesystem notes

`(scm fs)` handles Windows long paths internally (no `_long`/`\\?\` equivalent needed). `delete-directory` is **recursive** (serves as `rmtree` for prune; ancestor-pruning in `remove-file-pruning` guards on emptiness first). `make-directory` is already `mkdir -p` (recursive), so dabbak calls it directly. `norm-path` wraps `normalized-path` to strip a trailing separator, matching Python `os.path.normpath`. Symlinks are skipped during the walk (type `'symlink`), as in the Python original.

## Config shape

```json
{
  "source": { "directories": [...], "excludes": [...], "is-windows": bool },
  "destination": { "directory_full": "...", "directory_partial": "..." },
  "full_state_file": "...",
  "packaging_state_file": "..."
}
```
