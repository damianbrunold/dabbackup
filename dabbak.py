import argparse
import datetime
import json
import os
import shutil
import sys


# Filesystem mtime granularity tolerance: FAT/exFAT/SMB store mtime with 2s
# precision, so a freshly-copied file can read back with a 1-2s drift even
# when its contents are unchanged. Treat differences below this as equal.
MTIME_TOLERANCE_SECONDS = 2


def mtime_changed(a, b):
    return abs(int(a) - int(b)) >= MTIME_TOLERANCE_SECONDS


class Progress:
    """Periodic progress on stderr. Estimates totals from the previous
    state file (len(state), sum of sizes) so we don't pay for a pre-scan.
    On the first run the denominators are unknown and we print just the
    running tally. Throttled to ~once per second; rendered on a single
    line via \\r when stderr is a tty, else a heartbeat line every 30s.
    """

    def __init__(self, prev_state, enabled, interval=1.0):
        self.files_total = len(prev_state) if prev_state else 0
        self.bytes_total = sum(v[0] for v in prev_state.values()) if prev_state else 0
        self.files_done = 0
        self.bytes_done = 0
        self.last_t = 0.0
        self.interval = interval
        self.enabled = enabled
        self.is_tty = enabled and hasattr(sys.stderr, "isatty") \
            and sys.stderr.isatty()
        if enabled and not self.is_tty:
            # Non-tty: write a heartbeat every 30s instead of every 1s.
            self.interval = 30.0

    def tick(self, filepath, file_size):
        if not self.enabled:
            return
        self.files_done += 1
        self.bytes_done += file_size
        import time
        t = time.monotonic()
        if t - self.last_t < self.interval:
            return
        self.last_t = t
        if self.files_total:
            line = (
                f"[{self.files_done} / ~{self.files_total} files, "
                f"{format_size(self.bytes_done)} / "
                f"~{format_size(self.bytes_total)}]"
            )
        else:
            line = (
                f"[{self.files_done} files, "
                f"{format_size(self.bytes_done)}]"
            )
        tail = filepath
        if self.is_tty:
            try:
                width = os.get_terminal_size(sys.stderr.fileno()).columns
            except (OSError, ValueError):
                width = 80
            msg = f"{line} {tail}"
            if len(msg) > width - 1:
                msg = msg[:width - 1]
            sys.stderr.write("\r" + msg + " " * (width - len(msg) - 1))
            sys.stderr.flush()
        else:
            sys.stderr.write(f"{line} {tail}\n")
            sys.stderr.flush()

    def done(self):
        if self.enabled and self.is_tty:
            try:
                width = os.get_terminal_size(sys.stderr.fileno()).columns
            except (OSError, ValueError):
                width = 80
            sys.stderr.write("\r" + " " * (width - 1) + "\r")
            sys.stderr.flush()


def format_size(n):
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024
    return f"{n:.1f} TB"


def _long(path):
    """Prefix Windows absolute paths with \\\\?\\ to bypass MAX_PATH (260).

    No-op on POSIX. Idempotent. Pass through anything falsy so callers can
    forward args without checking.
    """
    if os.name != "nt" or not path:
        return path
    p = os.path.abspath(path)
    if p.startswith("\\\\?\\"):
        return p
    if p.startswith("\\\\"):
        return "\\\\?\\UNC\\" + p[2:]
    return "\\\\?\\" + p


def fs_stat(p):
    return os.stat(_long(p))


def fs_exists(p):
    return os.path.exists(_long(p))


def fs_isdir(p):
    return os.path.isdir(_long(p))


def fs_isfile(p):
    return os.path.isfile(_long(p))


def fs_islink(p):
    return os.path.islink(_long(p))


def fs_isjunction(p):
    fn = getattr(os.path, "isjunction", None)
    return fn(_long(p)) if fn else False


def fs_listdir(p):
    return os.listdir(_long(p))


def fs_makedirs(p, exist_ok=False):
    return os.makedirs(_long(p), exist_ok=exist_ok)


def fs_remove(p):
    return os.remove(_long(p))


def fs_rmdir(p):
    return os.rmdir(_long(p))


def fs_copy2(src, dst):
    return shutil.copy2(_long(src), _long(dst))


def fs_open(p, *args, **kwargs):
    return open(_long(p), *args, **kwargs)


def base_dir():
    path = os.path.normpath(os.path.abspath(os.path.dirname(sys.argv[0])))
    if os.path.basename(path) == "dabbak":
        path = os.path.dirname(path)
    return path


def get_full_log():
    return os.path.join(base_dir(), "backup-full.log")


# Max size of backup-full.log before rotation. One rotated file is kept
# (foo.log -> foo.log.1, previous foo.log.1 discarded).
FULL_LOG_MAX_BYTES = 10 * 1024 * 1024


def rotate_log_if_large(path, max_bytes=FULL_LOG_MAX_BYTES):
    try:
        if not fs_exists(path):
            return
        if fs_stat(path).st_size < max_bytes:
            return
        rotated = path + ".1"
        if fs_exists(rotated):
            fs_remove(rotated)
        os.replace(_long(path), _long(rotated))
    except Exception:
        # Rotation failure must never block a backup run.
        pass


def get_partial_log(dest_partial_base, today):
    return os.path.join(dest_partial_base, f"backup-partial-{today}.log")


def read_config():
    cfgfile = os.environ.get("DABBAK_CONFIG", "backup-config.json")
    filepath = os.path.join(base_dir(), cfgfile)
    with fs_open(filepath, encoding="utf8") as infile:
        return json.load(infile)


def read_full_state(config):
    return read_full_state_file(config["full_state_file"])


def read_full_state_file(filepath):
    if fs_exists(filepath):
        with fs_open(filepath, encoding="utf8") as infile:
            return json.load(infile)
    else:
        return {}


def write_full_state(config, state):
    write_full_state_file(config["full_state_file"], state)


def write_full_state_file(filepath, state):
    tmp = filepath + ".tmp"
    with fs_open(tmp, "w", encoding="utf8") as outfile:
        json.dump(state, outfile, indent=2, ensure_ascii=False)
        outfile.flush()
        os.fsync(outfile.fileno())
    os.replace(_long(tmp), _long(filepath))


def remove_file(filepath, dest_full):
    try:
        fs_remove(filepath)
        dirpath = os.path.dirname(filepath)
        while dirpath.startswith(dest_full) and not fs_listdir(dirpath):
            fs_rmdir(dirpath)
            dirpath = os.path.dirname(dirpath)
    except Exception:
        print(f"failed to delete {filepath}")


def compile_excludes(excludes):
    """Compile a list of exclude entries into an `is_excluded(path)` test.

    Syntax (gitignore-flavored):
      - no slash  -> match against basename anywhere (globs allowed).
                     `__pycache__` skips any directory of that name;
                     `*.pyc` skips any .pyc file anywhere.
      - has slash, has glob chars -> match against the full path via
                     fnmatch.  `**/build/*` works because fnmatch's `*`
                     matches path separators.
      - has slash, no glob -> exact absolute-path match (legacy form).

    fnmatch is case-insensitive on Windows (uses os.path.normcase), which
    matches the filesystem's own behavior.
    """
    import fnmatch
    basenames = []
    fullpaths = []
    abs_paths = set()
    for raw in excludes:
        has_slash = "/" in raw or "\\" in raw
        has_glob = any(c in raw for c in "*?[")
        if not has_slash:
            basenames.append(raw)
        elif has_glob:
            fullpaths.append(raw)
        else:
            abs_paths.add(os.path.normpath(raw))

    def is_excluded(path):
        if path in abs_paths:
            return True
        base = os.path.basename(path)
        for p in basenames:
            if fnmatch.fnmatch(base, p):
                return True
        for p in fullpaths:
            if fnmatch.fnmatch(path, p):
                return True
        return False

    return is_excluded


def walk(directory, excludes):
    is_excluded = (
        excludes if callable(excludes) else compile_excludes(excludes)
    )
    yield from _walk(directory, is_excluded)


def _walk(directory, is_excluded):
    if fs_isfile(directory):
        if not is_excluded(directory):
            yield directory
        return
    if is_excluded(directory):
        return
    for path in sorted(fs_listdir(directory)):
        fullpath = os.path.join(directory, path)
        if is_excluded(fullpath):
            continue
        if fs_isjunction(fullpath):
            continue
        if fs_islink(fullpath):
            continue
        if fs_isdir(fullpath):
            yield from _walk(fullpath, is_excluded)
        elif fs_isfile(fullpath):
            yield fullpath


def find_source_prefix(config, fullpath):
    """Return the prefix path that, joined with a tail, reconstructs paths
    stored under this source dir. Matches the `prefix = dirname(sourcedir)`
    convention used by make_backup for the *expanded* source dir.

    - plain source `/data/src` -> `/data` (parent of the dir itself)
    - wildcard `/data/users/*` -> `/data/users` (the wildcard's base, which
      is the parent of each expanded child like `/data/users/alice`)

    Handles Windows-formatted state paths read on POSIX (is-windows flag).
    """
    is_windows_state = config["source"].get("is-windows") and os.sep == "/"
    sep = "\\" if is_windows_state else os.sep
    for source_dir in config["source"]["directories"]:
        is_wild = source_dir.endswith("*")
        match_dir = source_dir[:-1] if is_wild else source_dir
        match_dir = match_dir.rstrip(sep)
        if fullpath != match_dir and not fullpath.startswith(match_dir + sep):
            continue
        if is_wild:
            return match_dir
        idx = match_dir.rfind(sep)
        return match_dir[:idx] if idx >= 0 else match_dir
    return None


def compute_prefixlen(prefix):
    if prefix.endswith("\\") or prefix.endswith("/"):
        return len(prefix)
    return len(prefix) + 1


def expand_source_dirs(directories):
    result = []
    for srcdir in directories:
        srcdir = os.path.normpath(srcdir)
        if srcdir.endswith("*") or srcdir.endswith("*" + os.sep):
            base = srcdir.rstrip("*").rstrip(os.sep)
            if not fs_isdir(base):
                continue
            for name in sorted(fs_listdir(base)):
                child = os.path.join(base, name)
                if fs_isdir(child) and not fs_islink(child):
                    result.append(child)
        else:
            result.append(srcdir)
    return result


def make_backup(config, dry_run=False, quiet=False, json_out=False):
    today = datetime.date.today().strftime("%Y-%m-%d")
    rotate_log_if_large(get_full_log())
    if dry_run and not json_out:
        print("DRY RUN: no files will be copied, deleted, or state written")

    source_dirs = expand_source_dirs(config["source"]["directories"])
    source_excludes = list(config["source"]["excludes"])
    is_excluded = compile_excludes(source_excludes)
    dest_full = os.path.normpath(
        config["destination"]["directory_full"]
    )
    dest_partial_base = os.path.normpath(
        config["destination"]["directory_partial"]
    )
    dest_partial = os.path.normpath(os.path.join(dest_partial_base, today))

    if not fs_exists(dest_partial_base):
        fs_makedirs(dest_partial_base, exist_ok=True)

    with fs_open(get_full_log(), "a", encoding="utf8") as full_log, \
            fs_open(get_partial_log(dest_partial_base, today), "a", encoding="utf8") as partial_log:

        def plog(msg, dest="full,partial", level="info"):
            """Log-file output is always written (audit trail). Stdout is
            gated on verbosity: --quiet suppresses info+file events,
            --json suppresses everything (the caller prints the JSON).
            """
            if "full" in dest:
                print(msg, file=full_log, flush=True)
            if "partial" in dest:
                print(msg, file=partial_log, flush=True)
            if json_out:
                return
            if quiet and level in ("info", "file"):
                return
            print(msg)

        now = datetime.datetime.now().isoformat()
        plog(f"backup run {now}")
        plog("sources:")
        for source in source_dirs:
            plog(source)
        if source_excludes:
            plog("excludes:")
            for exclude in source_excludes:
                plog(exclude)
        plog("destination:")
        plog(dest_full, "full")
        plog(dest_partial, "partial")

        plog("read state")
        state = read_full_state(config)

        if not fs_exists(dest_partial):
            plog(f"create {dest_partial}", "partial")
            fs_makedirs(dest_partial, exist_ok=True)

        # Pre-compute (sourcedir, prefixlen) so deletion can look up per-file.
        source_prefixes = [
            (sd, compute_prefixlen(os.path.dirname(sd))) for sd in source_dirs
        ]

        def relpath_for(filepath):
            for sd, plen in source_prefixes:
                if filepath == sd or filepath.startswith(sd + os.sep):
                    return filepath[plen:]
            return None

        new_state = {}
        errors_full = []
        errors_partial = []
        stats = {
            "new": 0, "changed": 0, "deleted": 0,
            "unchanged": 0, "failed": 0, "bytes_copied": 0,
        }

        def copy_into(filepath, destbase, relpath, overwrite, errors, tag):
            """Return True on success (or in dry-run), False on failure.
            Caller uses this to decide whether to record the file in state:
            a failed copy must NOT be marked done, so the next run retries.
            """
            destpath = os.path.normpath(os.path.join(destbase, relpath))
            if dry_run:
                return True
            try:
                fs_makedirs(os.path.dirname(destpath), exist_ok=True)
                if overwrite and fs_exists(destpath):
                    fs_remove(destpath)
                fs_copy2(filepath, destpath)
                return True
            except Exception as e:
                err = f"ERR: failed to copy {filepath} => {destpath}"
                errors.append(err)
                plog(err, tag, level="warn")
                plog(str(e), tag, level="warn")
                return False

        progress = Progress(state, enabled=not json_out)
        completed = False
        try:
            for sourcedir, prefixlen in source_prefixes:
                plog(f"processing {sourcedir}")
                for filepath in walk(sourcedir, is_excluded):
                    try:
                        fstat = fs_stat(filepath)
                    except Exception as e:
                        err = f"ERR: file {filepath} not found (fstat)"
                        errors_full.append(err)
                        errors_partial.append(err)
                        plog(err, level="warn")
                        plog(str(e), level="warn")
                        continue
                    progress.tick(filepath, fstat.st_size)
                    relpath = filepath[prefixlen:]
                    if filepath in state:
                        orig_size, orig_mtime = state[filepath]
                        if (
                            fstat.st_size != orig_size
                            or mtime_changed(fstat.st_mtime, orig_mtime)
                        ):
                            plog(f"** {filepath}", level="file")
                            ok_p = copy_into(filepath, dest_partial, relpath, True, errors_partial, "partial")
                            ok_f = copy_into(filepath, dest_full, relpath, True, errors_full, "full")
                            if ok_p and ok_f:
                                stats["changed"] += 1
                                stats["bytes_copied"] += fstat.st_size
                                new_state[filepath] = [
                                    fstat.st_size, int(fstat.st_mtime),
                                ]
                            else:
                                # Preserve the OLD (size, mtime) so on the next
                                # run the file's current stat still differs
                                # from state and we retry the copy. Without
                                # this carry-over, completion would write a
                                # state without this entry and the file would
                                # then be re-detected only as "new" (still
                                # works, but loses history).
                                new_state[filepath] = [orig_size, orig_mtime]
                                stats["failed"] += 1
                        else:
                            stats["unchanged"] += 1
                            new_state[filepath] = [
                                fstat.st_size, int(fstat.st_mtime),
                            ]
                    else:
                        plog(f"++ {filepath}", level="file")
                        ok_p = copy_into(filepath, dest_partial, relpath, False, errors_partial, "partial")
                        ok_f = copy_into(filepath, dest_full, relpath, False, errors_full, "full")
                        if ok_p and ok_f:
                            stats["new"] += 1
                            stats["bytes_copied"] += fstat.st_size
                            new_state[filepath] = [
                                fstat.st_size, int(fstat.st_mtime),
                            ]
                        else:
                            # New file that failed to copy: leave it out of
                            # state entirely so next run retries as "new".
                            stats["failed"] += 1
            completed = True
            progress.done()
        except BaseException as e:
            progress.done()
            # BaseException catches KeyboardInterrupt too — we want consistent
            # state even on Ctrl-C. Reraise after writing merged state.
            plog(f"interrupted by exception: {e}", level="warn")
            import logging
            logging.exception("backup walk failed")

        if completed:
            # Deletion pass: only safe when we successfully visited everything.
            for filepath in state:
                if filepath in new_state:
                    continue
                relpath = relpath_for(filepath)
                if relpath is None:
                    # State entry doesn't match any current source dir — skip,
                    # don't blindly delete using a wrong prefix.
                    plog(
                        f"WARN: orphan state entry, not deleting: {filepath}"
                    )
                    continue
                destpath = os.path.normpath(os.path.join(dest_full, relpath))
                if fs_exists(destpath):
                    plog(f"-- {filepath} (full)", "full", level="file")
                    if not dry_run:
                        remove_file(destpath, dest_full)
                    stats["deleted"] += 1
                destpath = os.path.normpath(
                    os.path.join(dest_partial, relpath)
                )
                if fs_exists(destpath):
                    plog(f"-- {filepath} (partial)", "partial", level="file")
                    if not dry_run:
                        remove_file(destpath, dest_partial)
            final_state = new_state
        else:
            # Merge: preserve old state entries for paths we never reached,
            # so a mid-run failure doesn't truncate state and trigger mass
            # re-copies (or worse, mass deletes) on the next run.
            final_state = {
                p: v for p, v in state.items() if p not in new_state
            }
            final_state.update(new_state)
            plog(
                "WARN: backup did not complete; state merged, "
                "deletion pass skipped"
            )

        plog("write state")
        if not dry_run:
            write_full_state(config, final_state)

        if dry_run:
            pass
        elif completed:
            plog("copying state to partial folder")
            full_state_src = config["full_state_file"]
            full_state_dest = os.path.join(dest_partial, "__state.json")
            fs_copy2(full_state_src, full_state_dest)
        else:
            # Mark snapshot as partial so restore won't trust it as a manifest.
            marker = os.path.join(dest_partial, "__incomplete")
            try:
                with fs_open(marker, "w", encoding="utf8") as f:
                    f.write(datetime.datetime.now().isoformat())
            except Exception:
                pass
        elapsed = (
            datetime.datetime.now()
            - datetime.datetime.fromisoformat(now)
        ).total_seconds()
        stats["elapsed_seconds"] = round(elapsed, 2)
        stats["completed"] = completed
        stats["dry_run"] = dry_run
        plog(
            f"summary: {stats['new']} new, {stats['changed']} changed, "
            f"{stats['deleted']} deleted, {stats['unchanged']} unchanged, "
            f"{stats['failed']} failed, "
            f"{format_size(stats['bytes_copied'])} copied "
            f"in {elapsed:.1f}s",
            level="summary",
        )
        plog("done")

        if errors_full:
            plog("Errors:", "full", level="warn")
            for err in errors_full:
                plog(err, "full", level="warn")

        if errors_partial:
            plog("Errors:", "partial", level="warn")
            for err in errors_partial:
                plog(err, "partial", level="warn")

        if json_out:
            print(json.dumps(stats, indent=2))

        return stats


def _path_matches(fullpath, patterns):
    """Match a fullpath against a list of patterns. Each pattern is:
      - a glob (contains *, ?, or [) -> fnmatch against the full path
      - otherwise a prefix match (legacy behavior)
    Empty patterns list matches everything.
    """
    import fnmatch
    if not patterns:
        return True
    for pat in patterns:
        if any(c in pat for c in "*?["):
            if fnmatch.fnmatchcase(fullpath, pat):
                return True
        else:
            if fullpath.startswith(pat):
                return True
    return False


def restore(config, destdir, timestamp, patterns=None,
            dry_run=False, force=False):
    print("restore" + (" (dry-run)" if dry_run else ""))
    if isinstance(patterns, str):
        patterns = [patterns] if patterns else []
    patterns = patterns or []
    if fs_exists(destdir) and not force and not dry_run:
        print(f"ERR: {destdir} exists, abort (use --force to merge into it)")
        sys.exit(1)
    partial_dir = config["destination"]["directory_partial"]
    history = [
        h
        for h in sorted(fs_listdir(partial_dir), reverse=True)
        if h <= timestamp
        and fs_isdir(os.path.join(partial_dir, h))
        and not fs_exists(
            os.path.join(partial_dir, h, "__incomplete")
        )
    ]
    if not history:
        print(f"ERR: no usable snapshot at or before {timestamp}")
        sys.exit(1)
    full_state = read_full_state_file(
        os.path.join(partial_dir, history[0], "__state.json")
    )
    restored = 0
    missing = 0
    for fullpath in full_state:
        if not _path_matches(fullpath, patterns):
            continue
        prefix = find_source_prefix(config, fullpath)
        if not prefix:
            print(f"ERR: {fullpath} could not be matched to source dirs")
            continue
        prefixlen = compute_prefixlen(prefix)
        relpath = fullpath[prefixlen:]
        for dirname in history:
            pathname = os.path.join(partial_dir, dirname, relpath)
            if fs_exists(pathname):
                destpath = os.path.join(destdir, relpath)
                if dry_run:
                    print(f"DRY {destpath}  <-  {dirname}/{relpath}")
                else:
                    fs_makedirs(os.path.dirname(destpath), exist_ok=True)
                    fs_copy2(pathname, destpath)
                    print(destpath)
                restored += 1
                break
        else:
            print(f"ERR: {relpath} not found in backup")
            missing += 1
    print(
        f"done: {restored} file(s) "
        f"{'would be ' if dry_run else ''}restored"
        + (f", {missing} missing" if missing else "")
    )


def package_data(
    config,
    destdir,
    max_size,
    timestamp,
    full=False,
    force=False,
):
    print("package-data")
    if fs_exists(destdir) and not force:
        print(f"ERR: {destdir} exists, abort")
        exit(1)
    if full:
        cutoff = "0000-00-00"
    else:
        pkg_state_file = config["packaging_state_file"]
        pkg_state_path = os.path.join(base_dir(), pkg_state_file)
        if fs_exists(pkg_state_path):
            with fs_open(pkg_state_path, encoding="utf8") as infile:
                data = json.load(infile)
                cutoff = data["timestamp"]
        else:
            cutoff = "0000-00-00"
    index = 1
    size = 0
    destbase = os.path.join(destdir, f"backup-{timestamp}-part-{index}")
    partial_dir = config["destination"]["directory_partial"]
    history = [
        h
        for h in sorted(fs_listdir(partial_dir), reverse=True)
        if h <= timestamp and h > cutoff
        and fs_isdir(os.path.join(partial_dir, h))
        and not fs_exists(
            os.path.join(partial_dir, h, "__incomplete")
        )
    ]
    full_state = read_full_state_file(
        os.path.join(partial_dir, history[0], "__state.json")
    )
    for fullpath in full_state:
        prefix = find_source_prefix(config, fullpath)
        if not prefix:
            print(f"ERR: {fullpath} could not be matched to source dirs")
            continue
        prefixlen = compute_prefixlen(prefix)
        relpath = fullpath[prefixlen:].replace("\\", "/")
        for dirname in history:
            pathname = os.path.join(partial_dir, dirname, relpath)
            if fs_exists(pathname):
                fstat = fs_stat(pathname)
                filesize = fstat.st_size
                if size > 0 and size + filesize > max_size:
                    index += 1
                    destbase = os.path.join(
                        destdir,
                        f"backup-{timestamp}-part-{index}",
                    )
                    size = 0
                size += filesize
                destpath = os.path.join(destbase, relpath)
                if not fs_exists(destpath):
                    fs_makedirs(os.path.dirname(destpath), exist_ok=True)
                    fs_copy2(pathname, destpath)
                    print(destpath)
                break
    if full:
        pkg_state_file = config["packaging_state_file"]
        pkg_state_path = os.path.join(base_dir(), pkg_state_file)
        with fs_open(pkg_state_path, "w", encoding="utf8") as outfile:
            json.dump({"timestamp": timestamp}, outfile)
    print("done")


CONFIG_TEMPLATE = {
    "source": {
        "directories": ["/path/to/source"],
        "excludes": [],
    },
    "destination": {
        "directory_full": "/path/to/backup/full",
        "directory_partial": "/path/to/backup/partial",
    },
    "full_state_file": "/path/to/state.json",
    "packaging_state_file": "packaging-state.json",
}


def cmd_init(name="backup-config.json", force=False):
    target = os.path.join(base_dir(), name)
    if fs_exists(target) and not force:
        print(f"ERR: {target} exists (use --force to overwrite)")
        sys.exit(1)
    with fs_open(target, "w", encoding="utf8") as f:
        json.dump(CONFIG_TEMPLATE, f, indent=2)
        f.write("\n")
    print(f"wrote {target}")
    print(
        "Edit it to point at your sources and backup destinations, "
        "then run: python dabbak.py backup"
    )


def enumerate_snapshots(partial_dir):
    """Return [{date, path, file_count, total_bytes, incomplete, log}].
    Sorted newest-first. Tolerates entries that aren't valid snapshot dirs
    (skips them silently — they may be the per-day log files).
    """
    out = []
    if not fs_isdir(partial_dir):
        return out
    for name in sorted(fs_listdir(partial_dir), reverse=True):
        full = os.path.join(partial_dir, name)
        if not fs_isdir(full):
            continue
        # Names are expected to be YYYY-MM-DD; tolerate others by skipping.
        try:
            datetime.date.fromisoformat(name)
        except ValueError:
            continue
        files = 0
        bytes_total = 0
        for _ in []:
            pass
        for path in walk(full, []):
            base = os.path.basename(path)
            if base in ("__state.json", "__incomplete"):
                continue
            try:
                files += 1
                bytes_total += fs_stat(path).st_size
            except Exception:
                pass
        out.append({
            "date": name,
            "path": full,
            "file_count": files,
            "total_bytes": bytes_total,
            "incomplete": fs_exists(os.path.join(full, "__incomplete")),
            "log": os.path.join(partial_dir, f"backup-partial-{name}.log"),
        })
    return out


def cmd_list(config, json_out=False):
    snaps = enumerate_snapshots(
        os.path.normpath(config["destination"]["directory_partial"])
    )
    if json_out:
        print(json.dumps(
            [{k: v for k, v in s.items() if k != "path"} for s in snaps],
            indent=2,
        ))
        return
    if not snaps:
        print("no snapshots")
        return
    print(f"{'date':<12} {'files':>10} {'size':>10}  status")
    for s in snaps:
        status = "incomplete" if s["incomplete"] else "ok"
        print(
            f"{s['date']:<12} {s['file_count']:>10,} "
            f"{format_size(s['total_bytes']):>10}  {status}"
        )


def select_snapshots_to_prune(snaps, keep_last=None, keep_days=None,
                              today=None):
    """Return the subset of `snaps` (newest-first) that should be deleted.

    A snapshot is KEPT if it satisfies EITHER policy:
      - within the most recent `keep_last` entries
      - dated within the last `keep_days` days (relative to `today`)
    Today's snapshot is always kept regardless of policy, so an in-progress
    run can't be deleted out from under itself.
    """
    if keep_last is None and keep_days is None:
        raise ValueError("must specify keep_last and/or keep_days")
    if today is None:
        today = datetime.date.today()
    cutoff = None
    if keep_days is not None:
        cutoff = today - datetime.timedelta(days=keep_days)
    to_delete = []
    for i, s in enumerate(snaps):
        if s["date"] == today.isoformat():
            continue
        keep = False
        if keep_last is not None and i < keep_last:
            keep = True
        if cutoff is not None:
            try:
                if datetime.date.fromisoformat(s["date"]) >= cutoff:
                    keep = True
            except ValueError:
                keep = True  # unparseable date: be safe, keep
        if not keep:
            to_delete.append(s)
    return to_delete


def cmd_prune(config, keep_last=None, keep_days=None, force=False,
              json_out=False):
    partial = os.path.normpath(config["destination"]["directory_partial"])
    snaps = enumerate_snapshots(partial)
    to_delete = select_snapshots_to_prune(
        snaps, keep_last=keep_last, keep_days=keep_days,
    )
    result = {
        "kept": [s["date"] for s in snaps
                 if s not in to_delete],
        "deleted": [],
        "would_delete": [s["date"] for s in to_delete],
        "force": force,
    }
    if not force:
        if not json_out:
            print(
                "DRY RUN (no --force). Would delete "
                f"{len(to_delete)} snapshot(s):"
            )
            for s in to_delete:
                print(
                    f"  {s['date']}  "
                    f"{s['file_count']:,} files  "
                    f"{format_size(s['total_bytes'])}"
                )
            print(f"Would keep {len(snaps) - len(to_delete)} snapshot(s).")
        if json_out:
            print(json.dumps(result, indent=2))
        return result
    for s in to_delete:
        try:
            shutil.rmtree(_long(s["path"]))
        except Exception as e:
            if not json_out:
                print(f"ERR: failed to remove {s['path']}: {e}")
            continue
        if fs_exists(s["log"]):
            try:
                fs_remove(s["log"])
            except Exception:
                pass
        result["deleted"].append(s["date"])
        if not json_out:
            print(f"deleted {s['date']}")
    if json_out:
        print(json.dumps(result, indent=2))
    return result


def refresh_state(config):
    print("refresh-state")
    source_dirs = expand_source_dirs(config["source"]["directories"])
    dest_full = os.path.normpath(
        config["destination"]["directory_full"]
    )
    dest_partial_base = os.path.normpath(
        config["destination"]["directory_partial"]
    )
    timestamp = datetime.date.today().strftime("%Y-%m-%d")
    dest_partial = os.path.join(dest_partial_base, timestamp)

    new_state = {}
    for sourcedir in source_dirs:
        prefix = os.path.dirname(sourcedir)
        prefixlen = compute_prefixlen(prefix)
        destdir = os.path.join(dest_full, sourcedir[prefixlen:])
        if not fs_isdir(destdir):
            continue
        destdir_prefixlen = compute_prefixlen(destdir)
        for filepath in walk(destdir, []):
            fstat = fs_stat(filepath)
            srcpath = os.path.join(sourcedir, filepath[destdir_prefixlen:])
            new_state[srcpath] = [
                fstat.st_size,
                int(fstat.st_mtime),
            ]

    write_full_state(config, new_state)
    if fs_isdir(dest_partial):
        full_state_src = config["full_state_file"]
        full_state_dest = os.path.join(dest_partial, "__state.json")
        fs_copy2(full_state_src, full_state_dest)
    print("done")


def parse_size(s):
    s = s.lower()
    mult = 1
    if s.endswith("g"):
        mult = 1024 * 1024 * 1024
        s = s[:-1]
    elif s.endswith("m"):
        mult = 1024 * 1024
        s = s[:-1]
    elif s.endswith("k"):
        mult = 1024
        s = s[:-1]
    return int(s) * mult


def today_str():
    return datetime.date.today().strftime("%Y-%m-%d")


def build_parser():
    p = argparse.ArgumentParser(
        prog="dabbak",
        description="Incremental backup tool with dated partial snapshots.",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    pb = sub.add_parser("backup", help="run incremental backup")
    pb.add_argument(
        "--dry-run",
        action="store_true",
        help="walk and diff but make no filesystem changes",
    )
    pb.add_argument(
        "--quiet", "-q", action="store_true",
        help="suppress per-file output (still shows warnings + summary)",
    )
    pb.add_argument(
        "--json", action="store_true", dest="json_out",
        help="emit a JSON summary on stdout (suppresses normal output)",
    )

    pr = sub.add_parser("restore", help="restore files from a snapshot")
    pr.add_argument("dest_dir")
    pr.add_argument("--timestamp", "-t", default=None,
                    help="YYYY-MM-DD (default: today)")
    pr.add_argument(
        "patterns", nargs="*",
        help="one or more path filters: a prefix, or a glob "
             "(*, ?, [..]) matched against the full source path. "
             "Omit to restore everything in the snapshot.",
    )
    pr.add_argument("--dry-run", action="store_true",
                    help="show what would be restored, do not copy")
    pr.add_argument("--force", action="store_true",
                    help="allow merging into an existing dest dir")

    pp = sub.add_parser("package", help="build size-chunked archives")
    pp.add_argument("dest_dir")
    pp.add_argument("max_size", help="bytes; suffix k/m/g supported")
    pp.add_argument("timestamp", nargs="?", default=None)
    pp.add_argument("--full", action="store_true",
                    help="ignore packaging cutoff and update it after")
    pp.add_argument("--force", action="store_true",
                    help="proceed even if dest_dir exists")

    sub.add_parser("refresh-state",
                   help="rebuild state from dest_full mirror")
    sub.add_parser("config", help="print effective config")

    pi = sub.add_parser(
        "init",
        help="create a config template next to dabbak.py",
    )
    pi.add_argument("--name", default="backup-config.json",
                    help="filename to write (default: backup-config.json)")
    pi.add_argument("--force", action="store_true",
                    help="overwrite if the file exists")

    pl = sub.add_parser("list", help="list partial snapshots")
    pl.add_argument("--json", action="store_true", dest="json_out")

    pp2 = sub.add_parser(
        "prune",
        help="delete old partial snapshots by retention policy",
    )
    pp2.add_argument("--keep-last", type=int, default=None,
                     help="keep the N most recent snapshots")
    pp2.add_argument("--keep-days", type=int, default=None,
                     help="keep snapshots within the last N days")
    pp2.add_argument("--force", action="store_true",
                     help="actually delete (default: dry-run)")
    pp2.add_argument("--json", action="store_true", dest="json_out")

    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    # init must not require an existing config.
    if args.cmd == "init":
        cmd_init(name=args.name, force=args.force)
        return
    config = read_config()
    if args.cmd == "backup":
        make_backup(
            config,
            dry_run=args.dry_run,
            quiet=args.quiet,
            json_out=args.json_out,
        )
    elif args.cmd == "restore":
        # Back-compat: the old CLI was `restore <dest> [<YYYY-MM-DD>
        # [<source-path>]]`. If `-t` wasn't given and the first positional
        # looks like a date, accept it as the timestamp so existing scripts
        # keep working.
        import re as _re
        pos = list(args.patterns)
        ts = args.timestamp
        if ts is None and pos and _re.fullmatch(r"\d{4}-\d{2}-\d{2}", pos[0]):
            ts = pos.pop(0)
        restore(
            config,
            args.dest_dir,
            ts or today_str(),
            patterns=pos,
            dry_run=args.dry_run,
            force=args.force,
        )
    elif args.cmd == "package":
        package_data(
            config,
            args.dest_dir,
            parse_size(args.max_size),
            args.timestamp or today_str(),
            full=args.full,
            force=args.force,
        )
    elif args.cmd == "refresh-state":
        refresh_state(config)
    elif args.cmd == "config":
        print(json.dumps(config, indent=2, ensure_ascii=False))
    elif args.cmd == "list":
        cmd_list(config, json_out=args.json_out)
    elif args.cmd == "prune":
        if args.keep_last is None and args.keep_days is None:
            sys.stderr.write(
                "prune: must specify --keep-last and/or --keep-days\n"
            )
            sys.exit(2)
        cmd_prune(
            config,
            keep_last=args.keep_last,
            keep_days=args.keep_days,
            force=args.force,
            json_out=args.json_out,
        )


if __name__ == "__main__":
    main()
