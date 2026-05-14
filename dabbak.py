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


def walk(directory, excludes):
    if fs_isfile(directory) and directory not in excludes:
        yield directory
    else:
        if directory in excludes:
            return
        for path in sorted(fs_listdir(directory)):
            fullpath = os.path.join(directory, path)
            if fullpath in excludes:
                continue
            if fs_isjunction(fullpath):
                continue
            if fs_islink(fullpath):
                continue
            if fs_isdir(fullpath):
                yield from walk(fullpath, excludes)
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


def make_backup(config, dry_run=False):
    today = datetime.date.today().strftime("%Y-%m-%d")
    if dry_run:
        print("DRY RUN: no files will be copied, deleted, or state written")

    source_dirs = expand_source_dirs(config["source"]["directories"])
    source_excludes = [
        os.path.normpath(path)
        for path in config["source"]["excludes"]
    ]
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

        def plog(msg, dest="full,partial"):
            if "full" in dest:
                print(msg, file=full_log, flush=True)
            if "partial" in dest:
                print(msg, file=partial_log, flush=True)
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
                plog(err, tag)
                plog(str(e), tag)
                return False

        completed = False
        try:
            for sourcedir, prefixlen in source_prefixes:
                plog(f"processing {sourcedir}")
                for filepath in walk(sourcedir, source_excludes):
                    try:
                        fstat = fs_stat(filepath)
                    except Exception as e:
                        err = f"ERR: file {filepath} not found (fstat)"
                        errors_full.append(err)
                        errors_partial.append(err)
                        plog(err)
                        plog(str(e))
                        continue
                    relpath = filepath[prefixlen:]
                    if filepath in state:
                        orig_size, orig_mtime = state[filepath]
                        if (
                            fstat.st_size != orig_size
                            or mtime_changed(fstat.st_mtime, orig_mtime)
                        ):
                            plog(f"** {filepath}")
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
                        plog(f"++ {filepath}")
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
        except BaseException as e:
            # BaseException catches KeyboardInterrupt too — we want consistent
            # state even on Ctrl-C. Reraise after writing merged state.
            plog(f"interrupted by exception: {e}")
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
                    plog(f"-- {filepath} (full)", "full")
                    if not dry_run:
                        remove_file(destpath, dest_full)
                    stats["deleted"] += 1
                destpath = os.path.normpath(
                    os.path.join(dest_partial, relpath)
                )
                if fs_exists(destpath):
                    plog(f"-- {filepath} (partial)", "partial")
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
        plog(
            f"summary: {stats['new']} new, {stats['changed']} changed, "
            f"{stats['deleted']} deleted, {stats['unchanged']} unchanged, "
            f"{stats['failed']} failed, "
            f"{format_size(stats['bytes_copied'])} copied "
            f"in {elapsed:.1f}s"
        )
        plog("done")

        if errors_full:
            plog("Errors:", "full")
            for err in errors_full:
                plog(err, "full")

        if errors_partial:
            plog("Errors:", "partial")
            for err in errors_partial:
                plog(err, "partial")

        return stats


def restore(config, destdir, timestamp, source_path):
    print("restore")
    if fs_exists(destdir):
        print(f"ERR: {destdir} exists, abort")
        exit(1)
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
    full_state = read_full_state_file(
        os.path.join(partial_dir, history[0], "__state.json")
    )
    for fullpath in full_state:
        if not fullpath.startswith(source_path):
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
                fs_makedirs(os.path.dirname(destpath), exist_ok=True)
                fs_copy2(pathname, destpath)
                print(destpath)
                break
        else:
            print(f"ERR: {relpath} not found in backup")
    print("done")


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

    pr = sub.add_parser("restore", help="restore files from a snapshot")
    pr.add_argument("dest_dir")
    pr.add_argument("timestamp", nargs="?", default=None,
                    help="YYYY-MM-DD (default: today)")
    pr.add_argument("source_path", nargs="?", default="",
                    help="filter restored files by source-path prefix")

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
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    config = read_config()
    if args.cmd == "backup":
        make_backup(config, dry_run=args.dry_run)
    elif args.cmd == "restore":
        restore(
            config,
            args.dest_dir,
            args.timestamp or today_str(),
            args.source_path,
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


if __name__ == "__main__":
    main()
