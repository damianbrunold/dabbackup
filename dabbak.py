import datetime
import json
import os
import shutil
import sys


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
    with open(filepath, encoding="utf8") as infile:
        return json.load(infile)


def read_full_state(config):
    return read_full_state_file(config["full_state_file"])


def read_full_state_file(filepath):
    if os.path.exists(filepath):
        with open(filepath, encoding="utf8") as infile:
            return json.load(infile)
    else:
        return {}


def write_full_state(config, state):
    write_full_state_file(config["full_state_file"], state)


def write_full_state_file(filepath, state):
    tmp = filepath + ".tmp"
    with open(tmp, "w", encoding="utf8") as outfile:
        json.dump(state, outfile, indent=2, ensure_ascii=False)
        outfile.flush()
        os.fsync(outfile.fileno())
    os.replace(tmp, filepath)


def remove_file(filepath, dest_full):
    try:
        os.remove(filepath)
        dirpath = os.path.dirname(filepath)
        while dirpath.startswith(dest_full) and not os.listdir(dirpath):
            os.rmdir(dirpath)
            dirpath = os.path.dirname(dirpath)
    except Exception:
        print(f"failed to delete {filepath}")


def walk(directory, excludes):
    if os.path.isfile(directory) and directory not in excludes:
        yield directory
    else:
        if directory in excludes:
            return
        for path in sorted(os.listdir(directory)):
            fullpath = os.path.join(directory, path)
            if fullpath in excludes:
                continue
            if os.path.isjunction(fullpath):
                continue
            if os.path.islink(fullpath):
                continue
            if os.path.isdir(fullpath):
                yield from walk(fullpath, excludes)
            elif os.path.isfile(fullpath):
                yield fullpath


def find_source_prefix(config, fullpath):
    is_windows_state = config["source"].get("is-windows") and os.sep == "/"
    for source_dir in config["source"]["directories"]:
        match_dir = source_dir
        if match_dir.endswith("*"):
            match_dir = match_dir[:-1]
        if is_windows_state:
            if not fullpath.startswith(match_dir):
                continue
            parts = source_dir.split("\\")
            if source_dir.endswith("*"):
                return "\\".join(parts[:-1]).rstrip("\\")
            return "\\".join(parts[:-1])
        if not fullpath.startswith(match_dir):
            continue
        if source_dir.endswith("*"):
            return os.path.dirname(match_dir.rstrip(os.sep))
        return os.path.dirname(source_dir)
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
            if not os.path.isdir(base):
                continue
            for name in sorted(os.listdir(base)):
                child = os.path.join(base, name)
                if os.path.isdir(child) and not os.path.islink(child):
                    result.append(child)
        else:
            result.append(srcdir)
    return result


def make_backup(config):
    today = datetime.date.today().strftime("%Y-%m-%d")

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

    if not os.path.exists(dest_partial_base):
        os.makedirs(dest_partial_base, exist_ok=True)

    with open(get_full_log(), "a", encoding="utf8") as full_log, \
            open(get_partial_log(dest_partial_base, today), "a", encoding="utf8") as partial_log:

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

        if not os.path.exists(dest_partial):
            plog(f"create {dest_partial}", "partial")
            os.makedirs(dest_partial, exist_ok=True)

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
        completed = False
        try:
            for sourcedir, prefixlen in source_prefixes:
                plog(f"processing {sourcedir}")
                for filepath in walk(sourcedir, source_excludes):
                    try:
                        fstat = os.stat(filepath)
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
                            or int(fstat.st_mtime) != int(orig_mtime)
                        ):
                            plog(f"** {filepath}")
                            destpath = os.path.normpath(
                                os.path.join(dest_partial, relpath)
                            )
                            try:
                                os.makedirs(
                                    os.path.dirname(destpath),
                                    exist_ok=True,
                                )
                                if os.path.exists(destpath):
                                    os.remove(destpath)
                                shutil.copy2(filepath, destpath)
                            except Exception as e:
                                err = f"ERR: failed to copy {filepath} => {destpath}"
                                errors_partial.append(err)
                                plog(err, "partial")
                                plog(str(e), "partial")
                            destpath = os.path.normpath(
                                os.path.join(dest_full, relpath)
                            )
                            try:
                                os.makedirs(
                                    os.path.dirname(destpath),
                                    exist_ok=True,
                                )
                                if os.path.exists(destpath):
                                    os.remove(destpath)
                                shutil.copy2(filepath, destpath)
                            except Exception as e:
                                err = f"ERR: failed to copy {filepath} => {destpath}"
                                errors_full.append(err)
                                plog(err, "full")
                                plog(str(e), "full")
                    else:
                        plog(f"++ {filepath}")
                        destpath = os.path.normpath(
                            os.path.join(dest_partial, relpath)
                        )
                        try:
                            os.makedirs(os.path.dirname(destpath), exist_ok=True)
                            shutil.copy2(filepath, destpath)
                        except Exception as e:
                            err = f"ERR: failed to copy {filepath} => {destpath}"
                            errors_partial.append(err)
                            plog(err, "partial")
                            plog(str(e), "partial")
                        destpath = os.path.normpath(
                            os.path.join(dest_full, relpath)
                        )
                        try:
                            os.makedirs(os.path.dirname(destpath), exist_ok=True)
                            shutil.copy2(filepath, destpath)
                        except Exception as e:
                            err = f"ERR: failed to copy {filepath} => {destpath}"
                            errors_full.append(err)
                            plog(err, "full")
                            plog(str(e), "full")
                    new_state[filepath] = [
                        fstat.st_size,
                        int(fstat.st_mtime),
                    ]
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
                if os.path.exists(destpath):
                    plog(f"-- {filepath} (full)", "full")
                    remove_file(destpath, dest_full)
                destpath = os.path.normpath(
                    os.path.join(dest_partial, relpath)
                )
                if os.path.exists(destpath):
                    plog(f"-- {filepath} (partial)", "partial")
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
        write_full_state(config, final_state)

        if completed:
            plog("copying state to partial folder")
            full_state_src = config["full_state_file"]
            full_state_dest = os.path.join(dest_partial, "__state.json")
            shutil.copy2(full_state_src, full_state_dest)
        else:
            # Mark snapshot as partial so restore won't trust it as a manifest.
            marker = os.path.join(dest_partial, "__incomplete")
            try:
                with open(marker, "w", encoding="utf8") as f:
                    f.write(datetime.datetime.now().isoformat())
            except Exception:
                pass
        plog("done")

        if errors_full:
            plog("Errors:", "full")
            for err in errors_full:
                plog(err, "full")

        if errors_partial:
            plog("Errors:", "partial")
            for err in errors_partial:
                plog(err, "partial")


def restore(config, destdir, timestamp, source_path):
    print("restore")
    if os.path.exists(destdir):
        print(f"ERR: {destdir} exists, abort")
        exit(1)
    partial_dir = config["destination"]["directory_partial"]
    history = [
        h
        for h in sorted(os.listdir(partial_dir), reverse=True)
        if h <= timestamp
        and os.path.isdir(os.path.join(partial_dir, h))
        and not os.path.exists(
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
            if os.path.exists(pathname):
                destpath = os.path.join(destdir, relpath)
                os.makedirs(os.path.dirname(destpath), exist_ok=True)
                shutil.copy2(pathname, destpath)
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
    if os.path.exists(destdir) and not force:
        print(f"ERR: {destdir} exists, abort")
        exit(1)
    if full:
        cutoff = "0000-00-00"
    else:
        pkg_state_file = config["packaging_state_file"]
        pkg_state_path = os.path.join(base_dir(), pkg_state_file)
        if os.path.exists(pkg_state_path):
            with open(pkg_state_path, encoding="utf8") as infile:
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
        for h in sorted(os.listdir(partial_dir), reverse=True)
        if h <= timestamp and h > cutoff
        and os.path.isdir(os.path.join(partial_dir, h))
        and not os.path.exists(
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
            if os.path.exists(pathname):
                fstat = os.stat(pathname)
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
                if not os.path.exists(destpath):
                    os.makedirs(os.path.dirname(destpath), exist_ok=True)
                    shutil.copy2(pathname, destpath)
                    print(destpath)
                break
    if full:
        pkg_state_file = config["packaging_state_file"]
        pkg_state_path = os.path.join(base_dir(), pkg_state_file)
        with open(pkg_state_path, "w", encoding="utf8") as outfile:
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
        if not os.path.isdir(destdir):
            continue
        destdir_prefixlen = compute_prefixlen(destdir)
        for filepath in walk(destdir, []):
            fstat = os.stat(filepath)
            srcpath = os.path.join(sourcedir, filepath[destdir_prefixlen:])
            new_state[srcpath] = [
                fstat.st_size,
                int(fstat.st_mtime),
            ]

    write_full_state(config, new_state)
    if os.path.isdir(dest_partial):
        full_state_src = config["full_state_file"]
        full_state_dest = os.path.join(dest_partial, "__state.json")
        shutil.copy2(full_state_src, full_state_dest)
    print("done")


def help():
    print("dabbak backup")
    print("dabbak restore <dest-dir> [<yyyy-mm-dd> [<source-path>]]")
    print("dabbak package <dest-dir> <max-size> [<yyyy-mm-dd>] [--full]")
    print("dabbak refresh-state")
    print("dabbak config")


if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) == 0:
        print(base_dir())
        help()
        exit(1)
    config = read_config()
    cmd = args[0]
    if cmd == "backup":
        make_backup(config)
    elif cmd == "restore":
        dest_dir = args[1]
        source_path = ""
        if len(args) > 2 and not args[2].startswith("--"):
            timestamp = args[2]
            if len(args) > 3:
                source_path = args[3]
        else:
            timestamp = datetime.date.today().strftime("%Y-%m-%d")
        restore(config, dest_dir, timestamp, source_path)
    elif cmd == "package":
        dest_dir = args[1]
        max_size = args[2].lower()
        if len(args) > 3 and not args[3].startswith("--"):
            timestamp = args[3]
        else:
            timestamp = datetime.date.today().strftime("%Y-%m-%d")
        if max_size.endswith("g"):
            max_size = int(max_size[:-1]) * 1024*1024*1024
        elif max_size.endswith("m"):
            max_size = int(max_size[:-1]) * 1024*1024
        elif max_size.endswith("k"):
            max_size = int(max_size[:-1]) * 1024
        else:
            max_size = int(max_size)
        package_data(
            config,
            dest_dir,
            max_size,
            timestamp,
            full="--full" in args,
            force="--force" in args,
        )
    elif cmd == "refresh-state":
        refresh_state(config)
    elif cmd == "config":
        print(json.dumps(config, indent=2, ensure_ascii=False))
    else:
        help()
        exit(1)
