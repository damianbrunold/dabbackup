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
    filepath = os.path.join(base_dir(), "backup-config.json")
    with open(filepath, encoding="utf8") as infile:
        return json.load(infile)


def read_full_state(config):
    read_full_state_file(config["full_state_file"])


def read_full_state_file(filepath):
    if os.path.exists(filepath):
        with open(filepath, encoding="utf8") as infile:
            return json.load(infile)
    else:
        return {}


def write_full_state(config, state):
    filepath = config["full_state_file"]
    with open(filepath, "w", encoding="utf8") as outfile:
        json.dump(state, outfile, indent=2, ensure_ascii=False)


def read_partial_state(partial_dir):
    filepath = os.path.join(partial_dir, "__state.json")
    if os.path.exists(filepath):
        with open(filepath, encoding="utf8") as infile:
            return json.load(infile)
    else:
        return {}


def write_partial_state(partial_dir, state):
    filepath = os.path.join(partial_dir, "__state.json")
    with open(filepath, "w", encoding="utf8") as outfile:
        json.dump(state, outfile, indent=2, ensure_ascii=False)


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
    for path in sorted(os.listdir(directory)):
        fullpath = os.path.join(directory, path)
        if fullpath in excludes:
            continue
        if os.path.isdir(fullpath):
            yield from walk(fullpath, excludes)
        elif os.path.isfile(fullpath):
            yield fullpath


def find_source_prefix(config, fullpath):
    for source_dir in config["source"]["directories"]:
        source_dir = os.path.normpath(source_dir)
        if fullpath.startswith(source_dir):
            return os.path.dirname(source_dir)


def make_backup(config):
    today = datetime.date.today().strftime("%Y-%m-%d")

    source_dirs = [
        os.path.normpath(path)
        for path in config["source"]["directories"]
    ]
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

    full_log = open(
        get_full_log(),
        "w+",
        encoding="utf8",
    )
    partial_log = open(
        get_partial_log(dest_partial_base, today),
        "w+",
        encoding="utf8",
    )

    def plog(msg, dest="full,partial"):
        if "full" in dest:
            print(msg, file=full_log)
        if "partial" in dest:
            print(msg, file=partial_log)
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
    partial_state = read_partial_state(dest_partial)

    if not os.path.exists(dest_partial):
        plog(f"create {dest_partial}", "partial")
        os.mkdir(dest_partial)

    new_state = {}
    new_partial_state = {}
    for sourcedir in source_dirs:
        plog(f"processing {sourcedir}")
        sourcedir = os.path.normpath(sourcedir)
        prefix = os.path.dirname(sourcedir)
        prefixlen = len(prefix) + 1
        for filepath in walk(sourcedir, source_excludes):
            try:
                fstat = os.stat(filepath)
            except Exception as e:
                plog(f"ERR: file {filepath} not found (fstat)")
                plog(str(e))
                continue
            if filepath in state:
                # existing file
                orig_size, orig_mtime = state[filepath]
                if (
                    fstat.st_size != orig_size
                    or int(fstat.st_mtime) != int(orig_mtime)
                ):
                    # changed
                    plog(f"** {filepath}")

                    # update in partial
                    destpath = os.path.normpath(
                        os.path.join(dest_partial, filepath[prefixlen:])
                    )
                    try:
                        os.makedirs(os.path.dirname(destpath), exist_ok=True)
                        shutil.copy2(filepath, destpath)
                        new_partial_state[filepath] = [
                            fstat.st_size,
                            int(fstat.st_mtime),
                        ]
                    except Exception as e:
                        plog(
                            f"ERR: failed to copy {filepath} => {destpath}",
                            "partial",
                        )
                        plog(str(e), "partial")

                    # update in full
                    destpath = os.path.normpath(
                        os.path.join(dest_full, filepath[prefixlen:])
                    )
                    if os.path.exists(destpath):
                        os.remove(destpath)
                    try:
                        shutil.copy2(filepath, destpath)
                    except Exception as e:
                        plog(
                            f"ERR: failed to copy {filepath} => {destpath}",
                            "full",
                        )
                        plog(str(e), "full")
                elif filepath in partial_state:
                    # was in previous partial state
                    plog(f"** {filepath}")

                    # include in partial if necessary
                    destpath = os.path.normpath(
                        os.path.join(dest_partial, filepath[prefixlen:])
                    )
                    if not os.path.exists(destpath):
                        os.makedirs(os.path.dirname(destpath), exist_ok=True)
                        try:
                            shutil.copy2(filepath, destpath)
                        except Exception as e:
                            plog(
                                "ERR: failed to copy "
                                f"{filepath} => {destpath}",
                                "partial",
                            )
                            plog(str(e), "partial")
                    new_partial_state[filepath] = [
                        orig_size,
                        int(orig_mtime),
                    ]
            else:
                # new file
                plog(f"++ {filepath}")
                
                # include in partial
                destpath = os.path.normpath(
                    os.path.join(dest_partial, filepath[prefixlen:])
                )
                os.makedirs(os.path.dirname(destpath), exist_ok=True)
                try:
                    shutil.copy2(filepath, destpath)
                except Exception as e:
                    plog(
                        f"ERR: failed to copy {filepath} => {destpath}",
                        "partial",
                    )
                    plog(str(e), "partial")
                new_partial_state[filepath] = [
                    fstat.st_size,
                    int(fstat.st_mtime),
                ]

                # include in full
                destpath = os.path.normpath(
                    os.path.join(dest_full, filepath[prefixlen:])
                )
                os.makedirs(os.path.dirname(destpath), exist_ok=True)
                try:
                    shutil.copy2(filepath, destpath)
                except Exception as e:
                    plog(
                        f"ERR: failed to copy {filepath} => {destpath}",
                        "full",
                    )
                    plog(str(e), "full")
            new_state[filepath] = [
                fstat.st_size,
                int(fstat.st_mtime),
            ]
    for filepath in state:
        if filepath not in new_state:
            # file does not exist anymore
            # remove from full
            plog(f"-- {filepath}", "full")
            destpath = os.path.normpath(
                os.path.join(dest_full, filepath[prefixlen:])
            )
            remove_file(destpath, dest_full)
            # remove from partial
            destpath = os.path.normpath(
                os.path.join(dest_partial, filepath[prefixlen:])
            )
            if os.path.exists(destpath):
                plog(f"-- {filepath}", "partial")
                remove_file(destpath, dest_partial)
    plog("write state")
    write_full_state(config, new_state)
    write_partial_state(dest_partial, new_partial_state)

    # copy full state to partial folder (required for restore)
    plog("copying full state to partial folder")
    full_state_src = config["full_state_file"]
    full_state_dest = os.path.join(dest_partial, "__state_full.json")
    shutil.copy2(full_state_src, full_state_dest)
    plog("done")

    full_log.close()
    partial_log.close()


def restore(config, destdir, timestamp):
    print("restore")
    if os.path.exists(destdir):
        print(f"ERR: {destdir} exists, abort")
        exit(1)
    partial_dir = config["destination"]["directory_partial"]
    history = [
        h 
        for h in sorted(os.listdir(partial_dir), reverse=True)
        if h <= timestamp
    ]
    full_state = read_full_state_file(
        os.path.join(partial_dir, history[0], "__state_full.json")
    )
    for fullpath in full_state:
        prefix = find_source_prefix(config, fullpath)
        if not prefix:
            print(f"ERR: {fullpath} could not be matched to source dirs")
            continue
        relpath = fullpath[len(prefix)+1:]
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


def package_data(config, max_size, timestamp):
    print("package-data")
    # TODO
    pass


def refresh_state(config):
    print("refresh-state")
    source_dirs = [
        os.path.normpath(path)
        for path in config["source"]["directories"]
    ]
    dest_full = os.path.normpath(
        config["destination"]["directory_full"]
    )

    new_state = {}
    for sourcedir in source_dirs:
        sourcedir = os.path.normpath(sourcedir)
        prefix = os.path.dirname(sourcedir)
        prefixlen = len(prefix) + 1
        destdir = os.path.join(dest_full, sourcedir[prefixlen:])
        prefixlen2 = len(destdir) + 1
        for filepath in walk(destdir, []):
            fstat = os.stat(filepath)
            srcpath = os.path.join(sourcedir, filepath[prefixlen2:])
            new_state[srcpath] = [
                fstat.st_size,
                int(fstat.st_mtime),
            ]
    write_full_state(config, new_state)
    print("done")


def help():
    print("dabbak backup")
    print("dabbak restore <dest-dir> [<yyyy-mm-dd>]")
    print("dabbak package <dest-dir> <max-size> [<yyyy-mm-dd>]")
    print("dabbak refresh-state")


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
        if len(args) > 2:
            timestamp = args[2]
        else:
            timestamp = datetime.date.today().strftime("%Y-%m-%d")
        restore(config, dest_dir, timestamp)
    elif cmd == "package":
        dest_dir = args[1]
        max_size = args[2].lower()
        if len(args) > 3:
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
        package_data(config, dest_dir, max_size, timestamp)
    elif cmd == "refresh-state":
        refresh_state(config)
    else:
        help()
        exit(1)
