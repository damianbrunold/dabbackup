import datetime
import json
import os
import shutil
import sys


def base_dir():
    return os.path.normpath(os.path.abspath(os.path.dirname(sys.argv[0])))


def get_full_log():
    return os.path.join(base_dir(), "backup-full.log")


def get_partial_log(dest_partial_base, today):
    return os.path.join(dest_partial_base, f"backup-partial-{today}.log")


def read_config():
    filepath = os.path.join(base_dir(), "backup-config.json")
    with open(filepath, encoding="utf8") as infile:
        return json.load(infile)


def read_full_state(config):
    filepath = config["full_state_file"]
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
        else:
            yield fullpath


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

    remove_partial = None
    if os.path.exists(dest_partial):
        plog(f"move existing {dest_partial} to temp", "partial")
        remove_partial = dest_partial + "-temp"
        try:
            os.rename(dest_partial, remove_partial)
        except Exception:
            print(f"failed to rename {dest_partial}")
            exit(1)
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
            fstat = os.stat(filepath)
            if filepath in state:
                # existing file
                orig_size, orig_mtime = state[filepath]
                if (
                    fstat.st_size != orig_size
                    or int(fstat.st_mtime) != int(orig_mtime)
                ):
                    # changed
                    plog(f"** {filepath}")

                    # include in partial
                    destpath = os.path.normpath(
                        os.path.join(dest_partial, filepath[prefixlen:])
                    )
                    os.makedirs(os.path.dirname(destpath), exist_ok=True)
                    shutil.copy2(filepath, destpath)
                    new_partial_state[filepath] = [
                        fstat.st_size,
                        int(fstat.st_mtime),
                    ]

                    # update in full
                    destpath = os.path.normpath(
                        os.path.join(dest_full, filepath[prefixlen:])
                    )
                    if os.path.exists(destpath):
                        os.remove(destpath)
                    shutil.copy2(filepath, destpath)
                elif filepath in partial_state:
                    # was in previous partial state
                    plog(f"** {filepath}")

                    # include in partial
                    destpath = os.path.normpath(
                        os.path.join(dest_partial, filepath[prefixlen:])
                    )
                    os.makedirs(os.path.dirname(destpath), exist_ok=True)
                    shutil.copy2(filepath, destpath)
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
                shutil.copy2(filepath, destpath)
                new_partial_state[filepath] = [
                    fstat.st_size,
                    int(fstat.st_mtime),
                ]

                # include in full
                destpath = os.path.normpath(
                    os.path.join(dest_full, filepath[prefixlen:])
                )
                os.makedirs(os.path.dirname(destpath), exist_ok=True)
                shutil.copy2(filepath, destpath)
            new_state[filepath] = [
                fstat.st_size,
                int(fstat.st_mtime),
            ]
    for filepath in state:
        if filepath in new_state:
            continue
        # file does not exist anymore, remove from full
        plog(f"-- {filepath}", "full")
        destpath = os.path.normpath(
            os.path.join(dest_full, filepath[prefixlen:])
        )
        remove_file(destpath, dest_full)
    plog(f"write state")
    write_full_state(config, new_state)
    write_partial_state(dest_partial, new_partial_state)

    if remove_partial:
        plog(f"remove {remove_partial}", "partial")
        shutil.rmtree(remove_partial)

    full_log.close()
    partial_log.close()


if __name__ == "__main__":
    make_backup(read_config())
