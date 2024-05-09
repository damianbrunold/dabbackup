import json
import os
import sys


def base_dir():
    return os.path.normpath(os.path.abspath(os.path.dirname(sys.argv[0])))


def read_config():
    filepath = os.path.join(base_dir(), "backup-config.json")
    with open(filepath, encoding="utf8") as infile:
        return json.load(infile)


def write_full_state(config, state):
    filepath = config["full_state_file"]
    with open(filepath, "w", encoding="utf8") as outfile:
        json.dump(state, outfile, indent=2, ensure_ascii=False)


def walk(directory):
    for path in sorted(os.listdir(directory)):
        fullpath = os.path.join(directory, path)
        if os.path.isdir(fullpath):
            yield from walk(fullpath)
        else:
            yield fullpath


def refresh_state(config):
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
        for filepath in walk(destdir):
            fstat = os.stat(filepath)
            srcpath = os.path.join(sourcedir, filepath[prefixlen2:])
            new_state[srcpath] = [
                fstat.st_size,
                int(fstat.st_mtime),
            ]
    write_full_state(config, new_state)


if __name__ == "__main__":
    refresh_state(read_config())
