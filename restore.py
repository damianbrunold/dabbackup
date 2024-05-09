import datetime
import json
import os
import shutil
import sys


def base_dir():
    return os.path.normpath(os.path.abspath(os.path.dirname(sys.argv[0])))


def read_config():
    filepath = os.path.join(base_dir(), "backup-config.json")
    with open(filepath, encoding="utf8") as infile:
        return json.load(infile)


def read_full_state(partial_dir):
    with open(
        os.path.join(partial_dir, "__state_full.json"),
        encoding="utf8",
    ) as infile:
        return json.load(infile)


def find_source_prefix(config, fullpath):
    for source_dir in config["source"]["directories"]:
        source_dir = os.path.normpath(source_dir)
        if fullpath.startswith(source_dir):
            return os.path.dirname(source_dir)


def restore(destdir, timestamp):
    if os.path.exists(destdir):
        print(f"ERR: {destdir} exists, abort")
        exit(1)
    config = read_config()
    partial_dir = config["destination"]["directory_partial"]
    history = [
        h 
        for h in sorted(os.listdir(partial_dir), reverse=True)
        if h <= timestamp
    ]
    full_state = read_full_state(os.path.join(partial_dir, history[0]))
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


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("restore [--timestamp <yyyy-mm-dd>] <dest>")
        exit(1)
    if sys.argv[1] == "--timestamp":
        timestamp = sys.argv[2]
        destdir  = sys.argv[3]
    else:
        timestamp = datetime.date.today().strftime("%Y-%d-%h")
        destdir = sys.argv[1]
    restore(destdir, timestamp)
