"""Cross-platform tests for dabbak.

Run with:  python -m unittest discover -s tests
Designed to pass on Linux, macOS, and Windows. Tests that probe Windows-
specific path behavior assert against the helper's pure logic so they
exercise the same code path on every OS; tests that need actual filesystem
behavior use only OS-neutral primitives (tempdir + os.path.join).
"""
import datetime
import json
import os
import shutil
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import dabbak  # noqa: E402


def write_file(path, content="x"):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf8") as f:
        f.write(content)


def read_file(path):
    with open(path, "r", encoding="utf8") as f:
        return f.read()


def make_config(tmp):
    src = os.path.join(tmp, "src")
    full = os.path.join(tmp, "full")
    partial = os.path.join(tmp, "partial")
    os.makedirs(src)
    os.makedirs(full)
    os.makedirs(partial)
    state_file = os.path.join(tmp, "state.json")
    return {
        "source": {
            "directories": [src],
            "excludes": [],
        },
        "destination": {
            "directory_full": full,
            "directory_partial": partial,
        },
        "full_state_file": state_file,
        "packaging_state_file": os.path.join(tmp, "pkg-state.json"),
    }


class TestPureHelpers(unittest.TestCase):
    def test_compute_prefixlen_with_trailing_sep(self):
        self.assertEqual(dabbak.compute_prefixlen("/a/b/"), len("/a/b/"))
        self.assertEqual(dabbak.compute_prefixlen("C:\\"), len("C:\\"))

    def test_compute_prefixlen_without_trailing_sep(self):
        self.assertEqual(dabbak.compute_prefixlen("/a/b"), len("/a/b") + 1)

    def test_mtime_changed_tolerates_small_drift(self):
        self.assertFalse(dabbak.mtime_changed(1000, 1001))
        self.assertTrue(dabbak.mtime_changed(1000, 1002))
        self.assertTrue(dabbak.mtime_changed(1000, 998))

    def test_parse_size_suffixes(self):
        self.assertEqual(dabbak.parse_size("10"), 10)
        self.assertEqual(dabbak.parse_size("2k"), 2048)
        self.assertEqual(dabbak.parse_size("3m"), 3 * 1024 * 1024)
        self.assertEqual(dabbak.parse_size("1G"), 1024 ** 3)

    def test_long_path_posix_passthrough(self):
        if os.name != "nt":
            self.assertEqual(dabbak._long("/foo/bar"), "/foo/bar")
            self.assertEqual(dabbak._long(""), "")

    def test_long_path_windows_logic(self):
        # Force NT behavior to test the prefix logic on any host.
        with mock.patch.object(dabbak.os, "name", "nt"), \
                mock.patch.object(dabbak.os.path, "abspath",
                                  side_effect=lambda p: p):
            self.assertEqual(dabbak._long("C:\\foo"), "\\\\?\\C:\\foo")
            self.assertEqual(
                dabbak._long("\\\\server\\share\\x"),
                "\\\\?\\UNC\\server\\share\\x",
            )
            # idempotent
            self.assertEqual(
                dabbak._long("\\\\?\\C:\\foo"), "\\\\?\\C:\\foo"
            )


class TestFindSourcePrefix(unittest.TestCase):
    def test_plain_source(self):
        config = {"source": {"directories": ["/data/src"]}}
        self.assertEqual(
            dabbak.find_source_prefix(config, "/data/src/a/b.txt"),
            "/data",
        )

    def test_wildcard_source(self):
        config = {"source": {"directories": ["/data/users/*"]}}
        self.assertEqual(
            dabbak.find_source_prefix(config, "/data/users/alice/x.txt"),
            "/data/users",
        )

    def test_no_match(self):
        config = {"source": {"directories": ["/data/src"]}}
        self.assertIsNone(
            dabbak.find_source_prefix(config, "/other/path.txt")
        )

    def test_windows_state_on_posix(self):
        if os.sep != "/":
            self.skipTest("scenario only matters when running on POSIX")
        config = {
            "source": {
                "directories": ["C:\\Users\\me\\Docs"],
                "is-windows": True,
            }
        }
        self.assertEqual(
            dabbak.find_source_prefix(
                config, "C:\\Users\\me\\Docs\\a.txt"
            ),
            "C:\\Users\\me",
        )


class TestExpandSourceDirs(unittest.TestCase):
    def test_plain(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = os.path.join(tmp, "src")
            os.makedirs(d)
            self.assertEqual(
                dabbak.expand_source_dirs([d]),
                [os.path.normpath(d)],
            )

    def test_wildcard_filters_non_dirs(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = os.path.join(tmp, "users")
            os.makedirs(os.path.join(base, "alice"))
            os.makedirs(os.path.join(base, "bob"))
            write_file(os.path.join(base, "stray.txt"))
            got = dabbak.expand_source_dirs([os.path.join(base, "*")])
            self.assertEqual(
                sorted(got),
                sorted([
                    os.path.normpath(os.path.join(base, "alice")),
                    os.path.normpath(os.path.join(base, "bob")),
                ]),
            )


class TestWalk(unittest.TestCase):
    def test_walk_yields_files_and_skips_excludes(self):
        with tempfile.TemporaryDirectory() as tmp:
            write_file(os.path.join(tmp, "a.txt"))
            write_file(os.path.join(tmp, "sub", "b.txt"))
            write_file(os.path.join(tmp, "skip", "c.txt"))
            excludes = [os.path.normpath(os.path.join(tmp, "skip"))]
            got = sorted(dabbak.walk(tmp, excludes))
            self.assertEqual(got, [
                os.path.join(tmp, "a.txt"),
                os.path.join(tmp, "sub", "b.txt"),
            ])


class TestCompileExcludes(unittest.TestCase):
    def test_basename_glob(self):
        m = dabbak.compile_excludes(["*.pyc"])
        self.assertTrue(m(os.path.join("foo", "bar.pyc")))
        self.assertTrue(m("a.pyc"))
        self.assertFalse(m(os.path.join("foo", "bar.py")))

    def test_basename_literal(self):
        m = dabbak.compile_excludes(["__pycache__"])
        self.assertTrue(m(os.path.join("a", "b", "__pycache__")))
        self.assertTrue(m("__pycache__"))
        self.assertFalse(m(os.path.join("a", "pycache_other")))

    def test_absolute_path_legacy(self):
        m = dabbak.compile_excludes([os.path.normpath("/tmp/skip")])
        self.assertTrue(m(os.path.normpath("/tmp/skip")))
        self.assertFalse(m(os.path.normpath("/tmp/skipnot")))

    def test_fullpath_glob(self):
        m = dabbak.compile_excludes(["**/build/*"])
        # fnmatch.fnmatch's `*` matches separators, so this matches paths
        # whose last directory is `build` and that have at least one tail.
        self.assertTrue(m(os.path.normpath("/a/b/build/x")))


class TestWalkExcludes(unittest.TestCase):
    def test_walk_skips_basename_glob(self):
        with tempfile.TemporaryDirectory() as tmp:
            write_file(os.path.join(tmp, "keep.py"))
            write_file(os.path.join(tmp, "drop.pyc"))
            write_file(os.path.join(tmp, "sub", "drop.pyc"))
            got = sorted(dabbak.walk(tmp, ["*.pyc"]))
            self.assertEqual(got, [os.path.join(tmp, "keep.py")])

    def test_walk_skips_named_dir_anywhere(self):
        with tempfile.TemporaryDirectory() as tmp:
            write_file(os.path.join(tmp, "a.py"))
            write_file(os.path.join(tmp, "__pycache__", "x.pyc"))
            write_file(os.path.join(tmp, "src", "__pycache__", "y.pyc"))
            got = sorted(dabbak.walk(tmp, ["__pycache__"]))
            self.assertEqual(got, [os.path.join(tmp, "a.py")])

    def test_walk_absolute_path_still_works(self):
        with tempfile.TemporaryDirectory() as tmp:
            write_file(os.path.join(tmp, "a.py"))
            write_file(os.path.join(tmp, "skip", "x.py"))
            got = sorted(dabbak.walk(
                tmp, [os.path.normpath(os.path.join(tmp, "skip"))]
            ))
            self.assertEqual(got, [os.path.join(tmp, "a.py")])


class TestBackupExcludeIntegration(unittest.TestCase):
    def test_backup_skips_glob_excluded_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = make_config(tmp)
            config["source"]["excludes"] = ["*.pyc", "__pycache__"]
            src = config["source"]["directories"][0]
            full = config["destination"]["directory_full"]
            with mock.patch.object(
                dabbak, "get_full_log",
                return_value=os.path.join(tmp, "backup-full.log"),
            ):
                write_file(os.path.join(src, "a.py"), "code")
                write_file(os.path.join(src, "a.pyc"), "bytecode")
                write_file(os.path.join(src, "pkg", "x.py"), "code")
                write_file(
                    os.path.join(src, "pkg", "__pycache__", "x.cpython.pyc"),
                    "bytecode",
                )
                stats = dabbak.make_backup(config)
                self.assertEqual(stats["new"], 2)  # only the .py files
            self.assertTrue(os.path.exists(
                os.path.join(full, "src", "a.py")
            ))
            self.assertFalse(os.path.exists(
                os.path.join(full, "src", "a.pyc")
            ))
            self.assertFalse(os.path.exists(
                os.path.join(full, "src", "pkg", "__pycache__")
            ))


class TestAtomicStateWrite(unittest.TestCase):
    def test_write_replaces_atomically(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "state.json")
            dabbak.write_full_state_file(path, {"a": [1, 2]})
            self.assertEqual(
                json.loads(read_file(path)), {"a": [1, 2]}
            )
            # tmp file cleaned up
            self.assertFalse(os.path.exists(path + ".tmp"))

    def test_write_overwrites(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "state.json")
            dabbak.write_full_state_file(path, {"a": [1, 2]})
            dabbak.write_full_state_file(path, {"b": [3, 4]})
            self.assertEqual(
                json.loads(read_file(path)), {"b": [3, 4]}
            )


class TestBackupIntegration(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = self.tmp.name
        self.config = make_config(self.root)
        self.src = self.config["source"]["directories"][0]
        self.full = self.config["destination"]["directory_full"]
        self.partial_base = self.config["destination"]["directory_partial"]
        # redirect the full log into the tempdir
        self._log_patch = mock.patch.object(
            dabbak, "get_full_log",
            return_value=os.path.join(self.root, "backup-full.log"),
        )
        self._log_patch.start()
        self.addCleanup(self._log_patch.stop)

    def today_partial(self):
        import datetime
        return os.path.join(
            self.partial_base,
            datetime.date.today().strftime("%Y-%m-%d"),
        )

    def test_initial_backup_copies_everything(self):
        write_file(os.path.join(self.src, "a.txt"), "hello")
        write_file(os.path.join(self.src, "sub", "b.txt"), "world")
        dabbak.make_backup(self.config)
        # mirror in dest_full
        self.assertEqual(
            read_file(os.path.join(self.full, "src", "a.txt")), "hello"
        )
        self.assertEqual(
            read_file(os.path.join(self.full, "src", "sub", "b.txt")),
            "world",
        )
        # snapshot in dated partial
        snap = self.today_partial()
        self.assertEqual(
            read_file(os.path.join(snap, "src", "a.txt")), "hello"
        )
        # state written
        self.assertTrue(os.path.exists(self.config["full_state_file"]))
        # __state.json embedded in snapshot
        self.assertTrue(
            os.path.exists(os.path.join(snap, "__state.json"))
        )
        self.assertFalse(
            os.path.exists(os.path.join(snap, "__incomplete"))
        )

    def test_unchanged_file_not_recopied(self):
        write_file(os.path.join(self.src, "a.txt"), "hello")
        dabbak.make_backup(self.config)
        snap = self.today_partial()
        # remove from snapshot so we can detect a re-copy
        os.remove(os.path.join(snap, "src", "a.txt"))
        dabbak.make_backup(self.config)
        self.assertFalse(
            os.path.exists(os.path.join(snap, "src", "a.txt")),
            "unchanged file should not be re-copied",
        )

    def test_changed_file_is_recopied(self):
        write_file(os.path.join(self.src, "a.txt"), "hello")
        dabbak.make_backup(self.config)
        # ensure mtime ticks past the 2s tolerance
        path = os.path.join(self.src, "a.txt")
        st = os.stat(path)
        os.utime(path, (st.st_atime, st.st_mtime + 10))
        write_file(path, "HELLO!")
        dabbak.make_backup(self.config)
        self.assertEqual(
            read_file(os.path.join(self.full, "src", "a.txt")), "HELLO!"
        )

    def test_deleted_file_removed_from_full(self):
        a = os.path.join(self.src, "a.txt")
        b = os.path.join(self.src, "b.txt")
        write_file(a, "1")
        write_file(b, "2")
        dabbak.make_backup(self.config)
        os.remove(a)
        dabbak.make_backup(self.config)
        self.assertFalse(
            os.path.exists(os.path.join(self.full, "src", "a.txt"))
        )
        self.assertTrue(
            os.path.exists(os.path.join(self.full, "src", "b.txt"))
        )

    def test_dry_run_writes_nothing(self):
        write_file(os.path.join(self.src, "a.txt"), "hello")
        dabbak.make_backup(self.config, dry_run=True)
        self.assertFalse(
            os.path.exists(os.path.join(self.full, "src", "a.txt"))
        )
        self.assertFalse(
            os.path.exists(self.config["full_state_file"])
        )

    def test_exception_midrun_preserves_state(self):
        write_file(os.path.join(self.src, "a.txt"), "1")
        write_file(os.path.join(self.src, "b.txt"), "2")
        dabbak.make_backup(self.config)
        old_state = json.loads(read_file(self.config["full_state_file"]))
        self.assertEqual(len(old_state), 2)

        # Modify a.txt so the second run wants to re-copy it, then delete
        # b.txt from disk. If the run completes normally, the deletion pass
        # would drop b.txt from state. We force an exception AFTER a.txt
        # is processed but BEFORE the walk finishes.
        path_a = os.path.join(self.src, "a.txt")
        st = os.stat(path_a)
        os.utime(path_a, (st.st_atime, st.st_mtime + 10))
        write_file(path_a, "11")

        # Simulate an abrupt failure that bypasses the per-file try/except —
        # walk() itself raising mid-iteration is the realistic case (USB
        # unplug, network share drop). KeyboardInterrupt is a BaseException
        # and is what we'd see on Ctrl-C; the code path is identical.
        real_walk = dabbak.walk

        def truncated_walk(d, ex):
            for i, p in enumerate(real_walk(d, ex)):
                if i >= 1:
                    raise KeyboardInterrupt("simulated abort")
                yield p

        with mock.patch.object(dabbak, "walk", side_effect=truncated_walk):
            dabbak.make_backup(self.config)

        new_state = json.loads(read_file(self.config["full_state_file"]))
        # b.txt's old state entry must survive — otherwise next run would
        # treat it as "new" and re-copy, or as "deleted" and remove it.
        b_key = os.path.join(self.src, "b.txt")
        self.assertIn(b_key, new_state)
        # __incomplete marker present in today's partial
        self.assertTrue(
            os.path.exists(
                os.path.join(self.today_partial(), "__incomplete")
            )
        )

    def test_incomplete_snapshot_skipped_by_restore(self):
        write_file(os.path.join(self.src, "a.txt"), "hello")
        dabbak.make_backup(self.config)
        # Mark today's snapshot as incomplete
        marker = os.path.join(self.today_partial(), "__incomplete")
        with open(marker, "w") as f:
            f.write("x")
        # Restore must error out cleanly (history is empty) rather than
        # trusting the incomplete snapshot's __state.json.
        destdir = os.path.join(self.root, "restore-target")
        with self.assertRaises(SystemExit):
            dabbak.restore(self.config, destdir,
                           __import__("datetime").date.today()
                           .strftime("%Y-%m-%d"), "")


class TestDeletionFailure(unittest.TestCase):
    """A failed mirror-delete must keep the state entry so the next run
    retries, AND surface the error to the errors list. Otherwise the file
    becomes a silent orphan in dest_full forever.
    """

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = self.tmp.name
        self.config = make_config(self.root)
        self.src = self.config["source"]["directories"][0]
        self.full = self.config["destination"]["directory_full"]
        self._log_patch = mock.patch.object(
            dabbak, "get_full_log",
            return_value=os.path.join(self.root, "backup-full.log"),
        )
        self._log_patch.start()
        self.addCleanup(self._log_patch.stop)

    def test_failed_delete_keeps_state_entry(self):
        # First run: two files in source.
        a = os.path.join(self.src, "a.txt")
        b = os.path.join(self.src, "b.txt")
        write_file(a, "A")
        write_file(b, "B")
        dabbak.make_backup(self.config)

        # Source file b is deleted; mirror's b will be the deletion target.
        os.remove(b)

        # Force the mirror-side delete to fail.
        real_remove = dabbak.remove_file

        def boom_on_b(path, dest_full):
            if path.endswith(os.sep + "b.txt") and dest_full == self.full:
                return False, "simulated removal failure"
            return real_remove(path, dest_full)

        with mock.patch.object(dabbak, "remove_file", side_effect=boom_on_b):
            stats = dabbak.make_backup(self.config)
        self.assertEqual(stats["deleted"], 0)
        self.assertGreaterEqual(stats["failed"], 1)

        # b is still in state -> next run will try again.
        state = json.loads(read_file(self.config["full_state_file"]))
        self.assertIn(b, state)

        # The actual file b.txt is still in dest_full because deletion
        # failed. Next run with a working remove_file actually deletes it.
        self.assertTrue(os.path.exists(os.path.join(self.full, "src", "b.txt")))
        dabbak.make_backup(self.config)
        self.assertFalse(os.path.exists(os.path.join(self.full, "src", "b.txt")))
        state = json.loads(read_file(self.config["full_state_file"]))
        self.assertNotIn(b, state)


class TestRemoveFile(unittest.TestCase):
    def test_success_prunes_empty_parents(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = os.path.join(tmp, "root")
            deep = os.path.join(root, "a", "b", "c")
            os.makedirs(deep)
            target = os.path.join(deep, "x.txt")
            write_file(target, "x")
            ok, err = dabbak.remove_file(target, root)
            self.assertTrue(ok)
            self.assertIsNone(err)
            # a/b/c chain is now empty -> pruned. root preserved.
            self.assertTrue(os.path.exists(root))
            self.assertFalse(os.path.exists(os.path.join(root, "a")))

    def test_failure_returns_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            ok, err = dabbak.remove_file(
                os.path.join(tmp, "nonexistent.txt"), tmp,
            )
            self.assertFalse(ok)
            self.assertIsNotNone(err)
            self.assertIn("failed to delete", err)


class TestFailureSemantics(unittest.TestCase):
    """F5/F8: a copy failure must not mark the file done in state, so the
    next run retries it.
    """
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = self.tmp.name
        self.config = make_config(self.root)
        self.src = self.config["source"]["directories"][0]
        self._log_patch = mock.patch.object(
            dabbak, "get_full_log",
            return_value=os.path.join(self.root, "backup-full.log"),
        )
        self._log_patch.start()
        self.addCleanup(self._log_patch.stop)

    def test_new_file_copy_failure_not_recorded(self):
        write_file(os.path.join(self.src, "a.txt"), "hello")
        real_copy = dabbak.fs_copy2
        state_file = self.config["full_state_file"]

        def fail_data_copies(s, d):
            # Let the post-run __state.json copy succeed; only fail data
            # copies (those whose source is the source tree).
            if str(s) == state_file:
                return real_copy(s, d)
            raise OSError("simulated copy failure")

        with mock.patch.object(
            dabbak, "fs_copy2", side_effect=fail_data_copies
        ):
            stats = dabbak.make_backup(self.config)
        self.assertEqual(stats["failed"], 1)
        self.assertEqual(stats["new"], 0)
        state = json.loads(read_file(self.config["full_state_file"]))
        # Failed new file must NOT be in state — next run retries as "new".
        self.assertNotIn(os.path.join(self.src, "a.txt"), state)

        # Now run normally and verify retry works.
        stats2 = dabbak.make_backup(self.config)
        self.assertEqual(stats2["new"], 1)
        state2 = json.loads(read_file(self.config["full_state_file"]))
        self.assertIn(os.path.join(self.src, "a.txt"), state2)

    def test_changed_file_copy_failure_preserves_old_state(self):
        path = os.path.join(self.src, "a.txt")
        write_file(path, "v1")
        dabbak.make_backup(self.config)
        old_state = json.loads(read_file(self.config["full_state_file"]))
        old_entry = old_state[path]

        # Bump mtime + content; first failed retry, then succeed.
        st = os.stat(path)
        os.utime(path, (st.st_atime, st.st_mtime + 10))
        write_file(path, "v2-bigger")

        real_copy = dabbak.fs_copy2
        state_file = self.config["full_state_file"]

        def fail_data_copies(s, d):
            if str(s) == state_file:
                return real_copy(s, d)
            raise OSError("boom")

        with mock.patch.object(
            dabbak, "fs_copy2", side_effect=fail_data_copies
        ):
            stats = dabbak.make_backup(self.config)
        self.assertEqual(stats["failed"], 1)
        # Old entry must be preserved so we still detect a change next run.
        mid_state = json.loads(read_file(self.config["full_state_file"]))
        self.assertEqual(mid_state[path], old_entry)

        # Real retry succeeds.
        stats2 = dabbak.make_backup(self.config)
        self.assertEqual(stats2["changed"], 1)
        self.assertEqual(
            read_file(os.path.join(
                self.config["destination"]["directory_full"],
                "src", "a.txt",
            )),
            "v2-bigger",
        )


class TestSummary(unittest.TestCase):
    def test_summary_counts(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = make_config(tmp)
            src = config["source"]["directories"][0]
            with mock.patch.object(
                dabbak, "get_full_log",
                return_value=os.path.join(tmp, "backup-full.log"),
            ):
                write_file(os.path.join(src, "a"), "1")
                write_file(os.path.join(src, "b"), "2")
                s = dabbak.make_backup(config)
                self.assertEqual(s["new"], 2)
                self.assertEqual(s["changed"], 0)
                self.assertEqual(s["unchanged"], 0)

                # Second run: nothing changed.
                s = dabbak.make_backup(config)
                self.assertEqual(s["new"], 0)
                self.assertEqual(s["unchanged"], 2)

                # Change one, delete one.
                pa = os.path.join(src, "a")
                st = os.stat(pa)
                os.utime(pa, (st.st_atime, st.st_mtime + 10))
                write_file(pa, "1-changed")
                os.remove(os.path.join(src, "b"))
                s = dabbak.make_backup(config)
                self.assertEqual(s["changed"], 1)
                self.assertEqual(s["deleted"], 1)


class TestLogRotation(unittest.TestCase):
    def test_rotate_when_large(self):
        with tempfile.TemporaryDirectory() as tmp:
            log = os.path.join(tmp, "big.log")
            with open(log, "wb") as f:
                f.write(b"x" * 200)
            dabbak.rotate_log_if_large(log, max_bytes=100)
            self.assertFalse(os.path.exists(log))
            self.assertTrue(os.path.exists(log + ".1"))

    def test_no_rotate_when_small(self):
        with tempfile.TemporaryDirectory() as tmp:
            log = os.path.join(tmp, "small.log")
            with open(log, "wb") as f:
                f.write(b"x" * 50)
            dabbak.rotate_log_if_large(log, max_bytes=100)
            self.assertTrue(os.path.exists(log))
            self.assertFalse(os.path.exists(log + ".1"))

    def test_rotate_discards_previous_backup(self):
        with tempfile.TemporaryDirectory() as tmp:
            log = os.path.join(tmp, "x.log")
            with open(log, "wb") as f:
                f.write(b"new" * 50)
            with open(log + ".1", "wb") as f:
                f.write(b"OLD")
            dabbak.rotate_log_if_large(log, max_bytes=10)
            with open(log + ".1", "rb") as f:
                self.assertNotEqual(f.read(), b"OLD")


class TestVerbosity(unittest.TestCase):
    def _run(self, **kw):
        import io, contextlib
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp, True)
        config = make_config(tmp)
        src = config["source"]["directories"][0]
        write_file(os.path.join(src, "a.txt"), "x")
        write_file(os.path.join(src, "b.txt"), "y")
        buf = io.StringIO()
        with mock.patch.object(
            dabbak, "get_full_log",
            return_value=os.path.join(tmp, "backup-full.log"),
        ), contextlib.redirect_stdout(buf):
            dabbak.make_backup(config, **kw)
        return buf.getvalue(), config

    def test_default_prints_per_file(self):
        out, _ = self._run()
        self.assertIn("++", out)
        self.assertIn("summary:", out)

    def test_quiet_suppresses_per_file_but_keeps_summary(self):
        out, _ = self._run(quiet=True)
        self.assertNotIn("++", out)
        self.assertIn("summary:", out)

    def test_json_emits_parseable_summary(self):
        out, _ = self._run(json_out=True)
        # JSON object is the only thing on stdout; per-file lines suppressed.
        self.assertNotIn("++", out)
        self.assertNotIn("summary:", out)
        payload = json.loads(out.strip())
        self.assertEqual(payload["new"], 2)
        self.assertTrue(payload["completed"])
        self.assertIn("elapsed_seconds", payload)


class TestProgress(unittest.TestCase):
    def test_estimate_from_prev_state(self):
        prev = {"/a": [100, 1], "/b": [200, 2]}
        p = dabbak.Progress(prev, enabled=True, interval=0)
        self.assertEqual(p.files_total, 2)
        self.assertEqual(p.bytes_total, 300)

    def test_no_estimate_when_empty(self):
        p = dabbak.Progress({}, enabled=True, interval=0)
        self.assertEqual(p.files_total, 0)
        self.assertEqual(p.bytes_total, 0)

    def test_tick_updates_running_totals(self):
        p = dabbak.Progress({}, enabled=False)
        p.tick("/a", 100)
        p.tick("/b", 200)
        self.assertEqual(p.files_done, 0)  # disabled
        self.assertEqual(p.bytes_done, 0)

        p2 = dabbak.Progress({}, enabled=True, interval=999)
        p2.tick("/a", 100)
        p2.tick("/b", 200)
        self.assertEqual(p2.files_done, 2)
        self.assertEqual(p2.bytes_done, 300)


class TestErrorCountInStats(unittest.TestCase):
    def test_clean_run_has_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = make_config(tmp)
            src = config["source"]["directories"][0]
            with mock.patch.object(
                dabbak, "get_full_log",
                return_value=os.path.join(tmp, "backup-full.log"),
            ):
                write_file(os.path.join(src, "a.txt"), "x")
                stats = dabbak.make_backup(config)
            self.assertEqual(stats["error_count"], 0)
            self.assertTrue(stats["completed"])

    def test_failed_copy_bumps_error_count(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = make_config(tmp)
            src = config["source"]["directories"][0]
            write_file(os.path.join(src, "a.txt"), "x")
            real_copy = dabbak.fs_copy2
            state_file = config["full_state_file"]

            def fail_data_copies(s, d):
                if str(s) == state_file:
                    return real_copy(s, d)
                raise OSError("simulated")

            with mock.patch.object(
                dabbak, "get_full_log",
                return_value=os.path.join(tmp, "backup-full.log"),
            ), mock.patch.object(
                dabbak, "fs_copy2", side_effect=fail_data_copies
            ):
                stats = dabbak.make_backup(config)
            self.assertGreater(stats["error_count"], 0)


class TestFormatSize(unittest.TestCase):
    def test_units(self):
        self.assertEqual(dabbak.format_size(0), "0 B")
        self.assertEqual(dabbak.format_size(500), "500 B")
        self.assertTrue(dabbak.format_size(1500).endswith("KB"))
        self.assertTrue(dabbak.format_size(2 * 1024 ** 3).endswith("GB"))


class TestListAndPrune(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.config = make_config(self.tmp.name)
        self.partial = self.config["destination"]["directory_partial"]

    def _make_snapshot(self, date, files=None, incomplete=False):
        d = os.path.join(self.partial, date)
        os.makedirs(d, exist_ok=True)
        for name, content in (files or {"src/a.txt": "x"}).items():
            write_file(os.path.join(d, name), content)
        if incomplete:
            with open(os.path.join(d, "__incomplete"), "w") as f:
                f.write("x")
        with open(
            os.path.join(self.partial, f"backup-partial-{date}.log"), "w"
        ) as f:
            f.write("log\n")

    def test_enumerate_skips_non_date_entries(self):
        self._make_snapshot("2026-05-10")
        self._make_snapshot("2026-05-11", incomplete=True)
        os.makedirs(os.path.join(self.partial, "not-a-date"))
        snaps = dabbak.enumerate_snapshots(self.partial)
        self.assertEqual([s["date"] for s in snaps],
                         ["2026-05-11", "2026-05-10"])
        self.assertTrue(snaps[0]["incomplete"])
        self.assertFalse(snaps[1]["incomplete"])
        self.assertEqual(snaps[0]["file_count"], 1)

    def test_select_keep_last(self):
        snaps = [
            {"date": "2026-05-14"},
            {"date": "2026-05-13"},
            {"date": "2026-05-12"},
            {"date": "2026-05-11"},
        ]
        td = dabbak.select_snapshots_to_prune(
            snaps, keep_last=2,
            today=datetime.date(2026, 5, 14),
        )
        # Today is always kept regardless; keep_last keeps 2 most recent.
        # Today (2026-05-14) and 2026-05-13 remain.
        self.assertEqual(
            [s["date"] for s in td], ["2026-05-12", "2026-05-11"]
        )

    def test_select_keep_days(self):
        snaps = [
            {"date": "2026-05-14"},
            {"date": "2026-05-13"},
            {"date": "2026-05-10"},
            {"date": "2026-05-01"},
        ]
        td = dabbak.select_snapshots_to_prune(
            snaps, keep_days=5,
            today=datetime.date(2026, 5, 14),
        )
        # Within 5 days of 2026-05-14: >= 2026-05-09. Drops 05-01 only;
        # 05-10 and 05-13 are within window; today always kept.
        self.assertEqual([s["date"] for s in td], ["2026-05-01"])

    def test_select_combined_policies(self):
        snaps = [{"date": f"2026-05-{d:02d}"} for d in (14, 13, 12, 11, 1)]
        td = dabbak.select_snapshots_to_prune(
            snaps, keep_last=2, keep_days=3,
            today=datetime.date(2026, 5, 14),
        )
        # keep_last=2 -> keep 05-14, 05-13. keep_days=3 -> keep >= 05-11.
        # Union of kept: 14, 13, 12, 11. Delete: 05-01.
        self.assertEqual([s["date"] for s in td], ["2026-05-01"])

    def test_prune_dry_run_deletes_nothing(self):
        self._make_snapshot("2026-05-01")
        self._make_snapshot("2026-05-14")
        r = dabbak.cmd_prune(self.config, keep_last=1, force=False)
        self.assertEqual(r["deleted"], [])
        self.assertIn("2026-05-01", r["would_delete"])
        self.assertTrue(
            os.path.isdir(os.path.join(self.partial, "2026-05-01"))
        )

    def test_prune_force_deletes_folder_and_log(self):
        self._make_snapshot("2026-05-01")
        self._make_snapshot("2026-05-14")
        r = dabbak.cmd_prune(self.config, keep_last=1, force=True)
        self.assertIn("2026-05-01", r["deleted"])
        self.assertFalse(
            os.path.isdir(os.path.join(self.partial, "2026-05-01"))
        )
        self.assertFalse(os.path.exists(
            os.path.join(self.partial, "backup-partial-2026-05-01.log")
        ))
        # 2026-05-14 (today, depending on system date) or just newer kept
        self.assertTrue(
            os.path.isdir(os.path.join(self.partial, "2026-05-14"))
        )


class TestRestore(unittest.TestCase):
    def test_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = make_config(tmp)
            src = config["source"]["directories"][0]
            with mock.patch.object(
                dabbak, "get_full_log",
                return_value=os.path.join(tmp, "backup-full.log"),
            ):
                write_file(os.path.join(src, "a.txt"), "hello")
                write_file(os.path.join(src, "sub", "b.txt"), "world")
                dabbak.make_backup(config)
                target = os.path.join(tmp, "out")
                import datetime as _dt
                dabbak.restore(
                    config, target,
                    _dt.date.today().strftime("%Y-%m-%d"), "",
                )
                self.assertEqual(
                    read_file(os.path.join(target, "src", "a.txt")),
                    "hello",
                )
                self.assertEqual(
                    read_file(
                        os.path.join(target, "src", "sub", "b.txt")
                    ),
                    "world",
                )


class TestRestoreExtensions(unittest.TestCase):
    """Q3: --dry-run, --force, glob patterns."""

    def _setup(self, tmp):
        config = make_config(tmp)
        src = config["source"]["directories"][0]
        with mock.patch.object(
            dabbak, "get_full_log",
            return_value=os.path.join(tmp, "backup-full.log"),
        ):
            write_file(os.path.join(src, "a.txt"), "A")
            write_file(os.path.join(src, "sub", "b.txt"), "B")
            write_file(os.path.join(src, "sub", "c.log"), "C")
            dabbak.make_backup(config)
        return config, src

    def test_dry_run_writes_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            config, _ = self._setup(tmp)
            target = os.path.join(tmp, "out")
            dabbak.restore(config, target, datetime.date.today().isoformat(),
                           patterns=[], dry_run=True)
            self.assertFalse(os.path.exists(target))

    def test_force_into_existing_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            config, _ = self._setup(tmp)
            target = os.path.join(tmp, "out")
            os.makedirs(target)
            write_file(os.path.join(target, "preexisting"), "x")
            # Without force this would sys.exit.
            dabbak.restore(config, target, datetime.date.today().isoformat(),
                           patterns=[], force=True)
            self.assertTrue(os.path.exists(
                os.path.join(target, "src", "a.txt")
            ))
            # preexisting file untouched
            self.assertEqual(
                read_file(os.path.join(target, "preexisting")), "x"
            )

    def test_existing_dir_refused_without_force(self):
        with tempfile.TemporaryDirectory() as tmp:
            config, _ = self._setup(tmp)
            target = os.path.join(tmp, "out")
            os.makedirs(target)
            with self.assertRaises(SystemExit):
                dabbak.restore(
                    config, target,
                    datetime.date.today().isoformat(),
                    patterns=[],
                )

    def test_glob_pattern(self):
        with tempfile.TemporaryDirectory() as tmp:
            config, src = self._setup(tmp)
            target = os.path.join(tmp, "out")
            dabbak.restore(
                config, target, datetime.date.today().isoformat(),
                patterns=["*.txt"],
            )
            # b.txt under sub/ matches *.txt? fnmatchcase: "*" matches /
            # too, so any path ending in .txt qualifies — both a.txt and
            # sub/b.txt match. c.log does NOT.
            self.assertTrue(os.path.exists(
                os.path.join(target, "src", "a.txt")
            ))
            self.assertTrue(os.path.exists(
                os.path.join(target, "src", "sub", "b.txt")
            ))
            self.assertFalse(os.path.exists(
                os.path.join(target, "src", "sub", "c.log")
            ))

    def test_prefix_pattern_legacy(self):
        with tempfile.TemporaryDirectory() as tmp:
            config, src = self._setup(tmp)
            target = os.path.join(tmp, "out")
            dabbak.restore(
                config, target, datetime.date.today().isoformat(),
                patterns=[os.path.join(src, "sub")],
            )
            self.assertFalse(os.path.exists(
                os.path.join(target, "src", "a.txt")
            ))
            self.assertTrue(os.path.exists(
                os.path.join(target, "src", "sub", "b.txt")
            ))


class TestRestoreCLIBackCompat(unittest.TestCase):
    """The old CLI was: `restore <dest> [<YYYY-MM-DD> [<source-path>]]`.
    The new CLI exposes `-t/--timestamp` + positional patterns. Old
    invocations must still resolve the date as a timestamp, not as a
    path pattern.
    """

    def test_old_positional_date_accepted(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = make_config(tmp)
            src = config["source"]["directories"][0]
            cfg_path = os.path.join(tmp, "config.json")
            with open(cfg_path, "w") as f:
                json.dump(config, f)
            with mock.patch.object(
                dabbak, "get_full_log",
                return_value=os.path.join(tmp, "backup-full.log"),
            ), mock.patch.object(
                dabbak, "read_config", return_value=config
            ):
                write_file(os.path.join(src, "a.txt"), "hello")
                dabbak.make_backup(config)
                today = datetime.date.today().isoformat()
                target = os.path.join(tmp, "out")
                # Old CLI form: dest then positional date
                dabbak.main(["restore", target, today])
                self.assertTrue(os.path.exists(
                    os.path.join(target, "src", "a.txt")
                ))

    def test_old_positional_date_and_source_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = make_config(tmp)
            src = config["source"]["directories"][0]
            with mock.patch.object(
                dabbak, "get_full_log",
                return_value=os.path.join(tmp, "backup-full.log"),
            ), mock.patch.object(
                dabbak, "read_config", return_value=config
            ):
                write_file(os.path.join(src, "a.txt"), "A")
                write_file(os.path.join(src, "sub", "b.txt"), "B")
                dabbak.make_backup(config)
                today = datetime.date.today().isoformat()
                target = os.path.join(tmp, "out")
                # Old CLI form: dest, date, source-path prefix
                dabbak.main(
                    ["restore", target, today, os.path.join(src, "sub")]
                )
                self.assertFalse(os.path.exists(
                    os.path.join(target, "src", "a.txt")
                ))
                self.assertTrue(os.path.exists(
                    os.path.join(target, "src", "sub", "b.txt")
                ))


class TestFileLock(unittest.TestCase):
    def test_concurrent_acquire_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "x.lock")
            with dabbak.FileLock(path):
                with self.assertRaises(dabbak.LockHeld):
                    with dabbak.FileLock(path):
                        pass

    def test_lock_released_after_exit(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "x.lock")
            with dabbak.FileLock(path):
                pass
            # Second acquire works.
            with dabbak.FileLock(path):
                pass

    def test_lock_released_on_exception(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "x.lock")
            try:
                with dabbak.FileLock(path):
                    raise RuntimeError("oops")
            except RuntimeError:
                pass
            with dabbak.FileLock(path):
                pass

    def test_lock_file_removed_after_release(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "x.lock")
            with dabbak.FileLock(path):
                self.assertTrue(os.path.exists(path))
            self.assertFalse(os.path.exists(path))

    def test_main_backup_locked(self):
        """CLI backup is wrapped in _with_lock — a concurrent invocation
        must exit with status 1 and not corrupt state."""
        with tempfile.TemporaryDirectory() as tmp:
            config = make_config(tmp)
            src = config["source"]["directories"][0]
            write_file(os.path.join(src, "a.txt"), "x")
            with mock.patch.object(
                dabbak, "get_full_log",
                return_value=os.path.join(tmp, "backup-full.log"),
            ), mock.patch.object(
                dabbak, "read_config", return_value=config
            ):
                # Pre-acquire the lock from this process, then invoke main.
                with dabbak.FileLock(dabbak.lock_path_for(config)):
                    with self.assertRaises(SystemExit) as ctx:
                        dabbak.main(["backup", "--quiet"])
                    self.assertEqual(ctx.exception.code, 1)
                # State file untouched (backup never ran).
                self.assertFalse(os.path.exists(config["full_state_file"]))
                # Now the lock is released; backup proceeds normally.
                dabbak.main(["backup", "--quiet"])
                self.assertTrue(os.path.exists(config["full_state_file"]))


class TestInit(unittest.TestCase):
    def test_init_creates_template(self):
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.object(dabbak, "base_dir", return_value=tmp):
                dabbak.cmd_init()
                path = os.path.join(tmp, "backup-config.json")
                self.assertTrue(os.path.exists(path))
                data = json.loads(read_file(path))
                self.assertIn("source", data)
                self.assertIn("destination", data)

    def test_init_refuses_existing(self):
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.object(dabbak, "base_dir", return_value=tmp):
                dabbak.cmd_init()
                with self.assertRaises(SystemExit):
                    dabbak.cmd_init()

    def test_init_force_overwrites(self):
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.object(dabbak, "base_dir", return_value=tmp):
                dabbak.cmd_init()
                path = os.path.join(tmp, "backup-config.json")
                with open(path, "w") as f:
                    f.write("garbage")
                dabbak.cmd_init(force=True)
                json.loads(read_file(path))  # parses again


class TestRefreshState(unittest.TestCase):
    def test_rebuilds_state_from_mirror(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = make_config(tmp)
            src = config["source"]["directories"][0]
            with mock.patch.object(
                dabbak, "get_full_log",
                return_value=os.path.join(tmp, "backup-full.log"),
            ):
                write_file(os.path.join(src, "a.txt"), "hello")
                dabbak.make_backup(config)
                # delete the state file and rebuild
                os.remove(config["full_state_file"])
                dabbak.refresh_state(config)
                self.assertTrue(
                    os.path.exists(config["full_state_file"])
                )
                state = json.loads(read_file(config["full_state_file"]))
                self.assertIn(os.path.join(src, "a.txt"), state)


if __name__ == "__main__":
    unittest.main()
