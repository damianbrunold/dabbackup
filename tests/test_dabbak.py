"""Cross-platform tests for dabbak.

Run with:  python -m unittest discover -s tests
Designed to pass on Linux, macOS, and Windows. Tests that probe Windows-
specific path behavior assert against the helper's pure logic so they
exercise the same code path on every OS; tests that need actual filesystem
behavior use only OS-neutral primitives (tempdir + os.path.join).
"""
import json
import os
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
        with self.assertRaises(IndexError):
            dabbak.restore(self.config, destdir,
                           __import__("datetime").date.today()
                           .strftime("%Y-%m-%d"), "")


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


class TestFormatSize(unittest.TestCase):
    def test_units(self):
        self.assertEqual(dabbak.format_size(0), "0 B")
        self.assertEqual(dabbak.format_size(500), "500 B")
        self.assertTrue(dabbak.format_size(1500).endswith("KB"))
        self.assertTrue(dabbak.format_size(2 * 1024 ** 3).endswith("GB"))


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
