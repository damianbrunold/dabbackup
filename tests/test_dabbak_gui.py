"""Tests for the GUI's non-Tk helpers.

The Tk widgets themselves require a display and are not exercised here;
test infrastructure on headless CI systems doesn't have one. The helpers
factored out into module-level functions are where the real logic lives,
so testing them gets the coverage that matters.
"""
import io
import json
import os
import queue
import sys
import tempfile
import unittest

sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)

import dabbak  # noqa: E402
import dabbak_gui  # noqa: E402


def write_file(path, content="x"):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf8") as f:
        f.write(content)


class TestSearchPaths(unittest.TestCase):
    def test_empty_pattern_returns_all(self):
        paths = ["/a/x.txt", "/b/y.txt"]
        self.assertEqual(dabbak_gui.search_paths(paths, ""), paths)

    def test_substring_case_insensitive(self):
        paths = [
            "/home/me/Documents/Report.docx",
            "/home/me/Downloads/cat.jpg",
            "/home/me/projects/report.md",
        ]
        got = dabbak_gui.search_paths(paths, "report")
        self.assertEqual(
            set(got),
            {
                "/home/me/Documents/Report.docx",
                "/home/me/projects/report.md",
            },
        )

    def test_glob_pattern(self):
        paths = [
            "/a/x.txt",
            "/a/y.md",
            "/b/x.txt",
        ]
        got = dabbak_gui.search_paths(paths, "*.txt")
        self.assertEqual(set(got), {"/a/x.txt", "/b/x.txt"})

    def test_glob_case_insensitive(self):
        paths = ["/a/Foo.PDF", "/a/bar.pdf"]
        got = dabbak_gui.search_paths(paths, "*.pdf")
        self.assertEqual(set(got), set(paths))


class TestSnapshotManifestPaths(unittest.TestCase):
    def test_reads_state_keys(self):
        with tempfile.TemporaryDirectory() as tmp:
            snap = os.path.join(tmp, "2026-05-14")
            os.makedirs(snap)
            with open(
                os.path.join(snap, "__state.json"), "w", encoding="utf8"
            ) as f:
                json.dump(
                    {"/src/a.txt": [1, 100], "/src/b.txt": [2, 200]}, f
                )
            got = dabbak_gui.snapshot_manifest_paths(tmp, "2026-05-14")
            self.assertEqual(got, ["/src/a.txt", "/src/b.txt"])

    def test_missing_snapshot_returns_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(
                dabbak_gui.snapshot_manifest_paths(tmp, "2099-01-01"),
                [],
            )

    def test_incomplete_snapshot_with_no_manifest_returns_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            snap = os.path.join(tmp, "2026-05-14")
            os.makedirs(snap)
            # No __state.json file -> empty list.
            self.assertEqual(
                dabbak_gui.snapshot_manifest_paths(tmp, "2026-05-14"),
                [],
            )


class TestSaveConfigAtomic(unittest.TestCase):
    def test_writes_and_replaces(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "cfg.json")
            cfg = {"source": {"directories": ["/a"], "excludes": []}}
            dabbak_gui.save_config_atomic(path, cfg)
            self.assertTrue(os.path.exists(path))
            self.assertEqual(json.load(open(path)), cfg)
            self.assertFalse(os.path.exists(path + ".tmp"))

    def test_overwrites_existing(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "cfg.json")
            dabbak_gui.save_config_atomic(path, {"k": "v1"})
            dabbak_gui.save_config_atomic(path, {"k": "v2"})
            self.assertEqual(json.load(open(path))["k"], "v2")


class TestQueueIO(unittest.TestCase):
    def test_lines_split_on_newline(self):
        q = queue.Queue()
        sink = dabbak_gui.QueueIO(q, tag="out")
        sink.write("hello\nworld\n")
        sink.flush()
        msgs = []
        while not q.empty():
            msgs.append(q.get_nowait())
        self.assertEqual(msgs, [("out", "hello"), ("out", "world")])

    def test_carriage_return_treated_as_newline(self):
        # Progress lines use \r overwrite; we surface each as its own line.
        q = queue.Queue()
        sink = dabbak_gui.QueueIO(q, tag="out")
        sink.write("a\rb\rc\n")
        sink.flush()
        msgs = []
        while not q.empty():
            msgs.append(q.get_nowait())
        self.assertEqual(
            msgs, [("out", "a"), ("out", "b"), ("out", "c")]
        )

    def test_partial_line_flushed(self):
        q = queue.Queue()
        sink = dabbak_gui.QueueIO(q, tag="x")
        sink.write("no-newline-here")
        # Without flush, nothing is enqueued.
        self.assertTrue(q.empty())
        sink.flush()
        self.assertEqual(q.get_nowait(), ("x", "no-newline-here"))


class TestCliGuiSubcommand(unittest.TestCase):
    def test_gui_subcommand_registered(self):
        parser = dabbak.build_parser()
        # Should parse without raising.
        ns = parser.parse_args(["gui"])
        self.assertEqual(ns.cmd, "gui")


if __name__ == "__main__":
    unittest.main()
