"""Tkinter GUI for dabbak.

Three tabs:
  - Backup   : show config summary, run backup (or dry-run), stream output
  - Restore  : pick a snapshot, search filenames, restore selected files
  - Settings : edit sources / excludes / destinations / state path

Long-running operations (backup, search) run on a worker thread and
communicate back via a queue polled by Tk's `after()`. Stdout is
redirected to that queue so per-file `++ ** --` lines stream live into
the Output pane.

Stdlib-only: depends on `dabbak` (this repo) and `tkinter`.
"""
import contextlib
import datetime
import fnmatch
import io
import json
import os
import queue
import sys
import threading

import dabbak


# tkinter is stdlib but some minimal Python installs (or headless CI
# environments) omit the bindings. Defer hard failures to main() so that
# importing this module for unit tests of its non-GUI helpers still works.
try:
    import tkinter as tk
    from tkinter import ttk, filedialog, messagebox, scrolledtext
    _TK_AVAILABLE = True
    _BaseFrame = ttk.Frame
    _TK_IMPORT_ERROR = None
except Exception as _e:  # ImportError on POSIX without python3-tk
    _TK_AVAILABLE = False
    _TK_IMPORT_ERROR = _e
    tk = ttk = filedialog = messagebox = scrolledtext = None

    class _BaseFrame:
        """Stub so tab classes are still definable on headless systems."""
        def __init__(self, *a, **kw):
            raise RuntimeError(
                "tkinter is not available; install your distro's "
                "python3-tk package"
            )


# ---------------------------------------------------------------------------
# non-GUI helpers (kept module-level so they're easily unit-testable)
# ---------------------------------------------------------------------------

def search_paths(paths, pattern):
    """Filter `paths` by `pattern`.

    - Empty pattern: returns everything.
    - Pattern containing glob chars (*, ?, [): fnmatch case-insensitive
      against the full path.
    - Otherwise: case-insensitive substring search.
    """
    if not pattern:
        return list(paths)
    if any(c in pattern for c in "*?["):
        pat = pattern.lower()
        return [p for p in paths if fnmatch.fnmatchcase(p.lower(), pat)]
    needle = pattern.lower()
    return [p for p in paths if needle in p.lower()]


def snapshot_manifest_paths(partial_dir, snapshot_date):
    """Return the sorted list of source paths recorded in a snapshot's
    `__state.json`. Empty list if the snapshot doesn't exist or has no
    manifest (which is the case for incomplete snapshots).
    """
    manifest = os.path.join(partial_dir, snapshot_date, "__state.json")
    if not dabbak.fs_exists(manifest):
        return []
    state = dabbak.read_full_state_file(manifest)
    return sorted(state.keys())


def save_config_atomic(path, config):
    """Atomic config write: tmp + fsync + os.replace, like the state file."""
    tmp = path + ".tmp"
    with dabbak.fs_open(tmp, "w", encoding="utf8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(dabbak._long(tmp), dabbak._long(path))


# ---------------------------------------------------------------------------
# stdout redirection for streaming worker output into the GUI
# ---------------------------------------------------------------------------

class QueueIO(io.TextIOBase):
    """File-like sink that pushes complete lines into a queue.

    Treats both \\n and \\r as line terminators so the Progress class's
    overwrite-with-\\r updates each show up as their own line in the GUI
    log (cleaner than buffering them into one ever-growing line).
    """

    def __init__(self, q, tag="out"):
        self.q = q
        self.tag = tag
        self._buf = ""

    def writable(self):
        return True

    def write(self, s):
        s = s.replace("\r", "\n")
        self._buf += s
        while "\n" in self._buf:
            line, _, self._buf = self._buf.partition("\n")
            self.q.put((self.tag, line))
        return len(s)

    def flush(self):
        if self._buf:
            self.q.put((self.tag, self._buf))
            self._buf = ""


# ---------------------------------------------------------------------------
# GUI
# ---------------------------------------------------------------------------

class BackupTab(_BaseFrame):
    def __init__(self, master, app):
        super().__init__(master)
        self.app = app
        self.queue = queue.Queue()
        self.worker = None
        self._build()
        self.refresh()

    def _build(self):
        top = ttk.LabelFrame(self, text="Configuration")
        top.pack(fill="x", padx=8, pady=8)
        self.summary = tk.Text(top, height=6, wrap="none")
        self.summary.pack(fill="x", padx=4, pady=4)
        self.summary.config(state="disabled")

        btns = ttk.Frame(self)
        btns.pack(fill="x", padx=8)
        self.run_btn = ttk.Button(
            btns, text="Run Backup", command=lambda: self.run_backup(False)
        )
        self.run_btn.pack(side="left", padx=4)
        self.dry_btn = ttk.Button(
            btns, text="Dry Run", command=lambda: self.run_backup(True)
        )
        self.dry_btn.pack(side="left", padx=4)
        self.clear_btn = ttk.Button(
            btns, text="Clear Output", command=self.clear_log
        )
        self.clear_btn.pack(side="left", padx=4)

        # "Set and forget" mode for cron-like manual runs. The window
        # closes only on a clean run (completed, no copy failures, no
        # logged errors) so any problem keeps it open for inspection.
        # Dry-runs never close — those are exploratory by nature.
        self.close_on_success_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            btns,
            text="Close window after successful backup",
            variable=self.close_on_success_var,
        ).pack(side="right", padx=4)

        log_frame = ttk.LabelFrame(self, text="Output")
        log_frame.pack(fill="both", expand=True, padx=8, pady=8)
        self.log = scrolledtext.ScrolledText(
            log_frame, wrap="none", height=20
        )
        self.log.pack(fill="both", expand=True)
        self.log.config(state="disabled")

    def refresh(self):
        self.summary.config(state="normal")
        self.summary.delete("1.0", "end")
        c = self.app.config
        if c is None:
            self.summary.insert(
                "end",
                "No config loaded. Open the Settings tab to create one.\n",
            )
        else:
            self.summary.insert("end", "Sources:\n")
            for s in c.get("source", {}).get("directories", []) or ["(none)"]:
                self.summary.insert("end", f"  {s}\n")
            d = c.get("destination", {})
            self.summary.insert(
                "end", f"Mirror:    {d.get('directory_full', '')}\n"
            )
            self.summary.insert(
                "end", f"Snapshots: {d.get('directory_partial', '')}\n"
            )
        self.summary.config(state="disabled")

    def clear_log(self):
        self.log.config(state="normal")
        self.log.delete("1.0", "end")
        self.log.config(state="disabled")

    def append_log(self, text):
        self.log.config(state="normal")
        self.log.insert("end", text + "\n")
        self.log.see("end")
        self.log.config(state="disabled")

    def run_backup(self, dry_run):
        if self.app.config is None:
            messagebox.showwarning(
                "No config", "Configure dabbak in the Settings tab first."
            )
            return
        if self.worker and self.worker.is_alive():
            messagebox.showinfo(
                "Running", "A backup is already in progress."
            )
            return
        self.run_btn.config(state="disabled")
        self.dry_btn.config(state="disabled")
        self.append_log(
            f"--- {'dry-run ' if dry_run else ''}"
            f"{datetime.datetime.now().isoformat()} ---"
        )
        self.worker = threading.Thread(
            target=self._run_worker,
            args=(self.app.config, dry_run),
            daemon=True,
        )
        self.worker.start()
        self.after(100, self._poll)

    def _run_worker(self, config, dry_run):
        out = QueueIO(self.queue, "out")
        clean = False
        try:
            with contextlib.redirect_stdout(out):
                try:
                    with dabbak.FileLock(dabbak.lock_path_for(config)):
                        stats = dabbak.make_backup(
                            config, dry_run=dry_run
                        )
                    clean = (
                        bool(stats.get("completed"))
                        and stats.get("failed", 0) == 0
                        and stats.get("error_count", 0) == 0
                    )
                except dabbak.LockHeld as e:
                    print(f"ERROR: {e}")
        except Exception as e:
            self.queue.put(("err", f"ERROR: {e}"))
        finally:
            out.flush()
            # Carry result through to the GUI thread. Tuple shape:
            # (clean_success_bool, dry_run_bool).
            self.queue.put(("done", (clean, dry_run)))

    def _poll(self):
        try:
            while True:
                tag, msg = self.queue.get_nowait()
                if tag == "done":
                    self.run_btn.config(state="normal")
                    self.dry_btn.config(state="normal")
                    self.append_log("--- done ---")
                    clean, dry_run = msg
                    if (
                        clean
                        and not dry_run
                        and self.close_on_success_var.get()
                    ):
                        # Brief delay so the user sees the "--- done ---"
                        # line; gives a chance to cancel by reading first
                        # but is short enough not to feel laggy.
                        self.after(800, self.app.root.destroy)
                    return
                if msg is not None:
                    self.append_log(msg)
        except queue.Empty:
            pass
        self.after(100, self._poll)


class RestoreTab(_BaseFrame):
    _RESULT_CAP = 5000

    def __init__(self, master, app):
        super().__init__(master)
        self.app = app
        self._build()
        self.refresh()

    def _build(self):
        top = ttk.Frame(self)
        top.pack(fill="x", padx=8, pady=8)

        ttk.Label(top, text="Snapshot:").pack(side="left")
        self.date_var = tk.StringVar()
        self.date_combo = ttk.Combobox(
            top, textvariable=self.date_var, state="readonly", width=14,
        )
        self.date_combo.pack(side="left", padx=4)
        self.date_combo.bind(
            "<<ComboboxSelected>>", lambda e: self.do_search()
        )

        ttk.Label(top, text="Search:").pack(side="left", padx=(16, 4))
        self.pattern_var = tk.StringVar()
        self.pattern_entry = ttk.Entry(
            top, textvariable=self.pattern_var, width=40
        )
        self.pattern_entry.pack(side="left", padx=4)
        self.pattern_entry.bind("<Return>", lambda e: self.do_search())
        ttk.Button(top, text="Search", command=self.do_search).pack(
            side="left", padx=4
        )

        ttk.Label(
            self,
            text="Results (Ctrl/Shift+click to select multiple). "
                 "Glob chars (*, ?, [..]) trigger glob matching; "
                 "otherwise it's a case-insensitive substring search.",
        ).pack(anchor="w", padx=8)

        list_frame = ttk.Frame(self)
        list_frame.pack(fill="both", expand=True, padx=8, pady=4)
        self.listbox = tk.Listbox(list_frame, selectmode="extended")
        sb = ttk.Scrollbar(
            list_frame, orient="vertical", command=self.listbox.yview
        )
        self.listbox.config(yscrollcommand=sb.set)
        sb.pack(side="right", fill="y")
        self.listbox.pack(side="left", fill="both", expand=True)

        bottom = ttk.Frame(self)
        bottom.pack(fill="x", padx=8, pady=8)
        ttk.Button(
            bottom, text="Restore Selected...", command=self.do_restore
        ).pack(side="left")
        ttk.Button(
            bottom, text="Select All",
            command=lambda: self.listbox.select_set(0, "end"),
        ).pack(side="left", padx=4)
        self.status = ttk.Label(bottom, text="")
        self.status.pack(side="left", padx=12)

    def refresh(self):
        if self.app.config is None:
            self.date_combo["values"] = []
            self.listbox.delete(0, "end")
            return
        partial_dir = self.app.config["destination"]["directory_partial"]
        try:
            snaps = dabbak.enumerate_snapshots(
                os.path.normpath(partial_dir)
            )
        except Exception:
            snaps = []
        valid = [s["date"] for s in snaps if not s["incomplete"]]
        self.date_combo["values"] = valid
        if valid and self.date_var.get() not in valid:
            self.date_var.set(valid[0])

    def do_search(self):
        self.listbox.delete(0, "end")
        if self.app.config is None or not self.date_var.get():
            self.status.config(text="(no snapshot selected)")
            return
        partial_dir = self.app.config["destination"]["directory_partial"]
        paths = snapshot_manifest_paths(
            os.path.normpath(partial_dir), self.date_var.get()
        )
        results = search_paths(paths, self.pattern_var.get())
        for p in results[: self._RESULT_CAP]:
            self.listbox.insert("end", p)
        if len(results) > self._RESULT_CAP:
            self.listbox.insert(
                "end",
                f"... and {len(results) - self._RESULT_CAP} more "
                f"(refine search)",
            )
        self.status.config(text=f"{len(results)} result(s)")

    def do_restore(self):
        sel = self.listbox.curselection()
        if not sel:
            messagebox.showinfo(
                "Nothing selected",
                "Select one or more files in the list, then click Restore.",
            )
            return
        paths = [self.listbox.get(i) for i in sel]
        paths = [p for p in paths if not p.startswith("... and ")]
        if not paths:
            return
        dest = filedialog.askdirectory(title="Restore into directory")
        if not dest:
            return
        try:
            dabbak.restore(
                self.app.config, dest, self.date_var.get(),
                patterns=paths, force=True,
            )
        except SystemExit as e:
            messagebox.showerror(
                "Restore",
                f"Restore aborted (exit code {e.code}). "
                f"See terminal output if any.",
            )
            return
        except Exception as e:
            messagebox.showerror("Restore", f"Restore failed: {e}")
            return
        messagebox.showinfo(
            "Restore", f"Restored {len(paths)} file(s) to:\n{dest}"
        )


class SettingsTab(_BaseFrame):
    def __init__(self, master, app):
        super().__init__(master)
        self.app = app
        self._build()
        self.refresh()

    def _build(self):
        # Sources
        f1 = ttk.LabelFrame(self, text="Source directories")
        f1.pack(fill="x", padx=8, pady=8)
        self.sources = tk.Listbox(f1, height=5)
        self.sources.pack(
            side="left", fill="both", expand=True, padx=4, pady=4
        )
        bs = ttk.Frame(f1)
        bs.pack(side="right", padx=4)
        ttk.Button(bs, text="Add...", command=self.add_source).pack(fill="x")
        ttk.Button(bs, text="Remove", command=self.remove_source).pack(fill="x")

        # Excludes
        f2 = ttk.LabelFrame(
            self,
            text="Excludes (e.g. *.pyc, __pycache__, /absolute/path)",
        )
        f2.pack(fill="x", padx=8, pady=8)
        self.excludes = tk.Listbox(f2, height=5)
        self.excludes.pack(
            side="left", fill="both", expand=True, padx=4, pady=4
        )
        be = ttk.Frame(f2)
        be.pack(side="right", padx=4)
        ttk.Button(be, text="Add...", command=self.add_exclude).pack(fill="x")
        ttk.Button(be, text="Remove", command=self.remove_exclude).pack(fill="x")

        # Destinations
        f3 = ttk.LabelFrame(self, text="Destinations")
        f3.pack(fill="x", padx=8, pady=8)
        self.full_var = tk.StringVar()
        self.partial_var = tk.StringVar()
        self.state_var = tk.StringVar()
        for label, var, picker in [
            (
                "Mirror (directory_full):",
                self.full_var,
                lambda: self._pick_dir(self.full_var),
            ),
            (
                "Snapshots (directory_partial):",
                self.partial_var,
                lambda: self._pick_dir(self.partial_var),
            ),
            (
                "State file:",
                self.state_var,
                lambda: self._pick_file(self.state_var),
            ),
        ]:
            row = ttk.Frame(f3)
            row.pack(fill="x", padx=4, pady=2)
            ttk.Label(row, text=label, width=32).pack(side="left")
            ttk.Entry(row, textvariable=var).pack(
                side="left", fill="x", expand=True, padx=4
            )
            ttk.Button(row, text="Browse...", command=picker).pack(side="left")

        # Save / reload
        bb = ttk.Frame(self)
        bb.pack(fill="x", padx=8, pady=8)
        ttk.Button(bb, text="Save", command=self.save_config).pack(
            side="left", padx=4
        )
        ttk.Button(
            bb, text="Reload from disk", command=self.reload_from_disk
        ).pack(side="left", padx=4)
        self.status = ttk.Label(bb, text="")
        self.status.pack(side="left", padx=12)

    def refresh(self):
        c = self.app.config or {}
        src = c.get("source", {})
        self.sources.delete(0, "end")
        for s in src.get("directories", []):
            self.sources.insert("end", s)
        self.excludes.delete(0, "end")
        for e in src.get("excludes", []):
            self.excludes.insert("end", e)
        d = c.get("destination", {})
        self.full_var.set(d.get("directory_full", ""))
        self.partial_var.set(d.get("directory_partial", ""))
        self.state_var.set(c.get("full_state_file", ""))

    def reload_from_disk(self):
        self.app.config = self.app._try_load_config()
        self.refresh()
        self.app.backup_tab.refresh()
        self.app.restore_tab.refresh()
        self.status.config(text="Reloaded.")

    def _pick_dir(self, var):
        path = filedialog.askdirectory(initialdir=var.get() or None)
        if path:
            var.set(path)

    def _pick_file(self, var):
        path = filedialog.asksaveasfilename(
            initialdir=os.path.dirname(var.get() or "") or None,
            initialfile=os.path.basename(var.get() or "state.json"),
        )
        if path:
            var.set(path)

    def add_source(self):
        path = filedialog.askdirectory(title="Add source directory")
        if path:
            self.sources.insert("end", path)

    def remove_source(self):
        for i in reversed(self.sources.curselection()):
            self.sources.delete(i)

    def add_exclude(self):
        dlg = tk.Toplevel(self)
        dlg.title("Add exclude pattern")
        dlg.geometry("460x130")
        ttk.Label(
            dlg,
            text="Pattern (no slash = basename anywhere, "
                 "slash = full-path / absolute):",
            wraplength=440,
        ).pack(padx=8, pady=(8, 4))
        var = tk.StringVar()
        entry = ttk.Entry(dlg, textvariable=var, width=50)
        entry.pack(padx=8, pady=4)
        entry.focus()

        def ok():
            v = var.get().strip()
            if v:
                self.excludes.insert("end", v)
            dlg.destroy()

        entry.bind("<Return>", lambda e: ok())
        ttk.Button(dlg, text="OK", command=ok).pack(pady=4)

    def remove_exclude(self):
        for i in reversed(self.excludes.curselection()):
            self.excludes.delete(i)

    def save_config(self):
        existing = self.app.config or {}
        new = {
            "source": {
                "directories": list(self.sources.get(0, "end")),
                "excludes": list(self.excludes.get(0, "end")),
            },
            "destination": {
                "directory_full": self.full_var.get(),
                "directory_partial": self.partial_var.get(),
            },
            "full_state_file": self.state_var.get(),
            "packaging_state_file": existing.get(
                "packaging_state_file", "packaging-state.json"
            ),
        }
        if "is-windows" in existing.get("source", {}):
            new["source"]["is-windows"] = existing["source"]["is-windows"]
        try:
            save_config_atomic(self.app.config_path, new)
        except Exception as e:
            messagebox.showerror("Save", f"Save failed: {e}")
            return
        self.status.config(text=f"Saved to {self.app.config_path}")
        self.app.config = new
        self.app.backup_tab.refresh()
        self.app.restore_tab.refresh()


class DabbakApp:
    def __init__(self, root):
        self.root = root
        root.title("dabbak")
        root.geometry("960x640")

        self.config_path = os.path.join(
            dabbak.base_dir(),
            os.environ.get("DABBAK_CONFIG", "backup-config.json"),
        )
        self.config = self._try_load_config()

        self.nb = ttk.Notebook(root)
        self.nb.pack(fill="both", expand=True)

        self.backup_tab = BackupTab(self.nb, self)
        self.restore_tab = RestoreTab(self.nb, self)
        self.settings_tab = SettingsTab(self.nb, self)

        self.nb.add(self.backup_tab, text="Backup")
        self.nb.add(self.restore_tab, text="Restore")
        self.nb.add(self.settings_tab, text="Settings")

    def _try_load_config(self):
        if not dabbak.fs_exists(self.config_path):
            return None
        try:
            with dabbak.fs_open(self.config_path, encoding="utf8") as f:
                return json.load(f)
        except Exception as e:
            messagebox.showerror(
                "Config",
                f"Failed to load {self.config_path}: {e}\n\n"
                "Use the Settings tab to fix or recreate it.",
            )
            return None


def main():
    if not _TK_AVAILABLE:
        sys.stderr.write(
            f"Could not import tkinter: {_TK_IMPORT_ERROR}\n"
            "On Linux, install your distro's python3-tk package "
            "(e.g. `apt install python3-tk`).\n"
        )
        sys.exit(1)
    try:
        root = tk.Tk()
    except Exception as e:
        sys.stderr.write(
            f"Could not open a display: {e}\n"
            "The GUI needs a graphical environment "
            "(DISPLAY on Linux, an interactive desktop session "
            "on Windows/macOS).\n"
        )
        sys.exit(1)
    DabbakApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
