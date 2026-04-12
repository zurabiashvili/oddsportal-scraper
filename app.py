"""
OddsPortal Scraper - GUI Application

Configure: URL, Market (FT/HT), Line (0.5/1.5/2.5), Match limit, Direction.
"""

import sys
import threading
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, font as tkfont

from scraper_config import ScraperConfig


def run_scraper(config: ScraperConfig, log_cb=None, progress_cb=None):
    """Run the scraper. log_cb(msg) for log lines. progress_cb(percent, message, eta_seconds) for progress."""
    import asyncio
    from scraper import main

    class LogCapture:
        def __init__(self, cb):
            self.cb = cb
            self.buf = ""

        def write(self, s):
            self.buf += s
            while "\n" in self.buf:
                line, self.buf = self.buf.split("\n", 1)
                if self.cb:
                    self.cb(line.rstrip())

        def flush(self):
            pass

    if log_cb:
        old_stdout = sys.stdout
        sys.stdout = LogCapture(log_cb)

    try:
        asyncio.run(main(run_config=config, progress_cb=progress_cb))
    finally:
        if log_cb:
            sys.stdout = old_stdout


def main_gui():
    root = tk.Tk()
    root.title("OddsPortal Scraper")
    root.minsize(520, 580)
    root.resizable(True, True)

    # Variables
    url_text_default = "https://www.oddsportal.com/football/england/league-one/results/"
    market_var = tk.StringVar(value="ft")
    line_var = tk.DoubleVar(value=1.5)
    limit_var = tk.StringVar(value="full")
    limit_num_var = tk.StringVar(value="100")
    direction_var = tk.StringVar(value="newest")
    fresh_run_var = tk.BooleanVar(value=False)  # Default: resume; check box for full reset
    running_var = tk.BooleanVar(value=False)

    def on_start():
        urls_text = url_text.get("1.0", tk.END)
        urls = [u.strip() for u in urls_text.splitlines() if u.strip() and "oddsportal.com" in u][:12]
        if not urls:
            messagebox.showerror("Error", "Please enter at least one valid OddsPortal league URL (up to 12).")
            return
        urls = [u if u.endswith("/") else u + "/" for u in urls]
        urls = [u if "/results" in u else u.rstrip("/") + "/results/" for u in urls]

        market = "ft" if market_var.get() == "ft" else "ht"
        line_val = line_var.get()
        if line_val not in (0.5, 1.5, 2.5):
            line_val = 1.5
        limit = None
        if limit_var.get() == "first":
            try:
                limit = int(limit_num_var.get())
                if limit < 1:
                    limit = 10
            except ValueError:
                limit = 100
        direction = "oldest" if direction_var.get() == "oldest" else "newest"
        fresh_run = fresh_run_var.get()

        config = ScraperConfig(
            league_urls=urls,
            market=market,
            line=line_val,
            match_limit=limit,
            direction=direction,
            fresh_run=fresh_run,
        )

        running_var.set(True)
        btn_start.config(state="disabled")
        btn_stop.config(state="normal")
        log_area.delete("1.0", tk.END)
        log_area.insert(tk.END, "Starting scraper...\n")
        progress_bar["value"] = 0
        status_label.config(text="Initializing...")
        eta_label.config(text="")

        def run():
            def log(msg):
                root.after(0, lambda m=msg: _append_log(m))

            def progress(percent, message, eta_seconds):
                def update():
                    if percent is not None:
                        try:
                            progress_bar.stop()
                        except Exception:
                            pass
                        progress_bar.config(mode="determinate", maximum=100)
                        progress_bar["value"] = percent
                    else:
                        progress_bar.config(mode="indeterminate")
                        progress_bar.start(8)
                    status_label.config(text=message or "Working...")
                    if eta_seconds is not None and eta_seconds > 0:
                        mins = int(eta_seconds // 60)
                        secs = int(eta_seconds % 60)
                        eta_label.config(text=f"Est. time left: {mins}m {secs}s")
                    else:
                        eta_label.config(text="")
                root.after(0, update)

            def _append_log(msg):
                log_area.insert(tk.END, msg + "\n")
                log_area.see(tk.END)

            try:
                run_scraper(config, log_cb=log, progress_cb=progress)
            except Exception as e:
                root.after(0, lambda: _append_log(f"Error: {e}"))
            finally:
                def _finish():
                    try:
                        progress_bar.stop()
                    except Exception:
                        pass
                    progress_bar.config(value=100, mode="determinate")
                    status_label.config(text="Done.")
                    eta_label.config(text="")
                root.after(0, _finish)
                root.after(0, lambda: running_var.set(False))
                root.after(0, lambda: btn_start.config(state="normal"))
                root.after(0, lambda: btn_stop.config(state="disabled"))
                root.after(0, lambda: _append_log("Done."))

        threading.Thread(target=run, daemon=True).start()

    def on_stop():
        from scraper import request_stop
        request_stop()
        log_area.insert(tk.END, "Stop requested. Finishing current task...\n")
        log_area.see(tk.END)

    # Layout
    pad = {"padx": 10, "pady": 6}
    ttk.Label(root, text="League URLs - queue up to 12 (one per line)", font=("", 10, "bold")).pack(anchor="w", **pad)
    url_text = scrolledtext.ScrolledText(root, height=7, width=70, wrap=tk.WORD, font=("Consolas", 9))
    url_text.pack(fill="x", **pad)
    url_text.insert("1.0", url_text_default)

    f1 = ttk.Frame(root)
    f1.pack(fill="x", **pad)
    ttk.Label(f1, text="Market:").pack(side="left", padx=(0, 20))
    ttk.Radiobutton(f1, text="Full Time", variable=market_var, value="ft").pack(side="left", padx=5)
    ttk.Radiobutton(f1, text="1st Half (Half Time)", variable=market_var, value="ht").pack(side="left", padx=5)
    ttk.Label(f1, text="  Line:").pack(side="left", padx=(20, 5))
    for v in (0.5, 1.5, 2.5):
        ttk.Radiobutton(f1, text=str(v), variable=line_var, value=v).pack(side="left", padx=3)

    f2 = ttk.Frame(root)
    f2.pack(fill="x", **pad)
    ttk.Label(f2, text="Number of matches:").pack(side="left", padx=(0, 15))
    ttk.Radiobutton(f2, text="Full season", variable=limit_var, value="full").pack(side="left", padx=5)
    ttk.Radiobutton(f2, text="Limit to", variable=limit_var, value="first").pack(side="left", padx=5)
    limit_entry = ttk.Entry(f2, textvariable=limit_num_var, width=6)
    limit_entry.pack(side="left", padx=2)
    ttk.Label(f2, text="matches").pack(side="left", padx=2)
    ttk.Label(
        f2,
        text="(counts existing CSV rows — enable Fresh run to replace with a new top-N)",
        font=("", 8),
        foreground="gray",
    ).pack(side="left", padx=(8, 0))

    f3 = ttk.Frame(root)
    f3.pack(fill="x", **pad)
    ttk.Label(f3, text="Start from:").pack(side="left", padx=(0, 15))
    ttk.Radiobutton(f3, text="Newest first (most recent matches)", variable=direction_var, value="newest").pack(side="left", padx=5)
    ttk.Radiobutton(f3, text="Oldest first (first matches of season)", variable=direction_var, value="oldest").pack(side="left", padx=5)

    ttk.Label(root, text="↑ Newest = end of season. Oldest = start of season.", font=("", 8), foreground="gray").pack(anchor="w", padx=10)

    fresh_cb = ttk.Checkbutton(
        root,
        text="Fresh run (clears this league’s CSV and starts over — use with care)",
        variable=fresh_run_var,
    )
    fresh_cb.pack(anchor="w", padx=10, pady=4)

    btn_frame = ttk.Frame(root)
    btn_frame.pack(fill="x", pady=10)
    btn_start = ttk.Button(btn_frame, text="Start", command=on_start)
    btn_start.pack(side="left", padx=10)
    btn_stop = ttk.Button(btn_frame, text="Stop", command=on_stop, state="disabled")
    btn_stop.pack(side="left", padx=10)

    ttk.Separator(root, orient="horizontal").pack(fill="x", pady=5)
    progress_frame = ttk.Frame(root)
    progress_frame.pack(fill="x", **pad)
    status_label = ttk.Label(progress_frame, text="Idle", font=("", 9))
    status_label.pack(anchor="w")
    progress_bar = ttk.Progressbar(progress_frame, length=400, mode="determinate", maximum=100)
    progress_bar.pack(fill="x", pady=4)
    eta_label = ttk.Label(progress_frame, text="", font=("", 9), foreground="gray")
    eta_label.pack(anchor="w")
    ttk.Label(root, text="Log", font=("", 10, "bold")).pack(anchor="w", **pad)
    log_frame = ttk.Frame(root)
    log_frame.pack(fill="both", expand=True, **pad)
    log_area = scrolledtext.ScrolledText(log_frame, height=14, width=70, wrap=tk.WORD, font=("Consolas", 9))
    log_area.pack(fill="both", expand=True)

    root.mainloop()


if __name__ == "__main__":
    main_gui()
