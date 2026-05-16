#!/usr/bin/env python3
"""
manga_server.py  –  Manga Scraper Control Panel
================================================
A single-file Flask web server that lets you:
  • Start / stop FlareSolverr
  • Scrape a manga URL (auto-starts FlareSolverr, auto-stops after)
  • Optimize + pack CBZ in the background
  • Download the final -cbz folder as a zip
  • Delete manga / output folders
  • Read chapters directly in the browser (no download needed)

INSTALL
  pip install flask

RUN
  python3 manga_server.py
  Then open http://<your-server-ip>:8080 in your browser.

CONFIG  (edit the CONFIG block below)
  BASE_DIR          – where manga folders live  (e.g. ~/Manga-Scraper/manga)
  SCRAPER_PATH      – path to manga_scraper.py
  OPTIMIZER_PATH    – path to manga_optimize.sh
  FLARE_CMD         – command to start FlareSolverr
  PORT              – web server port (default 8080)
"""

from __future__ import annotations

import os
import re
import sys
import json
import time
import shutil
import signal
import subprocess
import threading
import zipfile
import tempfile
from datetime import datetime
from pathlib import Path
from collections import deque
from decimal import Decimal, InvalidOperation

from flask import (
    Flask,
    Response,
    jsonify,
    render_template_string,
    request,
    send_file,
    abort,
)

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIG  –  edit these paths to match your setup
# ─────────────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.expanduser("~/Manga-Scraper/manga")
SCRAPER_PATH = os.path.expanduser("~/Manga-Scraper/manga-scraper.py")
OPTIMIZER_PATH = os.path.expanduser("~/Manga-Scraper/manga-optimizer.sh")
FLARE_CMD = [
    "python3",
    os.path.expanduser("~/Manga-Scraper/FlareSolverr/src/flaresolverr.py"),
]
PORT = 8080
LOG_LINES = 500  # max lines kept in memory per job
# ─────────────────────────────────────────────────────────────────────────────

app = Flask(__name__)

# ── Global state ──────────────────────────────────────────────────────────────
_lock = threading.Lock()
_flare_proc: subprocess.Popen | None = None
_jobs: dict[str, dict] = {}  # job_id → {type, status, log, proc, thread}


def _jid() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S_") + str(
        int(time.time() * 1000) % 10000
    )


def _new_job(kind: str) -> str:
    jid = _jid()
    _jobs[jid] = {
        "id": jid,
        "type": kind,
        "status": "running",  # running | done | failed
        "started": datetime.now().strftime("%H:%M:%S"),
        "ended": None,
        "log": deque(maxlen=LOG_LINES),
        "proc": None,
        "thread": None,
    }
    return jid


def _append(jid: str, line: str):
    ts = datetime.now().strftime("%H:%M:%S")
    _jobs[jid]["log"].append(f"[{ts}] {line.rstrip()}")


def _finish(jid: str, ok: bool):
    _jobs[jid]["status"] = "done" if ok else "failed"
    _jobs[jid]["ended"] = datetime.now().strftime("%H:%M:%S")


# ─────────────────────────────────────────────────────────────────────────────
#  FlareSolverr management
# ─────────────────────────────────────────────────────────────────────────────


def flare_running() -> bool:
    global _flare_proc
    return _flare_proc is not None and _flare_proc.poll() is None


def flare_start() -> bool:
    global _flare_proc
    if flare_running():
        return True
    try:
        _flare_proc = subprocess.Popen(
            FLARE_CMD,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            preexec_fn=os.setsid,
        )
        time.sleep(2)  # brief wait for it to bind
        return flare_running()
    except Exception as e:
        print(f"[flare_start] {e}")
        return False


def flare_stop():
    global _flare_proc
    if _flare_proc and _flare_proc.poll() is None:
        try:
            os.killpg(os.getpgid(_flare_proc.pid), signal.SIGTERM)
        except Exception:
            _flare_proc.terminate()
        _flare_proc = None


# ─────────────────────────────────────────────────────────────────────────────
#  Job runner  (generic streaming subprocess)
# ─────────────────────────────────────────────────────────────────────────────


def _run_process(jid: str, cmd: list[str], cwd: str | None = None, on_done=None):
    """Runs cmd in a thread, streams stdout/stderr into job log."""

    def _worker():
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                cwd=cwd,
            )
            _jobs[jid]["proc"] = proc
            for line in proc.stdout:
                _append(jid, line)
            proc.wait()
            ok = proc.returncode == 0
            _finish(jid, ok)
        except Exception as e:
            _append(jid, f"ERROR: {e}")
            _finish(jid, False)
        finally:
            if on_done:
                on_done(jid)

    t = threading.Thread(target=_worker, daemon=True)
    _jobs[jid]["thread"] = t
    t.start()


# ─────────────────────────────────────────────────────────────────────────────
#  Scrape job  (starts flare → scrape → stops flare)
# ─────────────────────────────────────────────────────────────────────────────


def _scrape_worker(jid: str, url: str, extra_args: list[str]):
    # 1. Start FlareSolverr
    _append(jid, "▶ Starting FlareSolverr …")
    if not flare_start():
        _append(jid, "✘ Could not start FlareSolverr")
        _finish(jid, False)
        return
    _append(jid, "✔ FlareSolverr is up")

    # 2. Run scraper
    cmd = ["python3", SCRAPER_PATH, url] + extra_args
    _append(jid, f"▶ Scraper: {' '.join(cmd)}")

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        _jobs[jid]["proc"] = proc
        for line in proc.stdout:
            _append(jid, line)
        proc.wait()
        ok = proc.returncode == 0
    except Exception as e:
        _append(jid, f"✘ Scraper error: {e}")
        ok = False

    # 3. Stop FlareSolverr
    _append(jid, "▶ Stopping FlareSolverr …")
    flare_stop()
    _append(jid, "✔ FlareSolverr stopped")

    _finish(jid, ok)


def start_scrape(url: str, extra_args: list[str]) -> str:
    jid = _new_job("scrape")
    t = threading.Thread(
        target=_scrape_worker,
        args=(jid, url, extra_args),
        daemon=True,
    )
    _jobs[jid]["thread"] = t
    t.start()
    return jid


# ─────────────────────────────────────────────────────────────────────────────
#  Optimize job
# ─────────────────────────────────────────────────────────────────────────────


def start_optimize(manga_dir: str, flags: list[str]) -> str:
    jid = _new_job("optimize")
    cmd = ["bash", OPTIMIZER_PATH] + flags + [manga_dir]
    _run_process(jid, cmd)
    return jid


# ─────────────────────────────────────────────────────────────────────────────
#  Manga directory listing helpers
# ─────────────────────────────────────────────────────────────────────────────

# Suffixes that mark derived/output dirs — excluded from the main manga list
_DERIVED_SUFFIXES = ("-cbz", "-optimized")


def list_manga() -> list[dict]:
    base = Path(BASE_DIR)
    if not base.is_dir():
        return []
    results = []
    for d in sorted(base.iterdir()):
        if not d.is_dir():
            continue
        # skip derived output directories
        if any(d.name.endswith(s) for s in _DERIVED_SUFFIXES):
            continue
        chapters = sum(1 for c in d.iterdir() if c.is_dir())
        has_cbz = base.joinpath(d.name + "-cbz").is_dir()
        has_opt = base.joinpath(d.name + "-optimized").is_dir()
        results.append(
            {
                "name": d.name,
                "path": str(d),
                "chapters": chapters,
                "has_cbz": has_cbz,
                "has_opt": has_opt,
            }
        )
    return results


def list_chapters(manga_name: str) -> list[str]:
    """Return sorted chapter folder names for a manga (prefers optimized if present)."""
    base = Path(BASE_DIR)
    # prefer optimized dir for reading (WebP = smaller/faster)
    for suffix in ("-optimized", ""):
        target = base / (manga_name + suffix)
        if target.is_dir():
            dirs = sorted(
                [c.name for c in target.iterdir() if c.is_dir()],
                key=lambda x: [
                    int(t) if t.isdigit() else t for t in re.split(r"(\d+)", x)
                ],
            )
            if dirs:
                return dirs
    return []


def list_chapter_images(manga_name: str, chapter: str) -> list[str]:
    """Return sorted image filenames for a chapter."""
    base = Path(BASE_DIR)
    for suffix in ("-optimized", ""):
        ch_dir = base / (manga_name + suffix) / chapter
        if ch_dir.is_dir():
            imgs = sorted(
                [
                    f.name
                    for f in ch_dir.iterdir()
                    if f.suffix.lower() in (".jpg", ".jpeg", ".png", ".webp")
                ],
                key=lambda x: [
                    int(t) if t.isdigit() else t for t in re.split(r"(\d+)", x)
                ],
            )
            if imgs:
                return imgs
    return []


def _resolve_chapter_dir(manga_name: str, chapter: str) -> Path | None:
    """Find the actual directory for a chapter (optimized or original)."""
    base = Path(BASE_DIR)
    for suffix in ("-optimized", ""):
        p = base / (manga_name + suffix) / chapter
        if p.is_dir():
            return p
    return None


# ─────────────────────────────────────────────────────────────────────────────
#  Download: zip up a -cbz folder on the fly
# ─────────────────────────────────────────────────────────────────────────────


def zip_cbz_dir(cbz_dir: Path) -> Path:
    """Creates a temp zip of the -cbz directory. Caller must delete it."""
    tmp = tempfile.mktemp(suffix=".zip")
    with zipfile.ZipFile(tmp, "w", compression=zipfile.ZIP_STORED) as zf:
        for f in sorted(cbz_dir.iterdir()):
            if f.is_file():
                zf.write(f, f.name)
    return Path(tmp)


# ─────────────────────────────────────────────────────────────────────────────
#  Path safety helper  (prevents directory traversal)
# ─────────────────────────────────────────────────────────────────────────────


def _safe_subpath(base: Path, *parts: str) -> Path | None:
    """
    Resolves a path under base from untrusted parts.
    Returns None if the result escapes base (traversal attempt).
    """
    try:
        candidate = (base / Path(*parts)).resolve()
        base_resolved = base.resolve()
        candidate.relative_to(base_resolved)  # raises ValueError if outside
        return candidate
    except (ValueError, Exception):
        return None


# ─────────────────────────────────────────────────────────────────────────────
#  API routes
# ─────────────────────────────────────────────────────────────────────────────


@app.get("/api/status")
def api_status():
    return jsonify(
        {
            "flare": flare_running(),
            "manga": list_manga(),
            "jobs": [
                {k: v for k, v in j.items() if k not in ("log", "proc", "thread")}
                for j in _jobs.values()
            ],
        }
    )


@app.post("/api/flare/start")
def api_flare_start():
    ok = flare_start()
    return jsonify({"ok": ok})


@app.post("/api/flare/stop")
def api_flare_stop():
    flare_stop()
    return jsonify({"ok": True})


@app.post("/api/scrape")
def api_scrape():
    data = request.json or {}
    url = (data.get("url") or "").strip()
    if not url:
        return jsonify({"error": "url required"}), 400
    extra = []
    if data.get("start"):
        extra += ["--start", str(data["start"])]
    if data.get("end"):
        extra += ["--end", str(data["end"])]
    jid = start_scrape(url, extra)
    return jsonify({"job_id": jid})


@app.post("/api/optimize")
def api_optimize():
    data = request.json or {}
    name = (data.get("manga") or "").strip()
    if not name:
        return jsonify({"error": "manga name required"}), 400
    manga_dir = str(Path(BASE_DIR) / name)
    if not Path(manga_dir).is_dir():
        return jsonify({"error": f"Directory not found: {manga_dir}"}), 404

    flags = []
    if data.get("cbz"):
        flags.append("--cbz")
    if data.get("cbz_only"):
        flags.append("--cbz-only")
    if data.get("delete_orig"):
        flags.append("--delete-orig")
    if data.get("quality"):
        flags += ["-q", str(data["quality"])]
    if data.get("max_width"):
        flags += ["-w", str(data["max_width"])]

    jid = start_optimize(manga_dir, flags)
    return jsonify({"job_id": jid})


@app.get("/api/job/<jid>/log")
def api_job_log(jid):
    job = _jobs.get(jid)
    if not job:
        abort(404)
    return jsonify(
        {
            "status": job["status"],
            "log": list(job["log"]),
        }
    )


@app.post("/api/job/<jid>/cancel")
def api_job_cancel(jid):
    job = _jobs.get(jid)
    if not job:
        abort(404)
    proc = job.get("proc")
    if proc and proc.poll() is None:
        proc.terminate()
        _finish(jid, False)
        return jsonify({"ok": True})
    return jsonify({"ok": False, "reason": "not running"})


@app.get("/api/download/<name>")
def api_download(name):
    """Zip and stream the -cbz folder for a given manga slug."""
    cbz_dir = Path(BASE_DIR) / (name + "-cbz")
    if not cbz_dir.is_dir():
        abort(404)
    tmp_zip = zip_cbz_dir(cbz_dir)

    def _stream():
        try:
            with open(tmp_zip, "rb") as fh:
                while chunk := fh.read(65536):
                    yield chunk
        finally:
            tmp_zip.unlink(missing_ok=True)

    return Response(
        _stream(),
        mimetype="application/zip",
        headers={
            "Content-Disposition": f'attachment; filename="{name}-cbz.zip"',
            "Content-Length": str(tmp_zip.stat().st_size),
        },
    )


# ── Cover image ───────────────────────────────────────────────────────────────

@app.get("/api/cover/<name>")
def api_cover(name):
    """Serve cover.jpg (or .jpeg/.png/.webp) from the manga's root directory."""
    base = Path(BASE_DIR)
    manga_dir = _safe_subpath(base, name)
    if manga_dir is None or not manga_dir.is_dir():
        abort(404)
    for fname in ("cover.jpg", "cover.jpeg", "cover.png", "cover.webp"):
        cover = manga_dir / fname
        if cover.is_file():
            return send_file(cover)
    abort(404)


# ── Delete API ────────────────────────────────────────────────────────────────


@app.delete("/api/delete/<name>")
def api_delete(name):
    """
    Delete a manga folder or a derived output folder.
    name can be:  solo-leveling
                  solo-leveling-cbz
                  solo-leveling-optimized
    Only directories inside BASE_DIR are permitted.
    """
    base = Path(BASE_DIR)
    target = _safe_subpath(base, name)
    if target is None or not target.is_dir():
        return jsonify({"error": "directory not found or invalid path"}), 404

    # Safety: must be a direct child of BASE_DIR (no nested deletes)
    if target.parent.resolve() != base.resolve():
        return jsonify({"error": "not a top-level manga directory"}), 403

    try:
        shutil.rmtree(target)
        return jsonify({"ok": True, "deleted": name})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Chapter reader API ────────────────────────────────────────────────────────


@app.get("/api/read/<manga>/chapters")
def api_chapters(manga):
    """List chapter folders for a manga (sorted)."""
    chapters = list_chapters(manga)
    if not chapters:
        abort(404)
    return jsonify({"manga": manga, "chapters": chapters})


@app.get("/api/read/<manga>/<chapter>/images")
def api_chapter_images(manga, chapter):
    """List image filenames in a chapter."""
    images = list_chapter_images(manga, chapter)
    if not images:
        abort(404)
    return jsonify({"manga": manga, "chapter": chapter, "images": images})


@app.get("/api/read/<manga>/<chapter>/img/<filename>")
def api_serve_image(manga, chapter, filename):
    """Serve a single chapter image from disk."""
    base = Path(BASE_DIR)
    # Try optimized first, then original
    ch_dir = _resolve_chapter_dir(manga, chapter)
    if ch_dir is None:
        abort(404)

    img_path = _safe_subpath(ch_dir, filename)
    if img_path is None or not img_path.is_file():
        abort(404)

    # Only serve known image types
    if img_path.suffix.lower() not in (".jpg", ".jpeg", ".png", ".webp"):
        abort(403)

    return send_file(img_path)


# ─────────────────────────────────────────────────────────────────────────────
#  Frontend  (UI redesign — all backend logic above is unchanged)
# ─────────────────────────────────────────────────────────────────────────────

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover">
<title>Nebula Manga</title>
<link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'%3E%3Cdefs%3E%3ClinearGradient id='g' x1='0' y1='0' x2='1' y2='1'%3E%3Cstop stop-color='%23ff4d8d'/%3E%3Cstop offset='1' stop-color='%238b5cf6'/%3E%3C/linearGradient%3E%3C/defs%3E%3Crect width='64' height='64' rx='18' fill='url(%23g)'/%3E%3Cpath d='M18 17h19a9 9 0 0 1 9 9v21H27a9 9 0 0 1-9-9V17Z' fill='none' stroke='white' stroke-width='4'/%3E%3Cpath d='M27 27h13M27 36h9' stroke='white' stroke-width='4' stroke-linecap='round'/%3E%3C/svg%3E">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&family=DM+Sans:ital,opsz,wght@0,9..40,300;0,9..40,400;0,9..40,500;0,9..40,600;1,9..40,300&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
<style>
:root {
  color-scheme: dark;
  --bg: #070912;
  --bg-2: #0d1020;
  --surface: rgba(18, 23, 42, 0.78);
  --surface-2: rgba(25, 31, 55, 0.86);
  --surface-3: rgba(39, 49, 83, 0.76);
  --line: rgba(148, 163, 184, 0.14);
  --line-strong: rgba(148, 163, 184, 0.24);
  --text: #f8fafc;
  --muted: #aab4c8;
  --dim: #68748e;
  --accent: #ff4d8d;
  --accent-2: #8b5cf6;
  --accent-3: #22d3ee;
  --success: #34d399;
  --warning: #fbbf24;
  --danger: #fb7185;
  --shadow: 0 24px 80px rgba(0, 0, 0, 0.42);
  --shadow-soft: 0 12px 35px rgba(0, 0, 0, 0.25);
  --radius-xl: 28px;
  --radius-lg: 20px;
  --radius-md: 14px;
  --radius-sm: 10px;
  --sidebar: 288px;
  --mobile-nav: 72px;
  --font: 'DM Sans', system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  --display: 'Bebas Neue', Impact, sans-serif;
  --mono: 'JetBrains Mono', ui-monospace, SFMono-Regular, monospace;
}
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
html,body{min-height:100%;background:var(--bg);color:var(--text);font-family:var(--font);-webkit-font-smoothing:antialiased;text-rendering:geometricPrecision}
body{background:
  radial-gradient(circle at 12% 4%, rgba(139,92,246,.32), transparent 32rem),
  radial-gradient(circle at 86% 14%, rgba(34,211,238,.18), transparent 30rem),
  radial-gradient(circle at 55% 105%, rgba(255,77,141,.18), transparent 34rem),
  linear-gradient(135deg, #070912 0%, #0b1020 44%, #05060c 100%);}
body::before{content:'';position:fixed;inset:0;pointer-events:none;opacity:.28;background-image:linear-gradient(rgba(255,255,255,.035) 1px, transparent 1px),linear-gradient(90deg, rgba(255,255,255,.035) 1px, transparent 1px);background-size:42px 42px;mask-image:linear-gradient(to bottom, black, transparent 76%)}
button,input,select{font:inherit}
button,a{touch-action:manipulation}
::-webkit-scrollbar{width:9px;height:9px}::-webkit-scrollbar-track{background:transparent}::-webkit-scrollbar-thumb{background:rgba(148,163,184,.24);border-radius:999px;border:2px solid transparent;background-clip:content-box}
.shell{display:grid;grid-template-columns:var(--sidebar) minmax(0,1fr);min-height:100vh;position:relative}
.sidebar{position:sticky;top:0;height:100vh;padding:18px;display:flex;flex-direction:column;gap:16px;background:linear-gradient(180deg,rgba(10,13,27,.88),rgba(12,16,32,.68));border-right:1px solid var(--line);backdrop-filter:blur(24px);box-shadow:12px 0 60px rgba(0,0,0,.22);z-index:30}
.logo-wrap{position:relative;overflow:hidden;padding:22px;border:1px solid var(--line);border-radius:var(--radius-xl);background:linear-gradient(145deg,rgba(255,255,255,.08),rgba(255,255,255,.025));box-shadow:var(--shadow-soft)}
.logo-wrap::after{content:'';position:absolute;inset:auto -35px -55px auto;width:130px;height:130px;border-radius:50%;background:linear-gradient(135deg,var(--accent),var(--accent-2));filter:blur(22px);opacity:.26}
.logo-mark{width:56px;height:56px;border-radius:18px;display:grid;place-items:center;background:linear-gradient(135deg,var(--accent),var(--accent-2));box-shadow:0 18px 45px rgba(255,77,141,.3);margin-bottom:16px}
.logo-mark svg{width:32px;height:32px;fill:none;stroke:white;stroke-width:1.8}
.logo-k{font-family:var(--display);font-size:18px;letter-spacing:2.8px;color:var(--accent-3);line-height:1;text-transform:uppercase}
.logo-en{font-family:var(--display);font-size:42px;letter-spacing:1.5px;line-height:.9;color:var(--text);text-transform:uppercase;margin-top:3px}
.logo-sub{font:600 10px/1.5 var(--mono);letter-spacing:1.4px;text-transform:uppercase;color:var(--dim);margin-top:10px;max-width:180px}
.flare-chip{border:1px solid var(--line);background:var(--surface);border-radius:18px;padding:12px;display:flex;align-items:center;gap:10px;justify-content:space-between;box-shadow:var(--shadow-soft)}
.flare-left{display:flex;align-items:center;gap:10px;min-width:0}.dot{width:10px;height:10px;border-radius:50%;background:var(--dim);box-shadow:0 0 0 6px rgba(104,116,142,.1);flex:none}.dot.on{background:var(--success);box-shadow:0 0 0 6px rgba(52,211,153,.12),0 0 24px rgba(52,211,153,.65)}.flare-name{font:700 11px/1 var(--mono);letter-spacing:.4px;color:var(--muted);white-space:nowrap}.flare-btn{border:1px solid transparent;border-radius:999px;padding:8px 13px;cursor:pointer;text-transform:uppercase;font:800 10px/1 var(--mono);letter-spacing:.8px;transition:.18s ease}.flare-btn.start{color:#062016;background:rgba(52,211,153,.9)}.flare-btn.stop{color:#2a0710;background:rgba(251,113,133,.92)}.flare-btn:hover{transform:translateY(-1px);filter:saturate(1.1) brightness(1.05)}
.nav{display:flex;flex-direction:column;gap:8px;padding:4px}.nav-label{font:800 10px/1 var(--mono);letter-spacing:2px;text-transform:uppercase;color:var(--dim);padding:6px 10px}.nav-item{position:relative;width:100%;border:1px solid transparent;background:transparent;color:var(--muted);border-radius:17px;padding:13px 14px;display:flex;align-items:center;gap:12px;cursor:pointer;text-align:left;font-weight:800;transition:.18s ease}.nav-item:hover{background:rgba(255,255,255,.055);color:var(--text);border-color:var(--line)}.nav-item.active{color:var(--text);background:linear-gradient(135deg,rgba(255,77,141,.22),rgba(139,92,246,.16));border-color:rgba(255,77,141,.26);box-shadow:0 10px 32px rgba(255,77,141,.12)}.nav-icon,.bn-ico{display:inline-grid;place-items:center;flex:0 0 auto}.nav-icon svg{width:21px;height:21px;stroke:currentColor;fill:none;stroke-width:2;stroke-linecap:round;stroke-linejoin:round}.nav-badge{margin-left:auto;display:none;min-width:21px;height:21px;place-items:center;border-radius:999px;background:var(--warning);color:#1c1300;font:900 10px/1 var(--mono)}
.main{min-width:0;padding:38px clamp(18px,4vw,58px) 48px}.panel{display:none;animation:panelIn .22s ease-out}.panel.active{display:block}@keyframes panelIn{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:none}}
.pg-title{font-family:var(--display);font-size:clamp(44px,7vw,78px);letter-spacing:1.4px;line-height:.88;text-transform:uppercase;background:linear-gradient(135deg,#fff 20%,#cbd5e1 48%,#8b5cf6 88%);-webkit-background-clip:text;background-clip:text;color:transparent}.pg-sub{margin:12px 0 28px;color:var(--muted);font:700 12px/1.6 var(--mono);letter-spacing:.8px;text-transform:uppercase}.pg-sub::before{content:'◆ ';color:var(--accent)}
.card{position:relative;overflow:hidden;background:linear-gradient(145deg,rgba(18,23,42,.86),rgba(13,16,32,.74));border:1px solid var(--line);border-radius:var(--radius-xl);padding:24px;margin-bottom:18px;box-shadow:var(--shadow-soft);backdrop-filter:blur(18px)}.card::before{content:'';position:absolute;inset:0 0 auto;height:1px;background:linear-gradient(90deg,transparent,rgba(255,255,255,.28),transparent)}.card-hd{display:flex;align-items:center;gap:10px;margin-bottom:18px;color:var(--muted);font:900 12px/1 var(--mono);letter-spacing:1.8px;text-transform:uppercase}.card-hd::before{content:'';width:28px;height:8px;border-radius:999px;background:linear-gradient(90deg,var(--accent),var(--accent-2))}
.field{margin-bottom:14px}.field label{display:block;margin-bottom:8px;color:var(--muted);font:900 11px/1 var(--mono);text-transform:uppercase;letter-spacing:1px}.field input,.field select{width:100%;border:1px solid var(--line);border-radius:16px;background:rgba(5,7,15,.74);color:var(--text);padding:14px 15px;outline:none;transition:.18s ease;font:600 13px/1.2 var(--mono)}.field input:focus,.field select:focus{border-color:rgba(255,77,141,.65);box-shadow:0 0 0 4px rgba(255,77,141,.13),0 14px 45px rgba(255,77,141,.08)}.field input::placeholder{color:var(--dim)}.row2{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}.checks{display:flex;gap:12px;flex-wrap:wrap;margin:6px 0 18px}.chk{display:flex;align-items:center;gap:9px;color:var(--muted);font-weight:800;font-size:13px;background:rgba(255,255,255,.045);border:1px solid var(--line);border-radius:999px;padding:10px 13px;cursor:pointer}.chk input{accent-color:var(--accent);width:15px;height:15px}.chk:hover{color:var(--text);border-color:var(--line-strong)}
.btn{display:inline-flex;align-items:center;justify-content:center;gap:8px;border:1px solid transparent;border-radius:999px;padding:11px 18px;cursor:pointer;font-weight:900;font-size:13px;letter-spacing:.2px;transition:.18s ease;text-decoration:none;white-space:nowrap}.btn svg,.m-overlay-btn svg,.reader-close svg,.rnav-btn svg{width:17px;height:17px;stroke:currentColor;fill:none;stroke-width:2;stroke-linecap:round;stroke-linejoin:round}.btn:hover{transform:translateY(-1px)}.btn:disabled{opacity:.36;cursor:not-allowed;transform:none!important}.btn-red{color:white;background:linear-gradient(135deg,var(--accent),var(--accent-2));box-shadow:0 14px 35px rgba(255,77,141,.25)}.btn-ghost{color:var(--muted);background:rgba(255,255,255,.055);border-color:var(--line)}.btn-ghost:hover{color:var(--text);border-color:var(--line-strong);background:rgba(255,255,255,.08)}.btn-read{color:white;background:linear-gradient(135deg,var(--accent),var(--accent-2))}.btn-dl{color:#042017;background:rgba(52,211,153,.92);border-color:rgba(52,211,153,.25)}.btn-del{color:var(--danger);background:rgba(251,113,133,.08);border-color:rgba(251,113,133,.2);font-size:12px;padding:9px 12px}.btn-del:hover{background:rgba(251,113,133,.14);border-color:rgba(251,113,133,.42)}
.lib-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(210px,1fr));gap:22px}.m-card{position:relative;overflow:hidden;border-radius:26px;background:linear-gradient(160deg,rgba(18,23,42,.92),rgba(13,16,32,.8));border:1px solid var(--line);box-shadow:var(--shadow-soft);transition:.22s ease}.m-card:hover{transform:translateY(-6px);border-color:rgba(255,255,255,.25);box-shadow:0 26px 70px rgba(0,0,0,.38)}.m-cover{position:relative;aspect-ratio:2/3;background:linear-gradient(135deg,rgba(139,92,246,.22),rgba(34,211,238,.1));overflow:hidden}.m-cover::after{content:'';position:absolute;inset:40% 0 0;background:linear-gradient(to top,rgba(7,9,18,.96),transparent);pointer-events:none}.m-cover img{width:100%;height:100%;object-fit:cover;display:block;transition:transform .45s ease,filter .45s ease}.m-card:hover .m-cover img{transform:scale(1.055);filter:saturate(1.08)}.m-cover-ph{width:100%;height:100%;display:flex;align-items:center;justify-content:center;color:rgba(255,255,255,.52)}.m-cover-ph svg{width:76px;height:76px;stroke:currentColor;fill:none;stroke-width:1.4}.m-badges{position:absolute;top:12px;left:12px;right:12px;display:flex;gap:7px;flex-wrap:wrap;z-index:2}.m-badge{font:900 10px/1 var(--mono);letter-spacing:.8px;text-transform:uppercase;padding:7px 9px;border-radius:999px;backdrop-filter:blur(12px);border:1px solid rgba(255,255,255,.16)}.mb-cbz{background:rgba(52,211,153,.86);color:#032016}.mb-opt{background:rgba(251,191,36,.88);color:#1f1400}.m-overlay{position:absolute;left:12px;right:12px;bottom:12px;z-index:3;opacity:0;transform:translateY(10px);transition:.22s ease}.m-card:hover .m-overlay{opacity:1;transform:none}.m-overlay-btn{width:100%;display:flex;align-items:center;justify-content:center;gap:8px;border:none;border-radius:999px;padding:12px 16px;color:white;background:linear-gradient(135deg,var(--accent),var(--accent-2));box-shadow:0 15px 38px rgba(255,77,141,.33);font-weight:1000;cursor:pointer}.m-info{padding:16px 16px 10px}.m-name{font-size:15px;font-weight:1000;line-height:1.25;min-height:38px;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}.m-meta{margin-top:8px;color:var(--muted);font:800 11px/1 var(--mono);letter-spacing:.5px;text-transform:uppercase}.m-actions{display:grid;gap:9px;padding:0 16px 16px}.m-actions .btn{width:100%;padding:10px 12px;font-size:12px}.m-del-row{display:grid;grid-template-columns:1fr;gap:7px}.m-del-row .btn{justify-content:flex-start;border-radius:14px;font-size:11px;white-space:normal;text-align:left;line-height:1.25}.m-del-row .btn .del-small{display:block;color:var(--dim);font:800 9px/1.35 var(--mono);text-transform:uppercase;letter-spacing:.4px;margin-left:auto}
.log-top{display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:14px}.logbox{height:330px;overflow:auto;padding:16px;border-radius:18px;border:1px solid var(--line);background:#04060c;color:var(--dim);font:600 12px/1.85 var(--mono);white-space:pre-wrap;word-break:break-word}.l-ok{color:var(--success)}.l-err{color:var(--danger)}.l-warn{color:var(--warning)}.l-dim{color:#8290aa}
.job-row{display:grid;grid-template-columns:auto minmax(0,1fr) auto auto auto;align-items:center;gap:10px;padding:13px 0;border-bottom:1px solid var(--line)}.job-row:last-child{border-bottom:0}.jt{border-radius:999px;padding:7px 10px;font:900 10px/1 var(--mono);letter-spacing:.8px;text-transform:uppercase}.jt-scrape{background:rgba(255,77,141,.14);color:#ff8fba}.jt-optimize{background:rgba(139,92,246,.17);color:#c4b5fd}.ji,.jtime{color:var(--dim);font:700 11px/1 var(--mono);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.jst{font:900 11px/1 var(--mono);text-transform:uppercase}.jst.running{color:var(--warning);animation:pulse 1.4s infinite}.jst.done{color:var(--success)}.jst.failed{color:var(--danger)}@keyframes pulse{50%{opacity:.35}}
.modal-bg{display:none;position:fixed;inset:0;z-index:400;padding:20px;align-items:center;justify-content:center;background:rgba(2,4,10,.76);backdrop-filter:blur(18px)}.modal-bg.open{display:flex}.modal{max-width:430px;width:100%;padding:26px;border-radius:28px;background:linear-gradient(145deg,rgba(18,23,42,.96),rgba(12,16,32,.94));border:1px solid rgba(251,113,133,.25);box-shadow:var(--shadow)}.modal-title{font-family:var(--display);font-size:46px;letter-spacing:1.3px;color:var(--danger);line-height:.9}.modal-body{margin:16px 0 24px;color:var(--muted);font-size:14px;line-height:1.7}.modal-body strong{display:inline-block;margin-top:6px;color:var(--text);font-family:var(--mono);word-break:break-word}.modal-acts{display:flex;justify-content:flex-end;gap:10px;flex-wrap:wrap}
.reader{display:none;position:fixed;inset:0;z-index:500;background:#02030a;flex-direction:column}.reader.open{display:flex}.reader-hd{position:relative;z-index:5;display:grid;grid-template-columns:auto minmax(0,1fr) auto;align-items:center;gap:12px;padding:12px 16px;border-bottom:1px solid rgba(255,255,255,.1);background:linear-gradient(180deg,rgba(5,7,15,.94),rgba(5,7,15,.78));backdrop-filter:blur(18px);box-shadow:0 10px 45px rgba(0,0,0,.32);flex:none}.reader-title{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-weight:1000;font-size:14px}.reader-close,.rnav-btn{display:inline-flex;align-items:center;justify-content:center;gap:7px;border:1px solid var(--line);border-radius:999px;color:var(--muted);background:rgba(255,255,255,.055);padding:10px 13px;cursor:pointer;font-weight:900;font-size:12px;transition:.18s ease}.reader-close:hover,.rnav-btn:hover{color:var(--text);border-color:var(--line-strong);background:rgba(255,255,255,.09)}.rnav-btn:disabled{opacity:.28;cursor:not-allowed}.reader-nav{display:flex;align-items:center;gap:8px}.rch-sel{max-width:190px;border:1px solid var(--line);border-radius:999px;background:rgba(5,7,15,.9);color:var(--text);padding:10px 12px;outline:0;font:800 12px/1 var(--mono)}.reader-body{flex:1;overflow:auto;display:block;background:radial-gradient(circle at top,rgba(139,92,246,.08),transparent 28rem),#02030a;-webkit-overflow-scrolling:touch}.reader-pages{width:100%;display:flex;flex-direction:column;align-items:center}.rpage{width:100%;max-width:min(920px,100vw);margin:0 auto;background:#000}.rpage img{display:block;width:100%;height:auto;margin:0 auto 2px}.rspin{padding:90px 20px;text-align:center;color:var(--dim);font:800 13px/1 var(--mono)}
.bot-nav{display:none;position:fixed;left:12px;right:12px;bottom:12px;z-index:200;padding-bottom:env(safe-area-inset-bottom)}.bot-nav-inner{height:64px;display:grid;grid-template-columns:repeat(4,1fr);gap:6px;padding:7px;border:1px solid var(--line);border-radius:24px;background:rgba(10,13,27,.84);backdrop-filter:blur(22px);box-shadow:var(--shadow)}.bn{position:relative;border:0;background:transparent;color:var(--muted);border-radius:18px;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:3px;font-size:10px;font-weight:1000;cursor:pointer}.bn.active{color:white;background:linear-gradient(135deg,rgba(255,77,141,.28),rgba(139,92,246,.22))}.bn svg{width:21px;height:21px;stroke:currentColor;fill:none;stroke-width:2;stroke-linecap:round;stroke-linejoin:round}.bn-bdg{position:absolute;top:5px;right:20%;display:none;min-width:18px;height:18px;border-radius:999px;background:var(--warning);color:#1d1400;font:900 9px/18px var(--mono)}
#toast{position:fixed;right:18px;bottom:18px;z-index:600;max-width:320px;opacity:0;transform:translateY(8px);pointer-events:none;transition:.2s ease;padding:13px 16px;border-radius:18px;background:rgba(12,16,32,.94);border:1px solid var(--line-strong);box-shadow:var(--shadow);font:800 12px/1.4 var(--mono);color:var(--text)}#toast.show{opacity:1;transform:none}.empty{grid-column:1/-1;color:var(--dim);font:800 13px/1.6 var(--mono);text-align:center;padding:60px 18px;border:1px dashed var(--line);border-radius:24px;background:rgba(255,255,255,.025)}
@media(max-width:900px){.shell{grid-template-columns:1fr}.sidebar{display:none}.main{padding:22px 14px calc(var(--mobile-nav) + 34px)}.bot-nav{display:block}.row2{grid-template-columns:1fr}.lib-grid{grid-template-columns:repeat(auto-fill,minmax(156px,1fr));gap:14px}.m-card{border-radius:22px}.m-info{padding:13px 13px 8px}.m-actions{padding:0 13px 13px}.m-overlay{opacity:1;transform:none}.m-del-row .btn .del-small{display:none}.job-row{grid-template-columns:auto minmax(0,1fr) auto}.jtime{display:none}.job-row .btn{grid-column:1/-1}.logbox{height:260px}#toast{bottom:calc(var(--mobile-nav) + 30px);left:14px;right:14px;max-width:none}.reader{background:#000}.reader-hd{order:2;position:fixed;left:10px;right:10px;bottom:10px;grid-template-columns:1fr;gap:8px;padding:10px;border:1px solid rgba(255,255,255,.13);border-radius:24px;background:rgba(6,8,18,.86);box-shadow:0 -18px 60px rgba(0,0,0,.5);padding-bottom:calc(10px + env(safe-area-inset-bottom))}.reader-title{order:-1;text-align:center;font-size:12px;padding:0 6px}.reader-close{position:absolute;left:10px;top:10px;width:42px;height:42px;padding:0;font-size:0;border-radius:16px}.reader-close svg{width:20px;height:20px}.reader-nav{width:100%;display:grid;grid-template-columns:44px minmax(0,1fr) 44px;gap:8px;padding-left:50px}.rnav-btn{width:44px;height:42px;padding:0;border-radius:16px;font-size:0}.rnav-btn svg{width:20px;height:20px}.rch-sel{max-width:100%;width:100%;height:42px;border-radius:16px;text-align:center}.reader-body{order:1;height:100vh;padding-bottom:116px}.rpage{max-width:100vw}.rpage img{margin-bottom:0}.rspin{padding-top:42vh}.pg-title{font-size:44px}}
@media(max-width:420px){.lib-grid{grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}.m-name{font-size:13px;min-height:34px}.m-actions .btn{font-size:11px}.btn{padding:10px 14px}.card{padding:18px;border-radius:22px}.pg-title{font-size:40px}}
</style>
</head>
<body>
<div class="shell">

<!-- Sidebar -->
<aside class="sidebar">
  <div class="logo-wrap">
    <div class="logo-mark"><svg viewBox="0 0 48 48" aria-hidden="true"><path d="M13 9h17a10 10 0 0 1 10 10v20H23a10 10 0 0 1-10-10V9Z"></path><path d="M21 19h12M21 27h8"></path></svg></div>
    <div class="logo-k">Nebula</div>
    <div class="logo-en">MANGA</div>
    <div class="logo-sub">scrape, optimize, and read your collection</div>
  </div>
  <div class="flare-chip">
    <div class="flare-left">
      <span class="dot off" id="flare-dot"></span>
      <span class="flare-name">FlareSolverr</span>
    </div>
    <button class="flare-btn start" id="flare-btn" onclick="toggleFlare()">start</button>
  </div>
  <nav class="nav">
    <div class="nav-label">Navigation</div>
    <button class="nav-item active" id="sb-scrape"   onclick="showPanel('scrape',this)">  <span class="nav-icon"><svg viewBox="0 0 24 24"><path d="M5 12h4l3-7 3 14 3-7h1"></path><path d="M4 19h16"></path></svg></span> Scrape</button>
    <button class="nav-item"        id="sb-library"  onclick="showPanel('library',this)"> <span class="nav-icon"><svg viewBox="0 0 24 24"><path d="M4 5.5A2.5 2.5 0 0 1 6.5 3H20v16H6.5A2.5 2.5 0 0 0 4 21.5v-16Z"></path><path d="M8 7h8M8 11h7"></path></svg></span> Library</button>
    <button class="nav-item"        id="sb-optimize" onclick="showPanel('optimize',this)"><span class="nav-icon"><svg viewBox="0 0 24 24"><path d="m13 2-9 12h7l-1 8 10-13h-7l1-7Z"></path></svg></span> Optimize</button>
    <button class="nav-item"        id="sb-jobs"     onclick="showPanel('jobs',this)">
      <span class="nav-icon"><svg viewBox="0 0 24 24"><path d="M12 15.5a3.5 3.5 0 1 0 0-7 3.5 3.5 0 0 0 0 7Z"></path><path d="M19.4 15a1.7 1.7 0 0 0 .34 1.88l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06A1.7 1.7 0 0 0 15 19.4a1.7 1.7 0 0 0-1 1.55V21a2 2 0 1 1-4 0v-.08A1.7 1.7 0 0 0 9 19.4a1.7 1.7 0 0 0-1.88.34l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06A1.7 1.7 0 0 0 4.6 15a1.7 1.7 0 0 0-1.55-1H3a2 2 0 1 1 0-4h.08A1.7 1.7 0 0 0 4.6 9a1.7 1.7 0 0 0-.34-1.88l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06A1.7 1.7 0 0 0 9 4.6a1.7 1.7 0 0 0 1-1.55V3a2 2 0 1 1 4 0v.08A1.7 1.7 0 0 0 15 4.6a1.7 1.7 0 0 0 1.88-.34l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06A1.7 1.7 0 0 0 19.4 9c.13.39.46.7.85.85.25.1.52.15.8.15H21a2 2 0 1 1 0 4h-.08A1.7 1.7 0 0 0 19.4 15Z"></path></svg></span> Jobs
      <span class="nav-badge" id="jobs-badge-sb"></span>
    </button>
  </nav>
</aside>

<!-- Main -->
<main class="main">

  <!-- SCRAPE -->
  <div id="panel-scrape" class="panel active">
    <div class="pg-title">SCRAPE</div>
    <div class="pg-sub">// FlareSolverr starts automatically</div>
    <div class="card">
      <div class="card-hd">Target URL</div>
      <div class="field">
        <label>URL</label>
        <input id="scrape-url" type="url" placeholder="https://lek-manga.net/manga/solo-leveling/">
      </div>
      <div class="row2">
        <div class="field"><label>Start Chapter</label><input id="scrape-start" type="text" placeholder="e.g. 1"></div>
        <div class="field"><label>End Chapter</label><input id="scrape-end" type="text" placeholder="e.g. 100 (default: all)"></div>
      </div>
      <button class="btn btn-red" onclick="startScrape()"><svg viewBox="0 0 24 24"><path d="M5 12h4l3-7 3 14 3-7h1"></path><path d="M4 19h16"></path></svg>Start Scrape</button>
    </div>
    <div class="card" id="scrape-log-card" style="display:none;">
      <div class="log-top">
        <div class="card-hd" style="margin-bottom:0;">Live Output</div>
        <button class="btn btn-del" id="scrape-cancel-btn" onclick="cancelJob(currentScrapeJob)">✕ Cancel</button>
      </div>
      <div class="logbox" id="scrape-log"></div>
    </div>
  </div>

  <!-- LIBRARY -->
  <div id="panel-library" class="panel">
    <div class="pg-title">LIBRARY</div>
    <div class="pg-sub">// your downloaded collection</div>
    <div id="library-grid" class="lib-grid"><div class="empty">Loading…</div></div>
  </div>

  <!-- OPTIMIZE -->
  <div id="panel-optimize" class="panel">
    <div class="pg-title">OPTIMIZE</div>
    <div class="pg-sub">// convert to WebP, resize, pack CBZ</div>
    <div class="card">
      <div class="card-hd">Settings</div>
      <div class="field">
        <label>Manga</label>
        <select id="opt-manga"><option value="">— select —</option></select>
      </div>
      <div class="row2">
        <div class="field"><label>Quality (0-100)</label><input id="opt-quality" type="number" min="0" max="100" value="82"></div>
        <div class="field"><label>Max Width px</label><input id="opt-width" type="number" min="0" value="1400"></div>
      </div>
      <div class="checks">
        <label class="chk"><input type="checkbox" id="opt-cbz" checked> Pack CBZ</label>
        <label class="chk"><input type="checkbox" id="opt-cbz-only"> CBZ only</label>
        <label class="chk"><input type="checkbox" id="opt-delete"> Delete originals</label>
      </div>
      <button class="btn btn-red" onclick="startOptimize()"><svg viewBox="0 0 24 24"><path d="m13 2-9 12h7l-1 8 10-13h-7l1-7Z"></path></svg>Start Optimize</button>
    </div>
    <div class="card" id="opt-log-card" style="display:none;">
      <div class="log-top">
        <div class="card-hd" style="margin-bottom:0;">Live Output</div>
        <button class="btn btn-del" id="opt-cancel-btn" onclick="cancelJob(currentOptJob)">✕ Cancel</button>
      </div>
      <div class="logbox" id="opt-log"></div>
    </div>
  </div>

  <!-- JOBS -->
  <div id="panel-jobs" class="panel">
    <div class="pg-title">JOBS</div>
    <div class="pg-sub">// all scrape and optimize tasks this session</div>
    <div class="card"><div id="jobs-list"><div class="empty">No jobs yet.</div></div></div>
    <div class="card" id="job-detail-card" style="display:none;">
      <div class="card-hd" id="job-detail-title">Job Log</div>
      <div class="logbox" id="job-detail-log"></div>
    </div>
  </div>

</main>
</div>

<!-- Bottom nav (mobile) -->
<nav class="bot-nav">
  <div class="bot-nav-inner">
    <button class="bn active" id="bn-scrape"   onclick="showPanel('scrape',this,true)">  <span class="bn-ico"><svg viewBox="0 0 24 24"><path d="M5 12h4l3-7 3 14 3-7h1"></path><path d="M4 19h16"></path></svg></span>Scrape</button>
    <button class="bn"        id="bn-library"  onclick="showPanel('library',this,true)"> <span class="bn-ico"><svg viewBox="0 0 24 24"><path d="M4 5.5A2.5 2.5 0 0 1 6.5 3H20v16H6.5A2.5 2.5 0 0 0 4 21.5v-16Z"></path><path d="M8 7h8M8 11h7"></path></svg></span>Library</button>
    <button class="bn"        id="bn-optimize" onclick="showPanel('optimize',this,true)"><span class="bn-ico"><svg viewBox="0 0 24 24"><path d="m13 2-9 12h7l-1 8 10-13h-7l1-7Z"></path></svg></span>Optimize</button>
    <button class="bn"        id="bn-jobs"     onclick="showPanel('jobs',this,true)">
      <span class="bn-ico"><svg viewBox="0 0 24 24"><path d="M12 15.5a3.5 3.5 0 1 0 0-7 3.5 3.5 0 0 0 0 7Z"></path><path d="M19.4 15a1.7 1.7 0 0 0 .34 1.88l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06A1.7 1.7 0 0 0 15 19.4a1.7 1.7 0 0 0-1 1.55V21a2 2 0 1 1-4 0v-.08A1.7 1.7 0 0 0 9 19.4a1.7 1.7 0 0 0-1.88.34l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06A1.7 1.7 0 0 0 4.6 15a1.7 1.7 0 0 0-1.55-1H3a2 2 0 1 1 0-4h.08A1.7 1.7 0 0 0 4.6 9a1.7 1.7 0 0 0-.34-1.88l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06A1.7 1.7 0 0 0 9 4.6a1.7 1.7 0 0 0 1-1.55V3a2 2 0 1 1 4 0v.08A1.7 1.7 0 0 0 15 4.6a1.7 1.7 0 0 0 1.88-.34l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06A1.7 1.7 0 0 0 19.4 9c.13.39.46.7.85.85.25.1.52.15.8.15H21a2 2 0 1 1 0 4h-.08A1.7 1.7 0 0 0 19.4 15Z"></path></svg></span>Jobs
      <span class="bn-bdg" id="jobs-badge-bn"></span>
    </button>
  </div>
</nav>

<!-- Delete modal -->
<div class="modal-bg" id="del-modal">
  <div class="modal">
    <div class="modal-title">DELETE</div>
    <div class="modal-body">Permanently remove:<br><strong id="del-target-label"></strong><br><br>This cannot be undone.</div>
    <div class="modal-acts">
      <button class="btn btn-ghost" onclick="closeDelModal()">Cancel</button>
      <button class="btn btn-read" style="border-color:rgba(232,41,74,.3)" onclick="confirmDelete()">Delete</button>
    </div>
  </div>
</div>

<!-- Reader -->
<div class="reader" id="reader">
  <div class="reader-hd">
    <button class="reader-close" onclick="closeReader()"><svg viewBox="0 0 24 24"><path d="M18 6 6 18M6 6l12 12"></path></svg><span>Close</span></button>
    <span class="reader-title" id="reader-title">—</span>
    <div class="reader-nav">
      <button class="rnav-btn" id="reader-prev" onclick="readerPrev()"><svg viewBox="0 0 24 24"><path d="m15 18-6-6 6-6"></path></svg><span>Prev</span></button>
      <select class="rch-sel" id="reader-sel" onchange="readerJump(this.value)"></select>
      <button class="rnav-btn" id="reader-next" onclick="readerNext()"><span>Next</span><svg viewBox="0 0 24 24"><path d="m9 18 6-6-6-6"></path></svg></button>
    </div>
  </div>
  <div class="reader-body" id="reader-body">
    <div class="rspin" id="reader-spin">Loading…</div>
    <div class="reader-pages" id="reader-pages"></div>
  </div>
</div>

<div id="toast"></div>

<script>
let currentScrapeJob=null,currentOptJob=null,_pollers={},_delTarget=null;
let _readerManga=null,_readerChs=[],_readerIdx=0;
const ICONS={
  read:'<svg viewBox="0 0 24 24"><path d="M8 5v14l11-7-11-7Z"></path></svg>',
  download:'<svg viewBox="0 0 24 24"><path d="M12 3v11"></path><path d="m7 10 5 5 5-5"></path><path d="M5 21h14"></path></svg>',
  bolt:'<svg viewBox="0 0 24 24"><path d="m13 2-9 12h7l-1 8 10-13h-7l1-7Z"></path></svg>',
  trash:'<svg viewBox="0 0 24 24"><path d="M3 6h18"></path><path d="M8 6V4h8v2"></path><path d="M6 6l1 15h10l1-15"></path><path d="M10 11v6M14 11v6"></path></svg>',
  book:'<svg viewBox="0 0 48 48"><path d="M10 11h21a7 7 0 0 1 7 7v19H17a7 7 0 0 1-7-7V11Z"></path><path d="M18 21h13M18 29h9"></path></svg>'
};

function showPanel(name,btn,mobile=false){
  document.querySelectorAll('.panel').forEach(p=>p.classList.remove('active'));
  document.getElementById('panel-'+name).classList.add('active');
  document.querySelectorAll('.nav-item,.bn').forEach(b=>b.classList.remove('active'));
  const sb=document.getElementById('sb-'+name);
  const bn=document.getElementById('bn-'+name);
  if(sb)sb.classList.add('active');
  if(bn)bn.classList.add('active');
  if(name==='library')refreshLibrary();
  if(name==='optimize')refreshOptSel();
  if(name==='jobs')refreshJobs();
}

function toast(msg,col){
  const t=document.getElementById('toast');
  t.textContent=msg;t.style.color=col||'var(--text)';
  t.classList.add('show');
  setTimeout(()=>t.classList.remove('show'),3000);
}

function esc(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}
function attr(s){return esc(s).replace(/"/g,'&quot;').replace(/'/g,'&#39;')}

async function pollStatus(){
  try{
    const d=await fetch('/api/status').then(r=>r.json());
    updateFlareUI(d.flare);updateBadge(d.jobs);
  }catch(e){}
}
setInterval(pollStatus,3000);pollStatus();

function updateFlareUI(on){
  document.getElementById('flare-dot').className='dot '+(on?'on':'off');
  const btn=document.getElementById('flare-btn');
  btn.textContent=on?'stop':'start';
  btn.className='flare-btn '+(on?'stop':'start');
}

function updateBadge(jobs){
  const n=jobs.filter(j=>j.status==='running').length;
  ['jobs-badge-sb','jobs-badge-bn'].forEach(id=>{
    const el=document.getElementById(id);
    if(!el)return;
    el.textContent=n;el.style.display=n>0?'inline':'none';
  });
}

async function toggleFlare(){
  const btn=document.getElementById('flare-btn');
  const starting=btn.textContent.trim()==='start';
  btn.disabled=true;btn.textContent='…';
  try{
    await fetch('/api/flare/'+(starting?'start':'stop'),{method:'POST'});
    await pollStatus();
    toast(starting?'✔ FlareSolverr started':'FlareSolverr stopped');
  }finally{btn.disabled=false;}
}

function colorLine(line){
  const s=esc(line);
  if(/✔|OK/.test(line))return`<span class="l-ok">${s}</span>`;
  if(/✘|ERROR|FAIL/.test(line))return`<span class="l-err">${s}</span>`;
  if(/⚠|WARN/.test(line))return`<span class="l-warn">${s}</span>`;
  return`<span class="l-dim">${s}</span>`;
}

function startLogPoll(jid,boxId,cancelId,onDone){
  if(_pollers[jid])return;
  _pollers[jid]=setInterval(async()=>{
    try{
      const d=await fetch(`/api/job/${jid}/log`).then(r=>r.json());
      const box=document.getElementById(boxId);
      if(box){
        const atBot=box.scrollHeight-box.clientHeight<=box.scrollTop+40;
        box.innerHTML=d.log.map(colorLine).join('\n');
        if(atBot)box.scrollTop=box.scrollHeight;
      }
      if(d.status!=='running'){
        clearInterval(_pollers[jid]);delete _pollers[jid];
        const cb=document.getElementById(cancelId);if(cb)cb.disabled=true;
        if(onDone)onDone(d.status);
      }
    }catch(e){}
  },1000);
}

async function startScrape(){
  const url=document.getElementById('scrape-url').value.trim();
  const start=document.getElementById('scrape-start').value.trim();
  const end=document.getElementById('scrape-end').value.trim();
  if(!url){toast('Enter a URL first','var(--red)');return;}
  const d=await fetch('/api/scrape',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({url,start:start||null,end:end||null})}).then(r=>r.json());
  if(d.error){toast(d.error,'var(--red)');return;}
  currentScrapeJob=d.job_id;
  document.getElementById('scrape-log-card').style.display='block';
  document.getElementById('scrape-cancel-btn').disabled=false;
  document.getElementById('scrape-log').innerHTML='';
  toast('Scrape started');
  startLogPoll(d.job_id,'scrape-log','scrape-cancel-btn',s=>{
    toast(s==='done'?'✔ Scrape complete':'✘ Scrape failed',s==='done'?'var(--green)':'var(--red)');
  });
}

async function refreshOptSel(){
  const d=await fetch('/api/status').then(r=>r.json());
  const sel=document.getElementById('opt-manga');
  const cur=sel.value;
  sel.innerHTML='<option value="">— select —</option>';
  d.manga.forEach(m=>{
    const o=document.createElement('option');
    o.value=m.name;o.textContent=m.name;
    if(m.name===cur)o.selected=true;
    sel.appendChild(o);
  });
}

async function startOptimize(){
  const manga=document.getElementById('opt-manga').value;
  const quality=document.getElementById('opt-quality').value;
  const max_width=document.getElementById('opt-width').value;
  const cbz=document.getElementById('opt-cbz').checked;
  const cbz_only=document.getElementById('opt-cbz-only').checked;
  const del_orig=document.getElementById('opt-delete').checked;
  if(!manga){toast('Select a manga first','var(--red)');return;}
  const d=await fetch('/api/optimize',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({manga,quality,max_width,cbz,cbz_only,delete_orig:del_orig})}).then(r=>r.json());
  if(d.error){toast(d.error,'var(--red)');return;}
  currentOptJob=d.job_id;
  document.getElementById('opt-log-card').style.display='block';
  document.getElementById('opt-cancel-btn').disabled=false;
  document.getElementById('opt-log').innerHTML='';
  toast('Optimize started');
  startLogPoll(d.job_id,'opt-log','opt-cancel-btn',s=>{
    toast(s==='done'?'✔ Optimize complete':'✘ Optimize failed',s==='done'?'var(--green)':'var(--red)');
    refreshLibrary();
  });
}

async function refreshJobs(){
  const d=await fetch('/api/status').then(r=>r.json());
  const list=document.getElementById('jobs-list');
  if(!d.jobs.length){list.innerHTML='<div class="empty">No jobs yet.</div>';return;}
  list.innerHTML=[...d.jobs].reverse().map(j=>`
    <div class="job-row">
      <span class="jt jt-${j.type}">${j.type}</span>
      <span class="ji">${j.id}</span>
      <span class="jtime">${j.started}${j.ended?' → '+j.ended:''}</span>
      <span class="jst ${j.status}">${j.status}</span>
      <button class="btn btn-ghost" style="font-size:10px;padding:4px 10px;" onclick="viewJobLog('${j.id}','${j.type}')">Log</button>
    </div>`).join('');
}

async function viewJobLog(jid,type){
  document.getElementById('job-detail-card').style.display='block';
  document.getElementById('job-detail-title').textContent=`${type} / ${jid}`;
  const d=await fetch(`/api/job/${jid}/log`).then(r=>r.json());
  const box=document.getElementById('job-detail-log');
  box.innerHTML=d.log.map(colorLine).join('\n');
  box.scrollTop=box.scrollHeight;
}

async function cancelJob(jid){
  if(!jid)return;
  await fetch(`/api/job/${jid}/cancel`,{method:'POST'});
  toast('Cancel requested');
}

async function refreshLibrary(){
  const d=await fetch('/api/status').then(r=>r.json());
  const grid=document.getElementById('library-grid');
  if(!d.manga.length){grid.innerHTML='<div class="empty">No manga downloaded yet.</div>';return;}
  grid.innerHTML=d.manga.map(m=>{
    const coverUrl=`/api/cover/${encodeURIComponent(m.name)}`;
    const nameArg=JSON.stringify(m.name);
    const sourceArg=JSON.stringify(m.name);
    const optArg=JSON.stringify(m.name+'-optimized');
    const cbzArg=JSON.stringify(m.name+'-cbz');
    const sourceLabel=JSON.stringify(`${m.name} — original manga folder`);
    const optLabel=JSON.stringify(`${m.name} — optimized WebP folder`);
    const cbzLabel=JSON.stringify(`${m.name} — CBZ export folder`);
    return`<article class="m-card">
      <div class="m-cover">
        <img src="${coverUrl}" loading="lazy" alt="${esc(m.name)} cover"
             onerror="this.style.display='none';this.nextElementSibling.style.display='flex';">
        <div class="m-cover-ph" style="display:none;">${ICONS.book}</div>
        <div class="m-badges">
          ${m.has_opt?'<span class="m-badge mb-opt">Optimized</span>':''}
          ${m.has_cbz?'<span class="m-badge mb-cbz">CBZ Ready</span>':''}
        </div>
        <div class="m-overlay">
          <button class="m-overlay-btn" type="button" onclick="openReader(${nameArg})">${ICONS.read} Read manga</button>
        </div>
      </div>
      <div class="m-info">
        <div class="m-name" title="${esc(m.name)}">${esc(m.name)}</div>
        <div class="m-meta">${m.chapters} chapter${m.chapters!==1?'s':''}</div>
      </div>
      <div class="m-actions">
        ${m.has_cbz
          ?`<a class="btn btn-dl" href="/api/download/${encodeURIComponent(m.name)}" download>${ICONS.download} Download CBZ</a>`
          :`<button class="btn btn-ghost" disabled>No CBZ export yet</button>`}
        <button class="btn btn-ghost" type="button" onclick="quickOptimize(${nameArg})">${ICONS.bolt} Optimize manga</button>
        <div class="m-del-row">
          <button class="btn btn-del" type="button" title="Delete original manga folder" onclick="openDelModal(${sourceArg},${sourceLabel})">${ICONS.trash} Delete original <span class="del-small">source files</span></button>
          ${m.has_opt?`<button class="btn btn-del" type="button" title="Delete optimized WebP folder" onclick="openDelModal(${optArg},${optLabel})">${ICONS.trash} Delete optimized <span class="del-small">WebP folder</span></button>`:''}
          ${m.has_cbz?`<button class="btn btn-del" type="button" title="Delete CBZ export folder" onclick="openDelModal(${cbzArg},${cbzLabel})">${ICONS.trash} Delete CBZ export <span class="del-small">packed files</span></button>`:''}
        </div>
      </div>
    </article>`;
  }).join('');
}
document.getElementById('library-grid').addEventListener('click',e=>{
  const el=e.target.closest('[data-action]');
  if(!el)return;
  const action=el.dataset.action;
  if(action==='read')openReader(el.dataset.name);
  if(action==='optimize')quickOptimize(el.dataset.name);
  if(action==='delete')openDelModal(el.dataset.dir,el.dataset.label);
});

function quickOptimize(name){
  showPanel('optimize',document.getElementById('sb-optimize'));
  setTimeout(()=>refreshOptSel().then(()=>{document.getElementById('opt-manga').value=name;}),120);
}

function openDelModal(dirName,label){
  _delTarget={name:dirName,label};
  document.getElementById('del-target-label').textContent=label;
  document.getElementById('del-modal').classList.add('open');
}
function closeDelModal(){
  _delTarget=null;
  document.getElementById('del-modal').classList.remove('open');
}
document.getElementById('del-modal').addEventListener('click',e=>{if(e.target===e.currentTarget)closeDelModal();});
async function confirmDelete(){
  if(!_delTarget)return;
  const{name}=_delTarget;closeDelModal();
  try{
    const d=await fetch(`/api/delete/${encodeURIComponent(name)}`,{method:'DELETE'}).then(r=>r.json());
    if(d.ok){toast(`✔ Deleted: ${name}`,'var(--green)');refreshLibrary();refreshOptSel();}
    else toast(`✘ ${d.error}`,'var(--red)');
  }catch(e){toast('Delete failed','var(--red)');}
}

function handleReaderScroll(){
  const body=document.getElementById('reader-body');
  const header=document.querySelector('.reader-hd');
  const current=body.scrollTop;
  if(current>_readerLastScroll+12 && current>70)header.classList.add('hidden');
  else if(current<_readerLastScroll-12)header.classList.remove('hidden');
  _readerLastScroll=Math.max(current,0);
}
function showReaderChrome(){
  const header=document.querySelector('.reader-hd');
  header.classList.remove('hidden');
}

async function openReader(manga){
  _readerManga=manga;
  document.getElementById('reader').classList.add('open');
  document.body.style.overflow='hidden';
  _readerLastScroll=0;
  showReaderChrome();
  const readerBody=document.getElementById('reader-body');
  readerBody.removeEventListener('scroll',handleReaderScroll);
  readerBody.addEventListener('scroll',handleReaderScroll,{passive:true});
  try{screen.orientation.lock('portrait').catch(()=>{});}catch(e){}
  const r=await fetch(`/api/read/${encodeURIComponent(manga)}/chapters`);
  if(!r.ok){toast('No chapters found','var(--red)');closeReader();return;}
  const d=await r.json();
  _readerChs=d.chapters;_readerIdx=0;
  const sel=document.getElementById('reader-sel');
  sel.innerHTML=_readerChs.map((ch,i)=>`<option value="${i}">${esc(ch)}</option>`).join('');
  await loadReaderCh(0);
}

async function loadReaderCh(idx){
  _readerIdx=idx;
  const ch=_readerChs[idx];
  document.getElementById('reader-title').textContent=`${_readerManga}  ›  ${ch}`;
  document.getElementById('reader-sel').value=idx;
  document.getElementById('reader-prev').disabled=idx===0;
  document.getElementById('reader-next').disabled=idx===_readerChs.length-1;
  const pages=document.getElementById('reader-pages');
  const spin=document.getElementById('reader-spin');
  pages.innerHTML='';spin.style.display='block';
  const readerBody=document.getElementById('reader-body');
  readerBody.scrollTop=0;
  _readerLastScroll=0;
  showReaderChrome();
  const r=await fetch(`/api/read/${encodeURIComponent(_readerManga)}/${encodeURIComponent(ch)}/images`);
  if(!r.ok){spin.textContent='Failed to load chapter.';return;}
  const d=await r.json();
  spin.style.display='none';
  pages.innerHTML=d.images.map(img=>`<div class="rpage"><img src="/api/read/${encodeURIComponent(_readerManga)}/${encodeURIComponent(ch)}/img/${encodeURIComponent(img)}" loading="lazy" decoding="async" alt="${esc(img)}"></div>`).join('');
}

function closeReader(){
  document.getElementById('reader').classList.remove('open');
  document.getElementById('reader-body').removeEventListener('scroll',handleReaderScroll);
  showReaderChrome();
  document.body.style.overflow='';
  _readerManga=null;_readerChs=[];_readerIdx=0;
}
function readerPrev(){if(_readerIdx>0)loadReaderCh(_readerIdx-1);}
function readerNext(){if(_readerIdx<_readerChs.length-1)loadReaderCh(_readerIdx+1);}
function readerJump(v){loadReaderCh(parseInt(v));}

document.addEventListener('keydown',e=>{
  if(!document.getElementById('reader').classList.contains('open'))return;
  if(e.key==='ArrowRight'||e.key==='ArrowDown')readerNext();
  if(e.key==='ArrowLeft'||e.key==='ArrowUp')readerPrev();
  if(e.key==='Escape')closeReader();
});

refreshOptSel();
</script>
</body>
</html>"""


@app.get("/")
def index():
    return render_template_string(HTML)


# ─────────────────────────────────────────────────────────────────────────────
#  Entry point
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    for label, path in [
        ("SCRAPER_PATH", SCRAPER_PATH),
        ("OPTIMIZER_PATH", OPTIMIZER_PATH),
        ("BASE_DIR", BASE_DIR),
    ]:
        if not os.path.exists(path):
            print(f"  ⚠  {label} not found: {path}")

    os.makedirs(BASE_DIR, exist_ok=True)

    print(f"\n  Nebula Manga running → http://0.0.0.0:{PORT}\n")
    app.run(host="0.0.0.0", port=PORT, debug=False, threaded=True)
