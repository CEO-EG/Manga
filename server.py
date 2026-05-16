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
<title>Manga</title>
<link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'%3E%3Crect width='64' height='64' rx='14' fill='%23090b10'/%3E%3Cpath d='M16 14h23a9 9 0 0 1 9 9v27H25a9 9 0 0 1-9-9V14Z' fill='none' stroke='%23f5f5f4' stroke-width='4'/%3E%3Cpath d='M25 26h15M25 36h10' stroke='%23d6a84f' stroke-width='4' stroke-linecap='round'/%3E%3C/svg%3E">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&family=DM+Sans:ital,opsz,wght@0,9..40,300;0,9..40,400;0,9..40,500;0,9..40,600;1,9..40,300&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
<style>
:root{
  color-scheme:dark;
  --bg:#050607;
  --bg2:#0a0c0f;
  --panel:#111419;
  --panel2:#171b21;
  --panel3:#20252d;
  --line:#262c35;
  --line2:#38414d;
  --text:#f2f2ef;
  --muted:#a4abb5;
  --dim:#69717d;
  --accent:#d6a84f;
  --accent2:#f0c86a;
  --success:#58c783;
  --warning:#d9a441;
  --danger:#df6b6b;
  --shadow:0 22px 70px rgba(0,0,0,.48);
  --sidebar:260px;
  --mobile-nav:68px;
  --font:'DM Sans',system-ui,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
  --display:'DM Sans',system-ui,sans-serif;
  --mono:'JetBrains Mono',ui-monospace,SFMono-Regular,monospace;
}
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
html,body{min-height:100%;background:var(--bg);color:var(--text);font-family:var(--font);-webkit-font-smoothing:antialiased}
body{background:linear-gradient(180deg,#070809 0%,#050607 42%,#030405 100%)}
button,input,select{font:inherit}button,a{touch-action:manipulation}svg{display:block}
::-webkit-scrollbar{width:8px;height:8px}::-webkit-scrollbar-track{background:transparent}::-webkit-scrollbar-thumb{background:#2a3039;border-radius:999px}
.shell{display:grid;grid-template-columns:var(--sidebar) minmax(0,1fr);min-height:100vh}
.sidebar{position:sticky;top:0;height:100vh;padding:24px 18px;display:flex;flex-direction:column;gap:18px;background:#090b0e;border-right:1px solid var(--line);z-index:30}
.logo-wrap{padding:4px 4px 18px;border-bottom:1px solid var(--line)}
.logo-en{font-size:30px;font-weight:900;letter-spacing:-.8px;line-height:1;color:var(--text)}
.logo-sub{margin-top:8px;color:var(--dim);font:700 10px/1.5 var(--mono);letter-spacing:1.2px;text-transform:uppercase}
.flare-chip{border:1px solid var(--line);background:var(--panel);border-radius:14px;padding:11px 12px;display:flex;align-items:center;gap:10px;justify-content:space-between}
.flare-left{display:flex;align-items:center;gap:10px;min-width:0}.dot{width:8px;height:8px;border-radius:50%;background:var(--dim);flex:none}.dot.on{background:var(--success);box-shadow:0 0 18px rgba(88,199,131,.75)}.flare-name{font:700 11px/1 var(--mono);letter-spacing:.3px;color:var(--muted);white-space:nowrap}.flare-btn{border:1px solid var(--line2);border-radius:999px;padding:7px 11px;cursor:pointer;text-transform:uppercase;font:900 10px/1 var(--mono);letter-spacing:.8px;transition:.16s ease;background:#0c0f13;color:var(--muted)}.flare-btn.start{color:var(--success)}.flare-btn.stop{color:var(--danger)}.flare-btn:hover{border-color:var(--accent);color:var(--text)}
.nav{display:flex;flex-direction:column;gap:6px}.nav-label{font:800 10px/1 var(--mono);letter-spacing:1.6px;text-transform:uppercase;color:var(--dim);padding:8px 10px}.nav-item{position:relative;width:100%;border:1px solid transparent;background:transparent;color:var(--muted);border-radius:12px;padding:12px 11px;display:flex;align-items:center;gap:11px;cursor:pointer;text-align:left;font-weight:800;transition:.16s ease}.nav-item:hover{background:var(--panel);color:var(--text);border-color:var(--line)}.nav-item.active{color:var(--text);background:var(--panel2);border-color:var(--line2)}.nav-item.active::before{content:'';position:absolute;left:-1px;top:10px;bottom:10px;width:3px;border-radius:4px;background:var(--accent)}.nav-icon,.bn-ico{display:inline-grid;place-items:center;flex:0 0 auto}.nav-icon svg{width:20px;height:20px;stroke:currentColor;fill:none;stroke-width:2;stroke-linecap:round;stroke-linejoin:round}.nav-badge{margin-left:auto;display:none;min-width:20px;height:20px;place-items:center;border-radius:999px;background:var(--accent);color:#111;font:900 10px/20px var(--mono)}
.main{min-width:0;padding:36px clamp(18px,4vw,54px) 48px}.panel{display:none;animation:panelIn .18s ease-out}.panel.active{display:block}@keyframes panelIn{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:none}}
.pg-title{font-size:38px;font-weight:900;letter-spacing:-1px;line-height:1;color:var(--text)}.pg-sub{margin:8px 0 26px;color:var(--dim);font:700 12px/1.6 var(--mono);letter-spacing:.5px;text-transform:uppercase}.pg-sub::before{content:'— ';color:var(--accent)}
.card{background:var(--panel);border:1px solid var(--line);border-radius:18px;padding:22px;margin-bottom:16px;box-shadow:0 12px 36px rgba(0,0,0,.22)}.card-hd{display:flex;align-items:center;gap:10px;margin-bottom:18px;color:var(--muted);font:900 12px/1 var(--mono);letter-spacing:1.3px;text-transform:uppercase}.card-hd::before{content:'';width:18px;height:2px;border-radius:999px;background:var(--accent)}
.field{margin-bottom:14px}.field label{display:block;margin-bottom:8px;color:var(--muted);font:900 11px/1 var(--mono);text-transform:uppercase;letter-spacing:.8px}.field input,.field select{width:100%;border:1px solid var(--line);border-radius:12px;background:#090b0e;color:var(--text);padding:13px 14px;outline:none;transition:.16s ease;font:600 13px/1.2 var(--mono)}.field input:focus,.field select:focus{border-color:var(--accent);box-shadow:0 0 0 3px rgba(214,168,79,.14)}.field input::placeholder{color:var(--dim)}.row2{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}.checks{display:flex;gap:10px;flex-wrap:wrap;margin:6px 0 18px}.chk{display:flex;align-items:center;gap:9px;color:var(--muted);font-weight:800;font-size:13px;background:#0c0f13;border:1px solid var(--line);border-radius:999px;padding:9px 12px;cursor:pointer}.chk input{accent-color:var(--accent);width:15px;height:15px}.chk:hover{color:var(--text);border-color:var(--line2)}
.btn{display:inline-flex;align-items:center;justify-content:center;gap:8px;border:1px solid transparent;border-radius:12px;padding:10px 16px;cursor:pointer;font-weight:900;font-size:13px;letter-spacing:.1px;transition:.16s ease;text-decoration:none;white-space:nowrap}.btn svg,.m-overlay-btn svg,.reader-close svg,.rnav-btn svg{width:17px;height:17px;stroke:currentColor;fill:none;stroke-width:2;stroke-linecap:round;stroke-linejoin:round}.btn:hover{transform:translateY(-1px)}.btn:disabled{opacity:.38;cursor:not-allowed;transform:none!important}.btn-red{color:#111;background:var(--accent);border-color:var(--accent)}.btn-red:hover{background:var(--accent2)}.btn-ghost{color:var(--muted);background:#0c0f13;border-color:var(--line)}.btn-ghost:hover{color:var(--text);border-color:var(--line2);background:#12161b}.btn-read{color:#111;background:var(--accent);border-color:var(--accent)}.btn-dl{color:#07150d;background:var(--success);border-color:rgba(88,199,131,.25)}.btn-del{color:var(--danger);background:rgba(223,107,107,.08);border-color:rgba(223,107,107,.22);font-size:12px;padding:9px 12px}.btn-del:hover{background:rgba(223,107,107,.13);border-color:rgba(223,107,107,.45)}
.lib-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(205px,1fr));gap:20px}.m-card{position:relative;overflow:hidden;border-radius:16px;background:var(--panel);border:1px solid var(--line);box-shadow:0 12px 34px rgba(0,0,0,.28);transition:.18s ease}.m-card:hover{transform:translateY(-4px);border-color:var(--line2);box-shadow:0 22px 56px rgba(0,0,0,.42)}.m-cover{position:relative;aspect-ratio:2/3;background:#0b0d10;overflow:hidden}.m-cover::after{content:'';position:absolute;inset:50% 0 0;background:linear-gradient(to top,rgba(5,6,7,.95),transparent);pointer-events:none}.m-cover img{width:100%;height:100%;object-fit:cover;display:block;transition:transform .35s ease,filter .35s ease}.m-card:hover .m-cover img{transform:scale(1.035);filter:saturate(1.03)}.m-cover-ph{width:100%;height:100%;display:flex;align-items:center;justify-content:center;color:rgba(255,255,255,.4)}.m-cover-ph svg{width:70px;height:70px;stroke:currentColor;fill:none;stroke-width:1.4}.m-badges{position:absolute;top:10px;left:10px;right:10px;display:flex;gap:6px;flex-wrap:wrap;z-index:2}.m-badge{font:900 9px/1 var(--mono);letter-spacing:.7px;text-transform:uppercase;padding:6px 8px;border-radius:999px;backdrop-filter:blur(10px);border:1px solid rgba(255,255,255,.14)}.mb-cbz{background:rgba(88,199,131,.88);color:#07150d}.mb-opt{background:rgba(214,168,79,.9);color:#151006}.m-overlay{position:absolute;left:10px;right:10px;bottom:10px;z-index:3;opacity:0;transform:translateY(8px);transition:.18s ease}.m-card:hover .m-overlay{opacity:1;transform:none}.m-overlay-btn{width:100%;display:flex;align-items:center;justify-content:center;gap:8px;border:1px solid rgba(255,255,255,.1);border-radius:12px;padding:11px 14px;color:#111;background:var(--accent);font-weight:1000;cursor:pointer}.m-overlay-btn:hover{background:var(--accent2)}.m-info{padding:14px 14px 9px}.m-name{font-size:15px;font-weight:1000;line-height:1.25;min-height:38px;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}.m-meta{margin-top:8px;color:var(--dim);font:800 11px/1 var(--mono);letter-spacing:.4px;text-transform:uppercase}.m-actions{display:grid;gap:8px;padding:0 14px 14px}.m-actions .btn{width:100%;padding:10px 12px;font-size:12px}.m-del-row{display:grid;grid-template-columns:1fr;gap:7px}.m-del-row .btn{justify-content:flex-start;border-radius:12px;font-size:11px;white-space:normal;text-align:left;line-height:1.25}.m-del-row .btn .del-small{display:block;color:var(--dim);font:800 9px/1.35 var(--mono);text-transform:uppercase;letter-spacing:.4px;margin-left:auto}
.log-top{display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:14px}.logbox{height:330px;overflow:auto;padding:16px;border-radius:14px;border:1px solid var(--line);background:#050607;color:var(--dim);font:600 12px/1.85 var(--mono);white-space:pre-wrap;word-break:break-word}.l-ok{color:var(--success)}.l-err{color:var(--danger)}.l-warn{color:var(--warning)}.l-dim{color:#858e9a}
.job-row{display:grid;grid-template-columns:auto minmax(0,1fr) auto auto auto;align-items:center;gap:10px;padding:13px 0;border-bottom:1px solid var(--line)}.job-row:last-child{border-bottom:0}.jt{border-radius:999px;padding:7px 10px;font:900 10px/1 var(--mono);letter-spacing:.8px;text-transform:uppercase}.jt-scrape{background:rgba(214,168,79,.13);color:var(--accent2)}.jt-optimize{background:rgba(164,171,181,.12);color:var(--muted)}.ji,.jtime{color:var(--dim);font:700 11px/1 var(--mono);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.jst{font:900 11px/1 var(--mono);text-transform:uppercase}.jst.running{color:var(--warning);animation:pulse 1.4s infinite}.jst.done{color:var(--success)}.jst.failed{color:var(--danger)}@keyframes pulse{50%{opacity:.35}}
.modal-bg{display:none;position:fixed;inset:0;z-index:400;padding:20px;align-items:center;justify-content:center;background:rgba(0,0,0,.76);backdrop-filter:blur(10px)}.modal-bg.open{display:flex}.modal{max-width:430px;width:100%;padding:24px;border-radius:18px;background:var(--panel);border:1px solid rgba(223,107,107,.28);box-shadow:var(--shadow)}.modal-title{font-size:26px;font-weight:1000;color:var(--danger);line-height:1}.modal-body{margin:16px 0 24px;color:var(--muted);font-size:14px;line-height:1.7}.modal-body strong{display:inline-block;margin-top:6px;color:var(--text);font-family:var(--mono);word-break:break-word}.modal-acts{display:flex;justify-content:flex-end;gap:10px;flex-wrap:wrap}
.reader{display:none;position:fixed;inset:0;z-index:500;background:#000;flex-direction:column}.reader.open{display:flex}.reader-hd{position:absolute;left:0;right:0;top:0;z-index:5;display:grid;grid-template-columns:auto minmax(0,1fr) auto;align-items:center;gap:12px;padding:10px 14px;border-bottom:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(0,0,0,.82),rgba(0,0,0,.55));backdrop-filter:blur(12px);transition:transform .22s ease,opacity .22s ease}.reader.controls-hidden .reader-hd{transform:translateY(-110%);opacity:0;pointer-events:none}.reader-title{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-weight:900;font-size:13px;color:#e7e5e4}.reader-close,.rnav-btn{display:inline-flex;align-items:center;justify-content:center;gap:7px;border:1px solid rgba(255,255,255,.13);border-radius:10px;color:#d6d3d1;background:rgba(20,20,20,.72);padding:9px 12px;cursor:pointer;font-weight:900;font-size:12px;transition:.16s ease}.reader-close:hover,.rnav-btn:hover{color:#fff;border-color:rgba(255,255,255,.28);background:rgba(35,35,35,.84)}.rnav-btn:disabled{opacity:.25;cursor:not-allowed}.reader-nav{display:flex;align-items:center;gap:8px}.rch-sel{max-width:210px;border:1px solid rgba(255,255,255,.13);border-radius:10px;background:rgba(8,8,8,.88);color:#f5f5f4;padding:9px 12px;outline:0;font:800 12px/1 var(--mono)}.reader-body{height:100vh;overflow:auto;display:block;background:#000;-webkit-overflow-scrolling:touch;scrollbar-width:thin}.reader-pages{width:100%;display:flex;flex-direction:column;align-items:center;padding-top:0}.rpage{width:100%;max-width:min(900px,100vw);margin:0 auto;background:#000}.rpage img{display:block;width:100%;height:auto;margin:0 auto}.rspin{padding:42vh 20px;text-align:center;color:var(--dim);font:800 13px/1 var(--mono)}
.bot-nav{display:none;position:fixed;left:12px;right:12px;bottom:12px;z-index:200;padding-bottom:env(safe-area-inset-bottom)}.bot-nav-inner{height:60px;display:grid;grid-template-columns:repeat(4,1fr);gap:4px;padding:6px;border:1px solid var(--line);border-radius:18px;background:rgba(9,11,14,.9);backdrop-filter:blur(16px);box-shadow:var(--shadow)}.bn{position:relative;border:0;background:transparent;color:var(--muted);border-radius:13px;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:3px;font-size:10px;font-weight:1000;cursor:pointer}.bn.active{color:#111;background:var(--accent)}.bn svg{width:20px;height:20px;stroke:currentColor;fill:none;stroke-width:2;stroke-linecap:round;stroke-linejoin:round}.bn-bdg{position:absolute;top:5px;right:20%;display:none;min-width:18px;height:18px;border-radius:999px;background:var(--danger);color:white;font:900 9px/18px var(--mono)}
#toast{position:fixed;right:18px;bottom:18px;z-index:600;max-width:320px;opacity:0;transform:translateY(8px);pointer-events:none;transition:.2s ease;padding:12px 15px;border-radius:13px;background:var(--panel2);border:1px solid var(--line2);box-shadow:var(--shadow);font:800 12px/1.4 var(--mono);color:var(--text)}#toast.show{opacity:1;transform:none}.empty{grid-column:1/-1;color:var(--dim);font:800 13px/1.6 var(--mono);text-align:center;padding:58px 18px;border:1px dashed var(--line);border-radius:16px;background:rgba(255,255,255,.02)}
@media(max-width:900px){.shell{grid-template-columns:1fr}.sidebar{display:none}.main{padding:22px 14px calc(var(--mobile-nav) + 34px)}.bot-nav{display:block}.row2{grid-template-columns:1fr}.lib-grid{grid-template-columns:repeat(auto-fill,minmax(156px,1fr));gap:14px}.m-card{border-radius:14px}.m-info{padding:12px 12px 8px}.m-actions{padding:0 12px 12px}.m-overlay{opacity:1;transform:none}.m-del-row .btn .del-small{display:none}.job-row{grid-template-columns:auto minmax(0,1fr) auto}.jtime{display:none}.job-row .btn{grid-column:1/-1}.logbox{height:260px}#toast{bottom:calc(var(--mobile-nav) + 30px);left:14px;right:14px;max-width:none}.reader-hd{position:absolute;top:0;left:0;right:0;grid-template-columns:1fr;gap:8px;padding:9px 10px;background:linear-gradient(180deg,rgba(0,0,0,.86),rgba(0,0,0,.45));border-bottom:1px solid rgba(255,255,255,.08)}.reader-title{text-align:center;font-size:12px;padding:0 52px}.reader-close{position:absolute;left:10px;top:8px;width:40px;height:40px;padding:0;font-size:0}.reader-close svg{width:20px;height:20px}.reader-nav{width:100%;display:grid;grid-template-columns:42px minmax(0,1fr) 42px;gap:8px}.rnav-btn{width:42px;height:40px;padding:0;border-radius:10px;font-size:0}.rnav-btn svg{width:20px;height:20px}.rch-sel{max-width:100%;width:100%;height:40px;text-align:center}.reader-body{height:100vh}.rpage{max-width:100vw}.pg-title{font-size:34px}}
@media(max-width:420px){.lib-grid{grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}.m-name{font-size:13px;min-height:34px}.m-actions .btn{font-size:11px}.btn{padding:10px 13px}.card{padding:18px;border-radius:16px}.pg-title{font-size:32px}}
</style>
</head>
<body>
<div class="shell">

<!-- Sidebar -->
<aside class="sidebar">
  <div class="logo-wrap">
    <div class="logo-en">Manga</div>
    <div class="logo-sub">library · reader · optimizer</div>
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
      <button class="btn btn-read"  onclick="confirmDelete()">Delete</button>
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
let _readerManga=null,_readerChs=[],_readerIdx=0,_readerLastScroll=0,_readerChromeTimer=null;
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
function jsArg(v){return JSON.stringify(v).replace(/&/g,'&amp;').replace(/'/g,'&#39;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}

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
  if(!url){toast('Enter a URL first','var(--danger)');return;}
  const d=await fetch('/api/scrape',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({url,start:start||null,end:end||null})}).then(r=>r.json());
  if(d.error){toast(d.error,'var(--danger)');return;}
  currentScrapeJob=d.job_id;
  document.getElementById('scrape-log-card').style.display='block';
  document.getElementById('scrape-cancel-btn').disabled=false;
  document.getElementById('scrape-log').innerHTML='';
  toast('Scrape started');
  startLogPoll(d.job_id,'scrape-log','scrape-cancel-btn',s=>{
    toast(s==='done'?'✔ Scrape complete':'✘ Scrape failed',s==='done'?'var(--success)':'var(--danger)');
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
  if(!manga){toast('Select a manga first','var(--danger)');return;}
  const d=await fetch('/api/optimize',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({manga,quality,max_width,cbz,cbz_only,delete_orig:del_orig})}).then(r=>r.json());
  if(d.error){toast(d.error,'var(--danger)');return;}
  currentOptJob=d.job_id;
  document.getElementById('opt-log-card').style.display='block';
  document.getElementById('opt-cancel-btn').disabled=false;
  document.getElementById('opt-log').innerHTML='';
  toast('Optimize started');
  startLogPoll(d.job_id,'opt-log','opt-cancel-btn',s=>{
    toast(s==='done'?'✔ Optimize complete':'✘ Optimize failed',s==='done'?'var(--success)':'var(--danger)');
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
    const nameArg=jsArg(m.name);
    const sourceArg=jsArg(m.name);
    const optArg=jsArg(m.name+'-optimized');
    const cbzArg=jsArg(m.name+'-cbz');
    const sourceLabel=jsArg(`${m.name} — original manga folder`);
    const optLabel=jsArg(`${m.name} — optimized WebP folder`);
    const cbzLabel=jsArg(`${m.name} — CBZ export folder`);
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
          <button class="m-overlay-btn" type="button" onclick='openReader(${nameArg})'>${ICONS.read} Read manga</button>
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
        <button class="btn btn-ghost" type="button" onclick='quickOptimize(${nameArg})'>${ICONS.bolt} Optimize manga</button>
        <div class="m-del-row">
          <button class="btn btn-del" type="button" title="Delete original manga folder" onclick='openDelModal(${sourceArg},${sourceLabel})'>${ICONS.trash} Delete original <span class="del-small">source files</span></button>
          ${m.has_opt?`<button class="btn btn-del" type="button" title="Delete optimized WebP folder" onclick='openDelModal(${optArg},${optLabel})'>${ICONS.trash} Delete optimized <span class="del-small">WebP folder</span></button>`:''}
          ${m.has_cbz?`<button class="btn btn-del" type="button" title="Delete CBZ export folder" onclick='openDelModal(${cbzArg},${cbzLabel})'>${ICONS.trash} Delete CBZ export <span class="del-small">packed files</span></button>`:''}
        </div>
      </div>
    </article>`;
  }).join('');
}

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
    if(d.ok){toast(`✔ Deleted: ${name}`,'var(--success)');refreshLibrary();refreshOptSel();}
    else toast(`✘ ${d.error}`,'var(--danger)');
  }catch(e){toast('Delete failed','var(--danger)');}
}

function showReaderControls(){
  const reader=document.getElementById('reader');
  reader.classList.remove('controls-hidden');
  clearTimeout(_readerChromeTimer);
  _readerChromeTimer=setTimeout(()=>{
    const body=document.getElementById('reader-body');
    if(reader.classList.contains('open') && body.scrollTop>120)reader.classList.add('controls-hidden');
  },1800);
}
function handleReaderScroll(){
  const body=document.getElementById('reader-body');
  const y=body.scrollTop;
  const reader=document.getElementById('reader');
  if(y<40 || y<_readerLastScroll-8)showReaderControls();
  else if(y>_readerLastScroll+8 && y>120)reader.classList.add('controls-hidden');
  _readerLastScroll=Math.max(0,y);
}

document.getElementById('reader-body').addEventListener('scroll',handleReaderScroll,{passive:true});

async function openReader(manga){
  _readerManga=manga;
  const reader=document.getElementById('reader');
  reader.classList.add('open');
  reader.classList.remove('controls-hidden');
  _readerLastScroll=0;
  document.body.style.overflow='hidden';
  try{screen.orientation.lock('portrait').catch(()=>{});}catch(e){}
  const r=await fetch(`/api/read/${encodeURIComponent(manga)}/chapters`);
  if(!r.ok){toast('No chapters found','var(--danger)');closeReader();return;}
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
  showReaderControls();
  const r=await fetch(`/api/read/${encodeURIComponent(_readerManga)}/${encodeURIComponent(ch)}/images`);
  if(!r.ok){spin.textContent='Failed to load chapter.';return;}
  const d=await r.json();
  spin.style.display='none';
  pages.innerHTML=d.images.map(img=>`<div class="rpage"><img src="/api/read/${encodeURIComponent(_readerManga)}/${encodeURIComponent(ch)}/img/${encodeURIComponent(img)}" loading="lazy" decoding="async" alt="${esc(img)}"></div>`).join('');
}

function closeReader(){
  const reader=document.getElementById('reader');
  reader.classList.remove('open','controls-hidden');
  clearTimeout(_readerChromeTimer);
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

    print(f"\n  Manga running → http://0.0.0.0:{PORT}\n")
    app.run(host="0.0.0.0", port=PORT, debug=False, threaded=True)
