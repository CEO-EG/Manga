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
<title>墨 Manga Vault</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&family=DM+Sans:ital,opsz,wght@0,9..40,300;0,9..40,400;0,9..40,500;0,9..40,600;1,9..40,300&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
<style>
:root {
  --ink:       #0b0b0f;
  --ink2:      #111118;
  --ink3:      #191921;
  --border:    #22222e;
  --border2:   #2e2e3e;
  --red:       #e8294a;
  --red-dim:   rgba(232,41,74,0.10);
  --red-glow:  rgba(232,41,74,0.22);
  --gold:      #f5c842;
  --gold-dim:  rgba(245,200,66,0.10);
  --green:     #2ec77a;
  --green-dim: rgba(46,199,122,0.10);
  --blue:      #4ea8ff;
  --text:      #eaeaf2;
  --text2:     #7878a0;
  --text3:     #44445a;
  --font-d:    'Bebas Neue', sans-serif;
  --font:      'DM Sans', sans-serif;
  --mono:      'JetBrains Mono', monospace;
  --r:         10px;
  --rs:        6px;
  --sw:        228px;
  --nh:        60px;
}
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
html,body{height:100%;background:var(--ink);color:var(--text);font-family:var(--font);-webkit-font-smoothing:antialiased}
::-webkit-scrollbar{width:4px;height:4px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:var(--border2);border-radius:4px}

/* Shell */
.shell{display:grid;grid-template-columns:var(--sw) 1fr;min-height:100vh}

/* Sidebar */
.sidebar{background:var(--ink2);border-right:1px solid var(--border);display:flex;flex-direction:column;position:sticky;top:0;height:100vh;overflow-y:auto}
.logo-wrap{padding:26px 20px 20px;border-bottom:1px solid var(--border)}
.logo-k{font-family:var(--font-d);font-size:30px;color:var(--red);letter-spacing:2px;line-height:1}
.logo-en{font-family:var(--font-d);font-size:19px;letter-spacing:3px;color:var(--text);margin-top:1px}
.logo-sub{font-family:var(--mono);font-size:9px;color:var(--text3);letter-spacing:1.5px;margin-top:5px;text-transform:uppercase}

/* FlareSolverr chip */
.flare-chip{margin:14px 12px;background:var(--ink3);border:1px solid var(--border);border-radius:var(--rs);padding:9px 12px;display:flex;align-items:center;justify-content:space-between;gap:8px}
.flare-left{display:flex;align-items:center;gap:8px}
.dot{width:7px;height:7px;border-radius:50%;flex-shrink:0;transition:all .3s}
.dot.on{background:var(--green);box-shadow:0 0 8px var(--green)}
.dot.off{background:var(--text3)}
.flare-name{font-family:var(--mono);font-size:10px;color:var(--text2)}
.flare-btn{font-family:var(--mono);font-size:9px;font-weight:600;letter-spacing:.5px;text-transform:uppercase;padding:4px 10px;border-radius:4px;border:none;cursor:pointer;transition:all .15s}
.flare-btn.start{background:var(--green-dim);color:var(--green);border:1px solid rgba(46,199,122,.2)}
.flare-btn.stop{background:var(--red-dim);color:var(--red);border:1px solid rgba(232,41,74,.2)}
.flare-btn:hover{filter:brightness(1.15)}

/* Nav */
.nav{padding:8px 10px;flex:1}
.nav-label{font-size:9px;font-weight:600;letter-spacing:2px;color:var(--text3);text-transform:uppercase;padding:0 10px;margin:10px 0 6px}
.nav-item{display:flex;align-items:center;gap:10px;padding:9px 12px;border-radius:var(--rs);cursor:pointer;border:none;background:none;color:var(--text2);font-family:var(--font);font-size:13px;font-weight:500;width:100%;text-align:left;transition:all .15s;position:relative}
.nav-item:hover{background:var(--ink3);color:var(--text)}
.nav-item.active{background:var(--red-dim);color:var(--red)}
.nav-item.active::before{content:'';position:absolute;left:0;top:20%;bottom:20%;width:3px;background:var(--red);border-radius:0 3px 3px 0}
.nav-icon{font-size:16px;flex-shrink:0}
.nav-badge{margin-left:auto;background:rgba(245,200,66,.18);color:var(--gold);font-size:9px;font-weight:700;font-family:var(--mono);padding:2px 6px;border-radius:10px;display:none}

/* Main */
.main{padding:34px 36px;min-width:0}

/* Bottom nav */
.bot-nav{display:none;position:fixed;bottom:0;left:0;right:0;height:var(--nh);background:var(--ink2);border-top:1px solid var(--border);z-index:200;padding-bottom:env(safe-area-inset-bottom)}
.bot-nav-inner{display:flex;height:100%}
.bn{flex:1;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:3px;border:none;background:none;cursor:pointer;color:var(--text3);font-family:var(--font);font-size:10px;font-weight:600;letter-spacing:.3px;transition:color .15s;position:relative}
.bn.active{color:var(--red)}
.bn-ico{font-size:19px}
.bn-bdg{position:absolute;top:8px;right:calc(50% - 18px);background:var(--gold);color:#1a1200;font-size:8px;font-weight:800;padding:1px 5px;border-radius:8px;display:none}

/* Panels */
.panel{display:none;animation:panelIn .18s ease-out}
.panel.active{display:block}
@keyframes panelIn{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:none}}

/* Page heading */
.pg-title{font-family:var(--font-d);font-size:36px;letter-spacing:2px;color:var(--text);line-height:1}
.pg-sub{font-size:11px;color:var(--text3);margin-top:5px;font-family:var(--mono);margin-bottom:26px}

/* Cards */
.card{background:var(--ink2);border:1px solid var(--border);border-radius:var(--r);padding:20px;margin-bottom:14px}
.card-hd{font-family:var(--mono);font-size:9px;font-weight:600;letter-spacing:2px;text-transform:uppercase;color:var(--text3);margin-bottom:16px;display:flex;align-items:center;gap:8px}
.card-hd::before{content:'';display:block;width:16px;height:2px;background:var(--red);border-radius:1px;flex-shrink:0}

/* Forms */
.field{margin-bottom:12px}
.field label{display:block;font-size:9px;font-weight:600;letter-spacing:1px;text-transform:uppercase;color:var(--text3);margin-bottom:5px;font-family:var(--mono)}
.field input,.field select{width:100%;background:var(--ink);border:1px solid var(--border);border-radius:var(--rs);color:var(--text);font-family:var(--mono);font-size:12px;padding:9px 11px;outline:none;transition:border-color .15s,box-shadow .15s}
.field input:focus,.field select:focus{border-color:var(--red);box-shadow:0 0 0 3px var(--red-dim)}
.field input::placeholder{color:var(--text3)}
.row2{display:flex;gap:12px}
.row2 .field{flex:1}
.checks{display:flex;gap:18px;flex-wrap:wrap;margin-bottom:16px;margin-top:2px}
.chk{display:flex;align-items:center;gap:7px;cursor:pointer;font-size:12px;color:var(--text2);font-weight:500;user-select:none}
.chk input[type=checkbox]{accent-color:var(--red);width:13px;height:13px}
.chk:hover{color:var(--text)}

/* Buttons */
.btn{display:inline-flex;align-items:center;justify-content:center;gap:6px;padding:9px 18px;border-radius:var(--rs);border:none;cursor:pointer;font-family:var(--font);font-size:12px;font-weight:600;letter-spacing:.2px;transition:all .15s;white-space:nowrap}
.btn-red{background:var(--red);color:#fff}
.btn-red:hover{opacity:.85;transform:translateY(-1px)}
.btn-ghost{background:var(--ink3);color:var(--text2);border:1px solid var(--border)}
.btn-ghost:hover{border-color:var(--border2);color:var(--text)}
.btn-read{background:var(--red-dim);color:var(--red);border:1px solid rgba(232,41,74,.22)}
.btn-read:hover{background:rgba(232,41,74,.17)}
.btn-dl{background:var(--green-dim);color:var(--green);border:1px solid rgba(46,199,122,.2);text-decoration:none}
.btn-dl:hover{background:rgba(46,199,122,.17)}
.btn-del{background:transparent;color:var(--text3);border:1px solid var(--border);font-size:11px;padding:6px 10px}
.btn-del:hover{border-color:var(--red);color:var(--red);background:var(--red-dim)}
.btn:disabled{opacity:.3;cursor:not-allowed;transform:none!important}

/* Library grid */
.lib-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(152px,1fr));gap:16px}

/* Manga card */
.m-card{background:var(--ink2);border:1px solid var(--border);border-radius:var(--r);overflow:hidden;display:flex;flex-direction:column;transition:border-color .2s,transform .2s}
.m-card:hover{border-color:var(--border2);transform:translateY(-4px)}

/* Cover */
.m-cover{position:relative;width:100%;aspect-ratio:2/3;background:var(--ink3);overflow:hidden;flex-shrink:0}
.m-cover img{width:100%;height:100%;object-fit:cover;display:block;transition:transform .35s}
.m-card:hover .m-cover img{transform:scale(1.05)}
.m-cover-ph{width:100%;height:100%;display:flex;align-items:center;justify-content:center;font-size:40px;color:var(--text3);font-family:var(--font-d);letter-spacing:2px}
.m-badges{position:absolute;top:7px;left:7px;display:flex;flex-direction:column;gap:4px}
.m-badge{font-family:var(--mono);font-size:8px;font-weight:600;letter-spacing:.8px;text-transform:uppercase;padding:3px 7px;border-radius:4px;backdrop-filter:blur(6px)}
.mb-cbz{background:rgba(46,199,122,.82);color:#00250e}
.mb-opt{background:rgba(245,200,66,.82);color:#251a00}
.m-overlay{position:absolute;inset:0;background:rgba(11,11,15,.72);display:flex;align-items:center;justify-content:center;opacity:0;transition:opacity .22s}
.m-card:hover .m-overlay{opacity:1}
.m-overlay-btn{background:var(--red);color:#fff;border:none;cursor:pointer;font-family:var(--font);font-size:12px;font-weight:600;padding:8px 20px;border-radius:var(--rs)}

/* Info */
.m-info{padding:9px 10px 5px;flex:1;display:flex;flex-direction:column;gap:3px}
.m-name{font-size:12px;font-weight:600;line-height:1.35;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
.m-meta{font-family:var(--mono);font-size:10px;color:var(--text3)}

/* Actions */
.m-actions{padding:7px 8px 9px;display:flex;flex-direction:column;gap:5px}
.m-actions .btn{font-size:11px;padding:7px 8px;width:100%}
.m-del-row{display:flex;gap:4px}
.m-del-row .btn{flex:1;padding:5px 4px;font-size:10px}

/* Log box */
.logbox{background:#050507;border:1px solid var(--border);border-radius:var(--rs);font-family:var(--mono);font-size:11px;padding:12px 14px;height:310px;overflow-y:auto;line-height:1.8;white-space:pre-wrap;word-break:break-all}
.l-ok{color:var(--green);font-weight:600}
.l-err{color:var(--red);font-weight:600}
.l-warn{color:var(--gold)}
.l-dim{color:var(--text3)}

/* Jobs */
.job-row{display:flex;align-items:center;flex-wrap:wrap;gap:8px;padding:11px 0;border-bottom:1px solid var(--border)}
.job-row:last-child{border-bottom:none}
.jt{font-family:var(--mono);font-size:8px;font-weight:600;letter-spacing:.8px;text-transform:uppercase;padding:3px 8px;border-radius:4px}
.jt-scrape{background:var(--red-dim);color:var(--red)}
.jt-optimize{background:var(--gold-dim);color:var(--gold)}
.ji{font-family:var(--mono);font-size:10px;color:var(--text3);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;min-width:0}
.jtime{font-family:var(--mono);font-size:10px;color:var(--text3)}
.jst{font-family:var(--mono);font-size:11px;font-weight:600;margin-left:auto}
.jst.running{color:var(--gold);animation:pulse 1.4s ease-in-out infinite}
.jst.done{color:var(--green)}
.jst.failed{color:var(--red)}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}

/* Delete modal */
.modal-bg{display:none;position:fixed;inset:0;background:rgba(8,8,12,.87);z-index:400;align-items:center;justify-content:center;padding:20px}
.modal-bg.open{display:flex}
.modal{background:var(--ink2);border:1px solid var(--border2);border-radius:var(--r);padding:26px;max-width:350px;width:100%}
.modal-title{font-family:var(--font-d);font-size:24px;letter-spacing:2px;color:var(--red);margin-bottom:10px}
.modal-body{font-size:13px;color:var(--text2);line-height:1.7;margin-bottom:22px}
.modal-body strong{color:var(--text);font-family:var(--mono)}
.modal-acts{display:flex;gap:10px;justify-content:flex-end}

/* Reader */
.reader{display:none;position:fixed;inset:0;background:#060608;z-index:500;flex-direction:column}
.reader.open{display:flex}
.reader-hd{background:rgba(6,6,8,.95);backdrop-filter:blur(12px);border-bottom:1px solid var(--border);padding:9px 14px;display:flex;align-items:center;flex-wrap:wrap;gap:8px;position:sticky;top:0;z-index:10;flex-shrink:0}
.reader-title{font-size:12px;font-weight:600;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;min-width:100px}
.reader-close{background:var(--ink3);border:1px solid var(--border);border-radius:var(--rs);color:var(--text2);cursor:pointer;padding:5px 13px;font-family:var(--font);font-size:12px;font-weight:600;transition:all .15s;flex-shrink:0}
.reader-close:hover{color:var(--red);border-color:var(--red)}
.reader-nav{display:flex;align-items:center;gap:6px;flex-shrink:0}
.rnav-btn{background:var(--ink3);border:1px solid var(--border);border-radius:var(--rs);color:var(--text2);cursor:pointer;padding:5px 11px;font-family:var(--font);font-size:12px;font-weight:600;transition:all .15s;white-space:nowrap}
.rnav-btn:hover{border-color:var(--border2);color:var(--text)}
.rnav-btn:disabled{opacity:.25;cursor:not-allowed}
.rch-sel{background:var(--ink3);border:1px solid var(--border);border-radius:var(--rs);color:var(--text);padding:5px 8px;font-family:var(--mono);font-size:11px;outline:none;cursor:pointer;max-width:140px}
.reader-body{flex:1;overflow-y:auto;overflow-x:hidden;display:flex;flex-direction:column;align-items:center;background:#080809;-webkit-overflow-scrolling:touch}
.rpage{width:100%;max-width:820px}
.rpage img{width:100%;display:block;margin-bottom:2px}
.rspin{color:var(--text3);font-family:var(--mono);font-size:12px;text-align:center;padding:80px 0}

/* Toast */
#toast{position:fixed;bottom:calc(var(--nh) + 14px);right:18px;background:var(--ink3);border:1px solid var(--border2);border-radius:var(--rs);padding:10px 18px;font-size:11px;font-family:var(--mono);color:var(--text);opacity:0;transition:all .2s;transform:translateY(6px);pointer-events:none;z-index:600;max-width:280px;box-shadow:0 10px 30px rgba(0,0,0,.6)}
#toast.show{opacity:1;transform:translateY(0)}

/* Misc */
.empty{color:var(--text3);font-family:var(--mono);font-size:12px;text-align:center;padding:50px 0}
.log-top{display:flex;align-items:center;justify-content:space-between;margin-bottom:12px}

/* Responsive */
@media(max-width:800px){
  .shell{grid-template-columns:1fr}
  .sidebar{display:none}
  .main{padding:18px 14px calc(var(--nh) + 18px)}
  .bot-nav{display:block}
  #toast{bottom:calc(var(--nh)+10px);right:12px}
  .pg-title{font-size:28px}
  .logbox{height:240px}
  .lib-grid{grid-template-columns:repeat(auto-fill,minmax(128px,1fr));gap:10px}
  .card{padding:14px}
  .row2{flex-direction:column;gap:0}
}
@media(max-width:380px){
  .lib-grid{grid-template-columns:repeat(2,1fr)}
}
</style>
</head>
<body>
<div class="shell">

<!-- Sidebar -->
<aside class="sidebar">
  <div class="logo-wrap">
    <div class="logo-k">墨</div>
    <div class="logo-en">MANGA VAULT</div>
    <div class="logo-sub">scraper · optimizer · reader</div>
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
    <button class="nav-item active" id="sb-scrape"   onclick="showPanel('scrape',this)">  <span class="nav-icon">📡</span> Scrape</button>
    <button class="nav-item"        id="sb-library"  onclick="showPanel('library',this)"> <span class="nav-icon">📚</span> Library</button>
    <button class="nav-item"        id="sb-optimize" onclick="showPanel('optimize',this)"><span class="nav-icon">⚡</span> Optimize</button>
    <button class="nav-item"        id="sb-jobs"     onclick="showPanel('jobs',this)">
      <span class="nav-icon">🔧</span> Jobs
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
      <button class="btn btn-red" onclick="startScrape()">▶ &nbsp;Start Scrape</button>
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
      <button class="btn btn-red" onclick="startOptimize()">⚡ &nbsp;Start Optimize</button>
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
    <button class="bn active" id="bn-scrape"   onclick="showPanel('scrape',this,true)">  <span class="bn-ico">📡</span>Scrape</button>
    <button class="bn"        id="bn-library"  onclick="showPanel('library',this,true)"> <span class="bn-ico">📚</span>Library</button>
    <button class="bn"        id="bn-optimize" onclick="showPanel('optimize',this,true)"><span class="bn-ico">⚡</span>Optimize</button>
    <button class="bn"        id="bn-jobs"     onclick="showPanel('jobs',this,true)">
      <span class="bn-ico">🔧</span>Jobs
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
    <button class="reader-close" onclick="closeReader()">✕ Close</button>
    <span class="reader-title" id="reader-title">—</span>
    <div class="reader-nav">
      <button class="rnav-btn" id="reader-prev" onclick="readerPrev()">‹ Prev</button>
      <select class="rch-sel" id="reader-sel" onchange="readerJump(this.value)"></select>
      <button class="rnav-btn" id="reader-next" onclick="readerNext()">Next ›</button>
    </div>
  </div>
  <div class="reader-body" id="reader-body">
    <div class="rspin" id="reader-spin">Loading…</div>
    <div id="reader-pages"></div>
  </div>
</div>

<div id="toast"></div>

<script>
let currentScrapeJob=null,currentOptJob=null,_pollers={},_delTarget=null;
let _readerManga=null,_readerChs=[],_readerIdx=0;

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
    return`<div class="m-card">
      <div class="m-cover">
        <img src="${coverUrl}" loading="lazy" alt="${esc(m.name)}"
             onerror="this.style.display='none';this.nextElementSibling.style.display='flex';">
        <div class="m-cover-ph" style="display:none;">墨</div>
        <div class="m-badges">
          ${m.has_opt?'<span class="m-badge mb-opt">webp</span>':''}
          ${m.has_cbz?'<span class="m-badge mb-cbz">cbz</span>':''}
        </div>
        <div class="m-overlay">
          <button class="m-overlay-btn" onclick="openReader('${esc(m.name)}')">▶ Read</button>
        </div>
      </div>
      <div class="m-info">
        <div class="m-name" title="${esc(m.name)}">${esc(m.name)}</div>
        <div class="m-meta">${m.chapters} ch${m.chapters!==1?'s':''}</div>
      </div>
      <div class="m-actions">
        ${m.has_cbz
          ?`<a class="btn btn-dl" href="/api/download/${encodeURIComponent(m.name)}" download>⬇ Download CBZ</a>`
          :`<button class="btn btn-ghost" disabled style="opacity:.3;cursor:not-allowed;">⬇ No CBZ yet</button>`}
        <button class="btn btn-ghost" onclick="quickOptimize('${esc(m.name)}')">⚡ Optimize</button>
        <div class="m-del-row">
          <button class="btn btn-del" title="Delete source files" onclick="openDelModal('${esc(m.name)}','${esc(m.name)} (source)')">🗑 Src</button>
          ${m.has_opt?`<button class="btn btn-del" title="Delete WebP folder" onclick="openDelModal('${esc(m.name)}-optimized','${esc(m.name)} (webp)')">🗑 WebP</button>`:''}
          ${m.has_cbz?`<button class="btn btn-del" title="Delete CBZ folder" onclick="openDelModal('${esc(m.name)}-cbz','${esc(m.name)} (cbz)')">🗑 CBZ</button>`:''}
        </div>
      </div>
    </div>`;
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
    if(d.ok){toast(`✔ Deleted: ${name}`,'var(--green)');refreshLibrary();refreshOptSel();}
    else toast(`✘ ${d.error}`,'var(--red)');
  }catch(e){toast('Delete failed','var(--red)');}
}

async function openReader(manga){
  _readerManga=manga;
  document.getElementById('reader').classList.add('open');
  document.body.style.overflow='hidden';
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
  document.getElementById('reader-body').scrollTop=0;
  const r=await fetch(`/api/read/${encodeURIComponent(_readerManga)}/${encodeURIComponent(ch)}/images`);
  if(!r.ok){spin.textContent='Failed to load chapter.';return;}
  const d=await r.json();
  spin.style.display='none';
  pages.innerHTML=d.images.map(img=>`<div class="rpage"><img src="/api/read/${encodeURIComponent(_readerManga)}/${encodeURIComponent(ch)}/img/${encodeURIComponent(img)}" loading="lazy" decoding="async" alt="${esc(img)}"></div>`).join('');
}

function closeReader(){
  document.getElementById('reader').classList.remove('open');
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

    print(f"\n  墨  Manga Vault running → http://0.0.0.0:{PORT}\n")
    app.run(host="0.0.0.0", port=PORT, debug=False, threaded=True)
