#!/usr/bin/env python3
"""
Balaena Quant — Control Panel
=============================
輕量即時控制台,模仿 Balaena Quant,但所有數據都是從本機真實狀態即時抓取
—— 沒有假 agent 數字、沒有假 log、沒有死連結。

模組:
  • Mission Control  主控台:健康列 + Agent Team + Strategy Pipeline + Daemon Log + 指令派發
  • KB Explorer      知識庫瀏覽:領域分類 + 檔案列表 + Markdown 預覽(掃 Obsidian vault)

資料源(全部即時):
  • 頂部健康列   systemctl --user is-active（gateway×2 + llama）+ nvidia-smi + 攝取追蹤檔
  • Agent Team   hermes profiles 的存活狀態 + index.db 各角色的真實活動指標
  • Pipeline     index.db factors 依 status 對應到階段
  • Log Stream   journalctl --user 合併三個 service 的真 log
  • KB Explorer  /home/anson/Documents/Obsidian Vault/量化交易 的真實 .md 檔

啟動:
  python3 mission_control.py                    # 唯讀,http://localhost:8900
  python3 mission_control.py --enable-dispatch  # 額外開啟「對 agent 下指令」(localhost only)

⚠️ dispatch 預設關閉:本服務無認證且只綁 127.0.0.1;開啟後 /api/dispatch 會以
   `hermes chat --profile <p> -q <cmd>` 背景執行指令。請自行確認環境安全再開。
"""

import http.server
import json
import os
import re
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse, parse_qs

PORT = 8900
HOST = "127.0.0.1"
DB_PATH = "/home/anson/factor-lab/index.db"
TRACKING = "/home/anson/.hermes/profiles/deepseek/data/quant_processed_urls.json"
VAULT_ROOT = "/home/anson/Documents/Obsidian Vault/量化交易"
SERVICES = [
    ("hermes-gateway.service", "Gateway"),
    ("hermes-gateway-deepseek.service", "Gateway·DS"),
    ("llama-qwen.service", "Llama·Qwen3.6-27B"),
]
TZ = timezone(timedelta(hours=8))  # Asia/Taipei
ENABLE_DISPATCH = "--enable-dispatch" in sys.argv

PROFILES_DIR = "/home/anson/.hermes/profiles"
CRON_JOBS = "/home/anson/.hermes/cron/jobs.json"
KANBAN_DB = "/home/anson/.hermes/kanban.db"
PROC_PATTERNS = ("hermes", "llama-server", "mission_control.py", "dashboard_server.py",
                 "quant_scanner", "uv run", "batch_extract")
FACTOR_LAB = "/home/anson/factor-lab"
UV = "/home/anson/.local/bin/uv"
# fl-<role> profile 對應 index.db debate_transcripts.agent_role(顯示真實活動步數)
PROFILE_ROLE = {"fl-proposer": "proposer", "fl-builder": "builder", "fl-skeptic": "skeptic",
                "fl-empiricist": "empiricist", "fl-librarian": "librarian"}

PIPELINE_STAGES = [
    ("stage_0", "Stage 0", "Alpha Idea"), ("stage_1", "Stage 1", "Research"),
    ("stage_2", "Stage 2", "Factor Dev"), ("stage_3", "Stage 3", "Full Backtest"),
    ("stage_4a", "Stage 4A", "Paper Trade"), ("stage_4b", "Stage 4B", "Small Capital"),
    ("stage_5", "Stage 5", "Live"), ("graveyard", "Graveyard", "Dead Pool"),
]

DOMAIN_ICONS = {
    "00": "📥", "01": "📈", "02": "📐", "03": "🛡️", "04": "🔬", "05": "🔭",
    "06": "📚", "07": "🧠", "08": "₿", "09": "👤", "10": "🧬", "11": "💡",
    "13": "🪦", "99": "⚙️",
}


# ── shell helpers ────────────────────────────────────────────────────
def sh(cmd, timeout=8):
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout).stdout.strip()
    except Exception:
        return ""


def now():
    return datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")


# ── Mission Control collectors ───────────────────────────────────────
def get_services():
    return [{"name": label, "unit": unit,
             "status": sh(["systemctl", "--user", "is-active", unit]) or "unknown"}
            for unit, label in SERVICES]


def get_gpu():
    raw = sh(["nvidia-smi", "--query-gpu=memory.used,memory.total,utilization.gpu",
              "--format=csv,noheader,nounits"])
    try:
        used, total, util = [int(x.strip()) for x in raw.split(",")]
        return {"used": used, "total": total, "util": util, "pct": round(used / total * 100)}
    except Exception:
        return {"used": 0, "total": 0, "util": 0, "pct": 0}


def get_ingestion():
    try:
        with open(TRACKING) as f:
            d = json.load(f)
        last = d.get("last_scan")
        healthy, age_txt = False, "—"
        if last:
            try:
                age = datetime.now() - datetime.fromisoformat(last).replace(tzinfo=None)
                healthy = age < timedelta(hours=24)
                age_txt = f"{age.days}d ago" if age.days else f"{age.seconds // 3600}h ago"
            except Exception:
                pass
        return {"status": "HEALTHY" if healthy else "UNHEALTHY",
                "last_scan": (last or "")[:19], "age": age_txt,
                "processed": len(d.get("processed_urls", []))}
    except Exception:
        return {"status": "UNKNOWN", "last_scan": "—", "age": "—", "processed": 0}


def get_db():
    out = {"role_steps": {}, "decisions": {}, "factor_status": {}, "kpis": {}}
    try:
        c = sqlite3.connect(DB_PATH)
        c.row_factory = sqlite3.Row
        factors = [dict(r) for r in c.execute("SELECT name,status FROM factors")]
        for r in c.execute("SELECT agent_role, COUNT(*) n FROM debate_transcripts GROUP BY agent_role"):
            out["role_steps"][r["agent_role"]] = r["n"]
        for r in c.execute("SELECT decision, COUNT(*) n FROM debate_rounds WHERE decision!='' GROUP BY decision"):
            out["decisions"][r["decision"]] = r["n"]
        status = {}
        for f in factors:
            status[f["status"]] = status.get(f["status"], 0) + 1
        out["factor_status"] = status
        out["kpis"] = {
            "factors": len(factors), "candidates": status.get("candidate", 0),
            "deprecated": status.get("deprecated", 0),
            "sessions": c.execute("SELECT COUNT(*) FROM debate_sessions").fetchone()[0],
            "papers": c.execute("SELECT COUNT(*) FROM literature").fetchone()[0],
            "insights": c.execute("SELECT COUNT(*) FROM paper_insights").fetchone()[0],
            "evidence": c.execute("SELECT COUNT(*) FROM evidence").fetchone()[0],
            "transcripts": c.execute("SELECT COUNT(*) FROM debate_transcripts").fetchone()[0],
        }
        c.close()
    except Exception as e:
        out["error"] = str(e)
    return out


def get_running_profiles():
    active = set()
    for line in sh(["pgrep", "-af", "hermes"]).splitlines():
        m = re.search(r"--profile\s+(\S+)", line)
        if m:
            active.add(m.group(1))
    # deepseek gateway service 跑的就是 deepseek profile
    if sh(["systemctl", "--user", "is-active", "hermes-gateway-deepseek.service"]) == "active":
        active.add("deepseek")
    return active


def get_model(profile):
    """讀 profile config.yaml 的真實 model.default / provider。"""
    try:
        t = open(os.path.join(PROFILES_DIR, profile, "config.yaml"), encoding="utf-8").read()
    except Exception:
        return ("—", "—")
    m = re.search(r"model:\s*\n\s*default:\s*(\S+)(?:\s*\n\s*provider:\s*(\S+))?", t)
    if not m:
        return ("—", "—")
    strip = lambda s: s.strip().strip("'\"") if s else "—"
    return (strip(m.group(1)), strip(m.group(2)))


def get_profiles(db, running):
    """列出真實 hermes profiles —— 名稱、模型、provider、存活狀態、真實活動。"""
    try:
        names = sorted(d for d in os.listdir(PROFILES_DIR)
                       if os.path.isdir(os.path.join(PROFILES_DIR, d)))
    except Exception:
        names = []
    out = []
    for name in names:
        model, provider = get_model(name)
        role = PROFILE_ROLE.get(name)
        out.append({"name": name, "model": model, "provider": provider,
                    "status": "RUNNING" if name in running else "IDLE",
                    "role": role,
                    "steps": db["role_steps"].get(role) if role else None})
    return out


def build_pipeline(db):
    # 只用真實 factor status,1:1 對應,不臆測階段:candidate→Factor Dev,deprecated→Graveyard
    st = db["factor_status"]
    counts = {"stage_2": st.get("candidate", 0), "graveyard": st.get("deprecated", 0)}
    return [{"id": sid, "title": t, "subtitle": s, "count": counts.get(sid, 0)}
            for sid, t, s in PIPELINE_STAGES]


def get_logs(n=80):
    raw = sh(["journalctl", "--user", "-u", "hermes-gateway.service",
              "-u", "hermes-gateway-deepseek.service", "-u", "llama-qwen.service",
              "-n", str(n), "--no-pager", "-o", "short-iso"], timeout=8)
    lines = []
    for ln in raw.splitlines():
        m = re.match(r"^(\S+)\s+\S+\s+(\S+?)(?:\[\d+\])?:\s*(.*)$", ln)
        if not m:
            continue
        ts, src, msg = m.groups()
        low = msg.lower()
        level = ("ERROR" if any(w in low for w in ("error", "traceback", "fail", "exception", "signal 9"))
                 else "WARN" if any(w in low for w in ("warn", "⚠", "deprecat", "truncat"))
                 else "INFO")
        lines.append({"time": ts[11:19], "level": level,
                      "source": src.replace(".service", ""), "msg": msg[:300]})
    return lines


def _human_dur(secs):
    secs = int(secs)
    if secs < 60:
        return f"{secs}s"
    if secs < 3600:
        return f"{secs // 60}m"
    if secs < 86400:
        return f"{secs // 3600}h{(secs % 3600) // 60}m"
    return f"{secs // 86400}d{(secs % 86400) // 3600}h"


def _proc_label(args):
    if "llama-server" in args:
        return "llama · Qwen3.6-27B"
    if "gateway run" in args and "--profile deepseek" in args:
        return "gateway · deepseek"
    if "gateway run" in args:
        return "gateway · default"
    m = re.search(r"--profile\s+(\S+).*?\bchat\b|\bchat\b.*?--profile\s+(\S+)", args)
    if m:
        return "agent · " + (m.group(1) or m.group(2))
    if "mission_control.py" in args:
        return "control-panel · 8900"
    if "dashboard_server.py" in args:
        return "kb-monitor · 8899"
    if "quant_scanner" in args:
        return "quant-scanner"
    if "uv run" in args:
        return "uv · " + (args.split("uv run", 1)[1].strip().split()[0].split("/")[-1] if "uv run" in args else "task")
    return os.path.basename(args.split()[0]) if args.split() else "?"


def get_processes():
    raw = sh(["ps", "-eo", "pid,etimes,pcpu,pmem,rss,args", "--sort=-pcpu"], timeout=8)
    out = []
    for ln in raw.splitlines()[1:]:
        parts = ln.split(None, 5)
        if len(parts) < 6:
            continue
        pid, et, cpu, mem, rss, args = parts
        low = args.lower()
        if not any(p in low for p in PROC_PATTERNS):
            continue
        if "ps -eo" in args or "pgrep" in args or "grep" in low:
            continue
        out.append({"pid": int(pid), "label": _proc_label(args),
                    "cpu": float(cpu), "mem": float(mem),
                    "rss_mb": round(int(rss) / 1024), "uptime": _human_dur(et)})
    return out[:30]


def get_cron_jobs():
    try:
        jobs = json.load(open(CRON_JOBS)).get("jobs", [])
    except Exception:
        return []
    out = []
    for j in jobs:
        s = j.get("schedule", {}) or {}
        disp = s.get("display") or s.get("expr") or (f"every {s.get('minutes')}m" if s.get("minutes") else "—")
        enabled = j.get("enabled", True)
        out.append({
            "id": j.get("id"), "name": j.get("name") or j.get("id"),
            "schedule": disp, "enabled": enabled,
            "state": "disabled" if not enabled else (j.get("state") or "scheduled"),
            "last_run": (j.get("last_run_at") or "")[:19].replace("T", " "),
            "next_run": (j.get("next_run_at") or "")[:19].replace("T", " "),
            "last_status": j.get("last_status") or "—",
            "error": (j.get("last_error") or "")[:160],
        })
    return out


def get_kanban_tasks():
    out = {"tasks": [], "counts": {}}
    try:
        c = sqlite3.connect(KANBAN_DB)
        c.row_factory = sqlite3.Row
        rows = [dict(r) for r in c.execute(
            "SELECT id,title,status,assignee,priority,started_at,completed_at,"
            "current_step_key,last_heartbeat_at,consecutive_failures,worker_pid "
            "FROM tasks ORDER BY created_at DESC LIMIT 100")]
        c.close()
        for r in rows:
            out["counts"][r["status"]] = out["counts"].get(r["status"], 0) + 1
        out["tasks"] = rows
    except Exception as e:
        out["error"] = str(e)
    return out


def build_processes():
    return {"generated_at": now(), "procs": get_processes(),
            "jobs": get_cron_jobs(), "kanban": get_kanban_tasks()}


def get_signal_modules():
    d = os.path.join(FACTOR_LAB, "core", "signals")
    try:
        return sorted("core.signals." + f[:-3] for f in os.listdir(d)
                      if f.endswith(".py") and f != "__init__.py")
    except Exception:
        return []


def run_backtest(module, cost_bps="10", asset="BTC", tf="1d"):
    if not module or module not in get_signal_modules():
        return {"error": "unknown module"}
    if asset not in ("BTC", "ETH", "XAU", "OIL"):
        asset = "BTC"
    if tf not in ("5m", "15m", "30m", "45m", "1h", "3h", "4h", "1d", "1w"):
        tf = "1d"
    try:
        cb = str(float(cost_bps))
    except Exception:
        cb = "10"
    try:
        r = subprocess.run([UV, "run", "--with", "numpy>=2", "--with", "pandas",
                            "python", "scripts/backtest_timeseries.py", module, "factor", cb, asset, tf],
                           cwd=FACTOR_LAB, capture_output=True, text=True, timeout=180)
        out = r.stdout.strip()
        if not out:
            return {"error": (r.stderr or "no output")[-300:]}
        return json.loads(out.splitlines()[-1])
    except Exception as e:
        return {"error": str(e)}


_FSIM_MOD = {"FSIM_IIR": "core.signals.FSIM_Illiquidity_Impact_Reversal",
             "FSIM_VWTP": "core.signals.FSIM_Volume_Weighted_Trend_Persistence",
             "FSIM_VOVT": "core.signals.FSIM_Vol_of_Vol_Timing"}


def _factor_module(fid, mods):
    """把 factors.id 對應到可回測的 signal 模組(找不到回 None)。"""
    if fid in _FSIM_MOD and _FSIM_MOD[fid] in mods:
        return _FSIM_MOD[fid]
    m = re.match(r"(F\d+)", fid)                        # F10 / F22_TSMOM / F6 ...
    if m:
        pref = "core.signals." + m.group(1) + "_"
        for mod in mods:
            if mod.startswith(pref):
                return mod
    return None


_EQ_NAME = {"EQ_SCV": "Small-Cap Value ★", "EQ_VALUE_SN": "Value (large-cap·衰減)",
            "EQ_ASSET_GROWTH": "Asset Growth (反轉)", "EQ_PIOTROSKI": "Piotroski F-score",
            "EQ_LOWVOL_MOM": "Low-vol / Momentum"}
_EQ_STATUS = {"VALIDATED": "validated", "REJECT": "deprecated", "DECAYED": "deprecated", "REWORK": "candidate"}


def get_equity_factors():
    """EQ 股票階段因子(讀 index.db 的 EQ-% sessions 最新裁決)。"""
    out = []
    try:
        c = sqlite3.connect(DB_PATH); c.row_factory = sqlite3.Row
        latest = {}
        for r in c.execute("SELECT factor_name,ic_mean,sharpe,decision,created_at FROM debate_rounds "
                            "WHERE session_id LIKE 'EQ-%' ORDER BY created_at"):
            latest[r["factor_name"]] = dict(r)            # 後者(較新)覆蓋
        c.close()
        for fn, rd in latest.items():
            dec = rd.get("decision") or ""
            out.append({"id": fn, "name": _EQ_NAME.get(fn, fn), "category": "equity·US",
                        "status": _EQ_STATUS.get(dec, "candidate"), "ic": rd.get("ic_mean"),
                        "sharpe": rd.get("sharpe"), "decision": dec, "module": None})
    except Exception:
        pass
    # validated 置頂,其餘依名稱
    return sorted(out, key=lambda f: (f["status"] != "validated", f["name"]))


def get_factors():
    """factors 表 + 各因子最新 debate_round 的 IC/Sharpe/裁決 + 可回測模組;末尾接 EQ 股票因子。"""
    out = []
    mods = get_signal_modules()
    try:
        c = sqlite3.connect(DB_PATH)
        c.row_factory = sqlite3.Row
        rounds = {}
        for r in c.execute("SELECT factor_name,ic_mean,sharpe,decision,round_num FROM debate_rounds ORDER BY round_num"):
            rounds[r["factor_name"]] = dict(r)  # round_num 大者覆蓋 → 最新裁決
        for f in c.execute("SELECT id,name,category,status FROM factors ORDER BY id"):
            rd = rounds.get(f["name"], {})
            out.append({"id": f["id"], "name": f["name"], "category": f["category"],
                        "status": f["status"], "ic": rd.get("ic_mean"),
                        "sharpe": rd.get("sharpe"), "decision": rd.get("decision") or "",
                        "module": _factor_module(f["id"], mods)})
        c.close()
    except Exception:
        pass
    seen = {f["id"] for f in out}                # factors 表已有者不再從 EQ sessions 重複列
    return out + [f for f in get_equity_factors() if f["id"] not in seen]


def build_state():
    db = get_db()
    running = get_running_profiles()
    profiles = get_profiles(db, running)
    return {
        "generated_at": now(), "dispatch_enabled": ENABLE_DISPATCH,
        "health": {"services": get_services(), "gpu": get_gpu(), "ingestion": get_ingestion()},
        "agents": profiles, "pipeline": build_pipeline(db),
        "factors": get_factors(),
        "kpis": db["kpis"], "dispatch_profiles": [p["name"] for p in profiles],
    }


# ── KB Explorer ──────────────────────────────────────────────────────
_KB_CACHE = {"ts": 0, "data": None}


def _read_frontmatter_tags(path):
    """Cheap: read head, pull first 'tags' / inline tags and date."""
    tags, date = [], ""
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            head = f.read(800)
    except Exception:
        return tags, date
    if head.startswith("---"):
        fm = head.split("---", 2)
        if len(fm) >= 2:
            block = fm[1]
            md = re.search(r"date:\s*(\S+)", block)
            if md:
                date = md.group(1).strip()[:10]
            inline = re.search(r"tags:\s*\[([^\]]*)\]", block)
            if inline:
                tags = [t.strip().strip("'\"") for t in inline.group(1).split(",") if t.strip()]
            else:
                tags = re.findall(r"^\s*-\s*(.+)$", block, re.M)
                tags = [t.strip().strip("'\"") for t in tags][:6]
    return tags[:4], date


def kb_index():
    if _KB_CACHE["data"] and time.time() - _KB_CACHE["ts"] < 60:
        return _KB_CACHE["data"]
    domains, files = {}, []
    if os.path.isdir(VAULT_ROOT):
        for dirpath, _, fnames in os.walk(VAULT_ROOT):
            for fn in fnames:
                if not fn.endswith(".md"):
                    continue
                full = os.path.join(dirpath, fn)
                rel = os.path.relpath(full, VAULT_ROOT)
                parts = rel.split(os.sep)
                domain = parts[0] if len(parts) > 1 else "（根目錄）"
                domains[domain] = domains.get(domain, 0) + 1
                tags, date = _read_frontmatter_tags(full)
                try:
                    mtime = os.path.getmtime(full)
                except Exception:
                    mtime = 0
                files.append({"name": fn[:-3], "path": rel, "domain": domain,
                              "date": date, "tags": tags, "mtime": int(mtime)})
    dom_list = sorted(([{"name": d, "count": n, "icon": DOMAIN_ICONS.get(d[:2], "📁")}
                        for d, n in domains.items()]), key=lambda x: -x["count"])
    data = {"domains": dom_list, "total": len(files), "files": files}
    _KB_CACHE.update(ts=time.time(), data=data)
    return data


def kb_files(domain=None, q="", sort="name"):
    idx = kb_index()
    files = idx["files"]
    if domain and domain != "all":
        files = [f for f in files if f["domain"] == domain]
    if q:
        ql = q.lower()
        files = [f for f in files if ql in f["name"].lower()]
    if sort == "date":
        files = sorted(files, key=lambda f: -f["mtime"])
    else:
        files = sorted(files, key=lambda f: f["name"].lower())
    return {"files": files[:500], "count": len(files)}


def kb_file(rel):
    base = os.path.realpath(VAULT_ROOT)
    full = os.path.realpath(os.path.join(VAULT_ROOT, rel))
    if not (full == base or full.startswith(base + os.sep)) or not full.endswith(".md"):
        return {"error": "invalid path"}
    if not os.path.isfile(full):
        return {"error": "not found"}
    try:
        with open(full, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read(200000)
        return {"name": os.path.basename(full)[:-3], "path": rel, "content": content}
    except Exception as e:
        return {"error": str(e)}


# ── dispatch (opt-in) ────────────────────────────────────────────────
def do_dispatch(profile, command):
    if not ENABLE_DISPATCH:
        return {"ok": False, "error": "dispatch 已停用,請用 --enable-dispatch 啟動伺服器"}
    if not profile or not command:
        return {"ok": False, "error": "缺少 profile 或 command"}
    valid = set(os.listdir(PROFILES_DIR)) if os.path.isdir(PROFILES_DIR) else set()
    if profile not in valid:
        return {"ok": False, "error": f"未知 profile: {profile}"}
    try:
        subprocess.Popen(["hermes", "chat", "--profile", profile, "-q", command],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
        return {"ok": True, "msg": f"已派發給 {profile}: {command[:60]}"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ── HTTP server ──────────────────────────────────────────────────────
class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, body, ctype="application/json"):
        data = body.encode("utf-8") if isinstance(body, str) else body
        self.send_response(code)
        self.send_header("Content-Type", ctype + "; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        u = urlparse(self.path)
        q = parse_qs(u.query)
        if u.path == "/":
            self._send(200, HTML, "text/html")
        elif u.path == "/api/state":
            self._send(200, json.dumps(build_state(), ensure_ascii=False))
        elif u.path == "/api/logs":
            self._send(200, json.dumps({"lines": get_logs(int(q.get("n", [80])[0]))}, ensure_ascii=False))
        elif u.path == "/api/processes":
            self._send(200, json.dumps(build_processes(), ensure_ascii=False))
        elif u.path == "/api/backtest/list":
            self._send(200, json.dumps({"modules": get_signal_modules()}, ensure_ascii=False))
        elif u.path == "/api/backtest":
            self._send(200, json.dumps(run_backtest(q.get("module", [""])[0],
                                                    q.get("cost", ["10"])[0],
                                                    q.get("asset", ["BTC"])[0],
                                                    q.get("tf", ["1d"])[0]), ensure_ascii=False))
        elif u.path == "/api/kb":
            idx = kb_index()
            self._send(200, json.dumps({"domains": idx["domains"], "total": idx["total"]}, ensure_ascii=False))
        elif u.path == "/api/kb/files":
            self._send(200, json.dumps(kb_files(q.get("domain", [None])[0], q.get("q", [""])[0],
                                                q.get("sort", ["name"])[0]), ensure_ascii=False))
        elif u.path == "/api/file":
            self._send(200, json.dumps(kb_file(q.get("path", [""])[0]), ensure_ascii=False))
        elif u.path == "/graph":
            self._send(200, _kb_graph_html(), "text/html")
        elif u.path == "/api/kb_graph":
            self._send(200, json.dumps(_kb_graph_data(), ensure_ascii=False))
        else:
            self._send(404, json.dumps({"error": "not found"}))

    def do_POST(self):
        if urlparse(self.path).path == "/api/dispatch":
            length = int(self.headers.get("Content-Length", 0))
            try:
                payload = json.loads(self.rfile.read(length) or "{}")
            except Exception:
                payload = {}
            res = do_dispatch(payload.get("profile"), payload.get("command"))
            self._send(200 if res.get("ok") else 400, json.dumps(res, ensure_ascii=False))
        else:
            self._send(404, json.dumps({"error": "not found"}))


def _kb_graph_data():
    import sys
    if FACTOR_LAB not in sys.path:
        sys.path.insert(0, FACTOR_LAB)
    from core.kb_graph import build_graph
    return build_graph(DB_PATH)


def _kb_graph_html():
    import sys
    if FACTOR_LAB not in sys.path:
        sys.path.insert(0, FACTOR_LAB)
    from core.kb_graph import build_graph, render_html
    return render_html(build_graph(DB_PATH))


HTML = r"""<!DOCTYPE html>
<html lang="zh-Hant"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Balaena Quant — Control Panel</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
:root{--bg:#0a0e14;--surf:#11161e;--hi:#161c25;--bd:#1f2632;--bd2:#2a3340;--bd3:#3a4658;
--tx:#e6edf3;--tx2:#9aa6b7;--tx3:#5b6878;--green:#4cc38a;--amber:#f5a524;--red:#e5484d;
--blue:#5eb1ff;--purple:#c084fc;--mono:'JetBrains Mono','Courier New',monospace;
--sans:'Inter',system-ui,sans-serif;--r:10px;}
*{box-sizing:border-box;margin:0;padding:0;}
body{font-family:var(--sans);font-size:13px;background:var(--bg);color:var(--tx);padding-left:50px;padding-bottom:30px;}
a{color:var(--blue);text-decoration:none;}
.hidden{display:none!important;}
/* nav rail */
.rail{position:fixed;top:0;left:0;bottom:0;width:50px;background:var(--surf);border-right:1px solid var(--bd);
display:flex;flex-direction:column;align-items:center;padding-top:12px;gap:6px;z-index:60;}
.rail button{width:38px;height:38px;border-radius:9px;border:none;background:none;color:var(--tx3);
font-size:18px;cursor:pointer;display:flex;align-items:center;justify-content:center;}
.rail button:hover{background:var(--hi);color:var(--tx);}
.rail button.active{background:rgba(94,177,255,.14);color:var(--blue);}
/* topbar */
.topbar{display:flex;align-items:center;gap:14px;padding:12px 22px;background:var(--surf);
border-bottom:1px solid var(--bd);position:sticky;top:0;z-index:50;}
.logo{font-size:15px;font-weight:700;}.logo b{color:var(--green);}
.tag{font-size:9px;padding:2px 7px;border-radius:9999px;background:var(--hi);color:var(--tx3);font-weight:600;letter-spacing:.05em;}
.tag.live{background:var(--green);color:#0a0e14;}
.spacer{flex:1;}
.hchip{display:flex;align-items:center;gap:6px;font-size:11px;color:var(--tx2);padding:4px 10px;
border-radius:9999px;background:var(--hi);border:1px solid var(--bd);}
.dot{width:7px;height:7px;border-radius:50%;background:var(--tx3);flex-shrink:0;}
.dot.active,.dot.HEALTHY{background:var(--green);}
.dot.WORKING,.dot.RUNNING{background:var(--green);animation:pulse 1.5s infinite;}
.dot.inactive,.dot.failed,.dot.UNHEALTHY{background:var(--red);}
.dot.WARN{background:var(--amber);}.dot.IDLE{background:var(--tx3);}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.35}}
.clock{font-family:var(--mono);font-size:11px;color:var(--tx3);}
.wrap{max-width:1500px;margin:0 auto;padding:18px 22px;}
/* dispatch */
.dispatch{display:flex;gap:10px;align-items:center;margin-bottom:8px;}
.dispatch select,.dispatch input{background:var(--surf);border:1px solid var(--bd2);color:var(--tx);
border-radius:8px;padding:9px 12px;font-family:var(--sans);font-size:13px;}
.dispatch select{min-width:150px;}.dispatch input{flex:1;}
.dispatch button{background:var(--blue);border:none;color:#0a0e14;font-weight:700;border-radius:8px;padding:9px 20px;cursor:pointer;}
.dispatch button:disabled{background:var(--bd2);color:var(--tx3);cursor:not-allowed;}
.dispatch-note{font-size:10px;color:var(--tx3);margin-bottom:18px;}
.sec{font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.06em;color:var(--tx2);
margin:22px 0 10px;display:flex;align-items:center;gap:10px;}
.sec .pill{font-size:9px;padding:2px 8px;border-radius:9999px;background:var(--hi);color:var(--tx2);}
.sec .pill.g{background:rgba(76,195,138,.16);color:var(--green);}
/* kpis */
.kpis{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:4px;}
.kpi{background:var(--surf);border:1px solid var(--bd);border-radius:8px;padding:8px 14px;}
.kpi b{font-family:var(--mono);font-size:18px;}.kpi span{font-size:10px;color:var(--tx3);margin-left:6px;}
/* agents */
.agrid{display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:12px;}
.acard{background:var(--surf);border:1px solid var(--bd2);border-radius:var(--r);padding:14px;position:relative;overflow:hidden;}
.acard::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;background:var(--accent);}
.ahead{display:flex;align-items:center;gap:8px;margin-bottom:2px;}
.aname{font-weight:700;font-size:14px;}.astatus{margin-left:auto;font-size:9px;font-weight:600;color:var(--tx3);}
.arole{font-size:11px;color:var(--tx3);margin-bottom:8px;}
.chips{display:flex;flex-wrap:wrap;gap:4px;margin-bottom:10px;}
.chip{font-size:9px;padding:2px 7px;border-radius:9999px;background:var(--hi);color:var(--tx2);border:1px solid var(--bd);}
.chip.m{background:rgba(94,177,255,.12);color:var(--blue);border:none;}
.atask{font-size:11px;color:var(--tx2);font-style:italic;margin-bottom:10px;min-height:30px;}
.amets{display:grid;grid-template-columns:1fr 1fr;gap:3px 10px;padding-top:10px;border-top:1px solid var(--bd);}
.amet{font-size:10px;}.amet b{font-family:var(--mono);color:var(--tx);font-weight:700;}.amet span{color:var(--tx3);}
/* pipeline */
.pgrid{display:grid;grid-template-columns:repeat(8,1fr);gap:10px;}
.pcard{background:var(--surf);border:1px solid var(--bd);border-radius:var(--r);padding:12px;text-align:center;}
.pcard.gy{opacity:.65;}.pcard.gy .pcount{color:var(--red);}
.ptitle{font-size:10px;font-weight:700;color:var(--tx2);text-transform:uppercase;}
.psub{font-size:9px;color:var(--tx3);margin-bottom:8px;}
.pcount{font-family:var(--mono);font-size:26px;font-weight:700;}
/* log */
.logwrap{background:var(--surf);border:1px solid var(--bd);border-radius:var(--r);}
.logfilters{display:flex;gap:6px;padding:10px 14px;border-bottom:1px solid var(--bd);}
.lf{font-size:10px;padding:3px 11px;border-radius:9999px;border:1px solid var(--bd);background:none;color:var(--tx3);cursor:pointer;}
.lf.active{background:var(--blue);border-color:var(--blue);color:#0a0e14;}
.logstream{max-height:320px;overflow-y:auto;font-family:var(--mono);font-size:11px;line-height:1.7;padding:8px 14px;}
.logline{display:flex;gap:10px;padding:1px 0;}
.lt{color:var(--tx3);flex-shrink:0;}.ll{font-weight:700;width:46px;flex-shrink:0;}
.ll.INFO{color:var(--tx3);}.ll.WARN{color:var(--amber);}.ll.ERROR{color:var(--red);}
.ls{color:var(--tx3);flex-shrink:0;width:130px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.lm{color:var(--tx2);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
/* KB Explorer */
.kbgrid{display:grid;grid-template-columns:220px 1fr 1.1fr;gap:14px;height:calc(100vh - 120px);}
.kbpanel{background:var(--surf);border:1px solid var(--bd);border-radius:var(--r);display:flex;flex-direction:column;overflow:hidden;}
.kbhead{padding:11px 14px;border-bottom:1px solid var(--bd);font-size:11px;font-weight:600;color:var(--tx2);
text-transform:uppercase;letter-spacing:.05em;display:flex;justify-content:space-between;}
.domlist{overflow-y:auto;padding:6px;}
.dom{display:flex;align-items:center;gap:9px;padding:7px 10px;border-radius:7px;cursor:pointer;font-size:12px;color:var(--tx2);}
.dom:hover{background:var(--hi);color:var(--tx);}
.dom.active{background:rgba(94,177,255,.1);color:var(--blue);}
.dom .c{margin-left:auto;font-family:var(--mono);font-size:11px;color:var(--tx3);}
.kbtools{display:flex;gap:8px;padding:10px 12px;border-bottom:1px solid var(--bd);}
.kbtools input{flex:1;background:var(--bg);border:1px solid var(--bd2);color:var(--tx);border-radius:7px;padding:7px 10px;font-size:12px;}
.kbtools button{background:var(--bg);border:1px solid var(--bd2);color:var(--tx2);border-radius:7px;padding:7px 11px;font-size:11px;cursor:pointer;}
.kbtools button.active{background:var(--blue);color:#0a0e14;border-color:var(--blue);}
.filelist{overflow-y:auto;flex:1;}
.frow{display:flex;align-items:center;gap:10px;padding:9px 14px;border-bottom:1px solid var(--bd);cursor:pointer;font-size:12px;}
.frow:hover{background:var(--hi);}.frow.active{background:rgba(94,177,255,.08);}
.fname{flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.fbadge{font-size:9px;padding:1px 7px;border-radius:9999px;background:var(--hi);color:var(--tx3);flex-shrink:0;}
.fdate{font-family:var(--mono);font-size:10px;color:var(--tx3);flex-shrink:0;}
.preview{overflow-y:auto;flex:1;padding:20px 24px;line-height:1.7;}
.preview h1{font-size:20px;margin:14px 0 8px;}.preview h2{font-size:16px;margin:14px 0 6px;border-bottom:1px solid var(--bd);padding-bottom:4px;}
.preview h3{font-size:14px;margin:12px 0 5px;color:var(--tx2);}
.preview p{margin:7px 0;color:var(--tx2);}.preview ul{margin:7px 0 7px 20px;color:var(--tx2);}
.preview li{margin:3px 0;}.preview code{font-family:var(--mono);background:var(--hi);padding:1px 5px;border-radius:4px;font-size:12px;}
.preview pre{background:var(--bg);border:1px solid var(--bd);border-radius:7px;padding:12px;overflow-x:auto;margin:9px 0;}
.preview pre code{background:none;padding:0;}.preview hr{border:none;border-top:1px solid var(--bd);margin:12px 0;}
.preview blockquote{border-left:3px solid var(--bd3);padding-left:12px;color:var(--tx3);margin:8px 0;}
.empty{color:var(--tx3);font-size:12px;padding:24px;text-align:center;}
/* Processes */
.ptable{width:100%;border-collapse:collapse;}
.ptable th{text-align:left;font-size:10px;text-transform:uppercase;letter-spacing:.05em;color:var(--tx3);
padding:9px 14px;border-bottom:1px solid var(--bd);font-weight:600;}
.ptable td{padding:9px 14px;border-bottom:1px solid var(--bd);font-size:12px;}
.ptable tr:last-child td{border-bottom:none;}
.ptable .mono{font-family:var(--mono);}
.ptable .r{text-align:right;font-family:var(--mono);}
.bar{height:5px;background:var(--bd);border-radius:3px;overflow:hidden;min-width:60px;}
.bar>i{display:block;height:100%;background:var(--blue);}
.sbadge{font-size:9px;padding:2px 8px;border-radius:9999px;font-weight:600;text-transform:uppercase;}
.sbadge.ok,.sbadge.success,.sbadge.scheduled{background:rgba(76,195,138,.16);color:var(--green);}
.sbadge.disabled,.sbadge.paused{background:var(--hi);color:var(--tx3);}
.sbadge.error,.sbadge.failed{background:rgba(229,72,77,.16);color:var(--red);}
.sbadge.running{background:rgba(94,177,255,.16);color:var(--blue);}
.sbadge.candidate,.sbadge.REWORK{background:rgba(245,165,36,.16);color:var(--amber);}
.sbadge.deprecated,.sbadge.REJECT,.sbadge.DECAYED{background:rgba(229,72,77,.16);color:var(--red);}
.sbadge.validated,.sbadge.VALIDATE,.sbadge.VALIDATED{background:rgba(76,195,138,.16);color:var(--green);}
.sbadge.sim{background:rgba(192,132,252,.16);color:var(--purple);}
.num{font-family:var(--mono);text-align:right;} .num.pos{color:var(--green);} .num.neg{color:var(--red);} .num.na{color:var(--tx3);}
.toast{position:fixed;bottom:20px;right:20px;background:var(--surf);border:1px solid var(--bd3);border-radius:8px;
padding:12px 18px;font-size:12px;opacity:0;transition:opacity .3s;z-index:99;}.toast.show{opacity:1;}
</style></head><body>

<div class="rail">
  <button id="nav-mc" class="active" title="Mission Control">🛸</button>
  <button id="nav-proc" title="Processes">⚙️</button>
  <button id="nav-bt" title="Backtest">📈</button>
  <button id="nav-kb" title="KB Explorer">📚</button>
</div>

<div class="topbar">
  <div class="logo">🐋 Balaena <b>Quant</b></div>
  <span class="tag" id="modtag">MISSION CONTROL</span>
  <span class="tag live">LIVE</span>
  <div class="spacer"></div>
  <div class="hchip" id="ingest"><span class="dot"></span> Ingestion</div>
  <div id="svc-chips" style="display:flex;gap:8px;"></div>
  <div class="hchip" id="gpu"><span class="dot active"></span> GPU</div>
  <span class="clock" id="clock"></span>
</div>

<!-- ══ Mission Control ══ -->
<div class="wrap" id="view-mc">
  <div class="dispatch">
    <select id="d-profile"></select>
    <input id="d-cmd" placeholder="對 agent 下指令… (Enter 送出)">
    <button id="d-send">Send</button>
  </div>
  <div class="dispatch-note" id="d-note"></div>
  <div class="kpis" id="kpis"></div>
  <div class="sec">Hermes Profiles <span class="pill g" id="n-active"></span><span class="pill" id="n-idle"></span></div>
  <div class="agrid" id="agents"></div>
  <div class="sec">Strategy Pipeline <span class="pill" id="n-strat"></span></div>
  <div class="pgrid" id="pipeline"></div>
  <div class="sec">Factors <span class="pill" id="n-fac"></span></div>
  <div class="logwrap">
    <table class="ptable"><thead><tr>
      <th>因子</th><th>類別</th><th>狀態</th><th class="r">IC</th><th class="r">Sharpe</th><th>裁決</th>
    </tr></thead><tbody id="fac-tbody"></tbody></table>
  </div>

  <div class="sec">Daemon Log Stream <span class="pill" id="n-log"></span></div>
  <div class="logwrap">
    <div class="logfilters" id="logfilters">
      <button class="lf active" data-f="all">All</button><button class="lf" data-f="INFO">Info</button>
      <button class="lf" data-f="WARN">Warn</button><button class="lf" data-f="ERROR">Error</button>
    </div>
    <div class="logstream" id="logstream"></div>
  </div>
</div>

<!-- ══ KB Explorer ══ -->
<div class="wrap hidden" id="view-kb">
  <div class="kbgrid">
    <div class="kbpanel">
      <div class="kbhead">Knowledge Domains <span id="kb-total"></span></div>
      <div class="domlist" id="domlist"></div>
    </div>
    <div class="kbpanel">
      <div class="kbtools">
        <input id="kb-search" placeholder="搜尋檔名…">
        <button id="kb-sort-name" class="active">A-Z</button>
        <button id="kb-sort-date">日期</button>
      </div>
      <div class="filelist" id="filelist"></div>
    </div>
    <div class="kbpanel">
      <div class="kbhead" id="prev-head">檔案預覽</div>
      <div class="preview" id="preview"><div class="empty">點左側檔案以預覽內容</div></div>
    </div>
  </div>
</div>

<!-- ══ Backtest ══ -->
<div class="wrap hidden" id="view-bt">
  <div class="dispatch">
    <select id="bt-sel" style="min-width:280px"></select>
    <select id="bt-asset"><option>BTC</option><option>ETH</option><option>XAU</option><option>OIL</option></select>
    <select id="bt-tf"><option value="5m">5m</option><option value="15m">15m</option><option value="30m">30m</option><option value="45m">45m</option><option value="1h">1h</option><option value="3h">3h</option><option value="4h">4h</option><option value="1d" selected>1d</option><option value="1w">1w</option></select>
    <span style="font-size:11px;color:var(--tx2)">成本/邊</span>
    <input id="bt-cost" type="number" value="10" min="0" step="1" style="width:70px"><span style="font-size:11px;color:var(--tx3)">bps</span>
    <button id="bt-run">回測</button>
    <span class="dispatch-note" id="bt-note" style="margin:0 0 0 6px">選 signal + 設定成本,跑真實回測(BTCUSDT 1d,net of cost)</span>
  </div>
  <div class="kpis" id="bt-kpis" style="margin-top:8px"></div>
  <div class="card" style="margin-top:14px"><div class="card__head">Equity Curve & Drawdown</div><div class="card__body"><div id="bt-eq" style="height:300px"></div></div></div>
  <div class="card" style="margin-top:14px"><div class="card__head">Daily P&L</div><div class="card__body"><div id="bt-pnl" style="height:170px"></div></div></div>
  <div class="card" style="margin-top:14px"><div class="card__head">Monthly Returns</div><div class="card__body"><div id="bt-mon" style="height:240px"></div></div></div>
  <div class="card" style="margin-top:14px"><div class="card__head">K 線 + 進出場(▲多 ▼空 · ✕出場)</div><div class="card__body"><div id="bt-kline" style="height:340px"></div></div></div>
  <div class="card" style="margin-top:14px"><div class="card__head">逐筆交易明細 <span id="bt-ntrade" style="color:var(--tx3);font-weight:400"></span></div>
    <div class="card__body" style="max-height:340px;overflow-y:auto;padding:0">
      <table class="ptable"><thead><tr><th>#</th><th>進場</th><th>出場</th><th>方向</th><th class="r">進場價</th><th class="r">出場價</th><th class="r">持有</th><th class="r">報酬(net)</th><th class="r">累積</th></tr></thead>
      <tbody id="bt-trades"></tbody></table>
    </div>
  </div>
</div>

<!-- ══ Processes ══ -->
<div class="wrap hidden" id="view-proc">
  <div class="sec">即時進程 <span class="pill" id="n-proc"></span></div>
  <div class="logwrap">
    <table class="ptable"><thead><tr>
      <th>進程</th><th>PID</th><th class="r">CPU%</th><th class="r">記憶體</th><th>CPU 用量</th><th class="r">運行時間</th>
    </tr></thead><tbody id="proc-tbody"></tbody></table>
  </div>

  <div class="sec">排程工作 (cron) <span class="pill" id="n-jobs"></span></div>
  <div class="logwrap">
    <table class="ptable"><thead><tr>
      <th>工作</th><th>排程</th><th>狀態</th><th>上次執行</th><th>結果</th><th>下次執行</th>
    </tr></thead><tbody id="jobs-tbody"></tbody></table>
  </div>

  <div class="sec">Kanban 任務 <span class="pill" id="n-tasks"></span></div>
  <div class="logwrap">
    <table class="ptable"><thead><tr>
      <th>任務</th><th>狀態</th><th>負責</th><th>當前步驟</th><th>心跳</th><th class="r">失敗</th>
    </tr></thead><tbody id="tasks-tbody"></tbody></table>
  </div>
</div>

<div class="toast" id="toast"></div>

<script>
const esc = s => String(s ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');

/* ── view switching ── */
let curView='mc';
function show(view){
  curView=view;
  for(const v of ['mc','kb','proc','bt']){
    document.getElementById('view-'+v).classList.toggle('hidden', view!==v);
    document.getElementById('nav-'+v).classList.toggle('active', view===v);
  }
  document.getElementById('modtag').textContent =
    {mc:'MISSION CONTROL',kb:'KB EXPLORER',proc:'PROCESSES',bt:'BACKTEST'}[view];
  if(location.hash.slice(1)!==view) history.replaceState(null,'','#'+view);
  if(view==='kb' && !kbLoaded){ loadKB(); kbLoaded=true; }
  if(view==='proc') refreshProc();
  if(view==='bt' && !btLoaded){ loadBtList(); btLoaded=true; }
}
document.getElementById('nav-mc').onclick = ()=>show('mc');
document.getElementById('nav-kb').onclick = ()=>show('kb');
document.getElementById('nav-proc').onclick = ()=>show('proc');
document.getElementById('nav-bt').onclick = ()=>show('bt');

/* ── Mission Control ── */
let logFilter='all', logCache=[];
async function refreshState(){
  let s; try{ s=await (await fetch('/api/state')).json(); }catch(e){ return; }
  const ing=s.health.ingestion;
  document.getElementById('ingest').innerHTML=`<span class="dot ${ing.status}"></span> Ingestion ${ing.status} · ${esc(ing.age)}`;
  document.getElementById('svc-chips').innerHTML=s.health.services.map(v=>
    `<div class="hchip"><span class="dot ${v.status}"></span> ${esc(v.name)}</div>`).join('');
  const g=s.health.gpu;
  document.getElementById('gpu').innerHTML=`<span class="dot active"></span> GPU ${g.used}/${g.total}MB · ${g.util}%`;
  const sel=document.getElementById('d-profile');
  if(!sel.dataset.filled){ sel.innerHTML=s.dispatch_profiles.map(p=>`<option>${esc(p)}</option>`).join(''); sel.dataset.filled='1'; }
  const dis=!s.dispatch_enabled;
  document.getElementById('d-send').disabled=dis; document.getElementById('d-cmd').disabled=dis;
  document.getElementById('d-note').textContent=dis
    ? '⚠️ 指令派發已停用。用  python3 mission_control.py --enable-dispatch  啟動才能對 agent 下令。'
    : '✓ 指令派發已啟用(localhost only) — 會以 hermes chat 背景執行。';
  const k=s.kpis;
  document.getElementById('kpis').innerHTML=[['factors','因子'],['candidates','候選'],['deprecated','棄置'],
    ['sessions','辯論'],['papers','文獻'],['insights','洞見'],['evidence','實證']].map(([key,l])=>
    `<div class="kpi"><b>${k[key]??0}</b><span>${l}</span></div>`).join('');
  const active=s.agents.filter(a=>a.status==='RUNNING').length;
  document.getElementById('n-active').textContent=active+' RUNNING';
  document.getElementById('n-idle').textContent=(s.agents.length-active)+' IDLE';
  document.getElementById('agents').innerHTML=s.agents.map(a=>`
    <div class="acard" style="--accent:${a.status==='RUNNING'?'var(--green)':'var(--bd2)'}">
      <div class="ahead"><span class="dot ${a.status}"></span><span class="aname">${esc(a.name)}</span><span class="astatus">${a.status}</span></div>
      <div class="arole">${esc(a.provider)}</div>
      <div class="chips"><span class="chip m">${esc(a.model)}</span>${a.role?`<span class="chip">${esc(a.role)}</span>`:''}</div>
      ${a.steps!=null?`<div class="amets"><div class="amet"><b>${a.steps}</b> <span>辯論步驟</span></div></div>`:''}
    </div>`).join('');
  document.getElementById('n-strat').textContent=s.kpis.factors+' FACTORS';
  document.getElementById('pipeline').innerHTML=s.pipeline.map(p=>`
    <div class="pcard ${p.id==='graveyard'?'gy':''}"><div class="ptitle">${esc(p.title)}</div>
    <div class="psub">${esc(p.subtitle)}</div><div class="pcount">${p.count}</div></div>`).join('');
  // factors
  const fac=s.factors||[];
  document.getElementById('n-fac').textContent=fac.length+' 個';
  const numc=v=>(v===null||v===undefined)?'<td class="num na">—</td>':`<td class="num ${v>=0?'pos':'neg'}">${Number(v).toFixed(3)}</td>`;
  document.getElementById('fac-tbody').innerHTML=fac.map(f=>{
    const sim=f.id.startsWith('FSIM')?' <span class="sbadge sim">SIM</span>':'';
    const dec=f.decision?`<span class="sbadge ${esc(f.decision)}">${esc(f.decision)}</span>`:'<span class="num na">—</span>';
    const bt=f.module?`<span style="color:var(--blue);font-size:10px">📈 回測</span>`:'<span class="num na" style="font-size:10px">無模組</span>';
    const click=f.module?` class="factor-bt" data-mod="${esc(f.module)}" style="cursor:pointer"`:'';
    return `<tr${click}><td>${esc(f.name)}${sim}</td><td>${esc(f.category)}</td>
      <td><span class="sbadge ${esc(f.status)}">${esc(f.status)}</span></td>
      ${numc(f.ic)}${numc(f.sharpe)}<td>${dec} ${bt}</td></tr>`;}).join('')||'<tr><td colspan="6" class="empty">無因子</td></tr>';
  document.querySelectorAll('#fac-tbody tr.factor-bt').forEach(tr=>tr.onclick=()=>jumpToBacktest(tr.dataset.mod));
}
async function refreshLogs(){
  let r; try{ r=await (await fetch('/api/logs?n=120')).json(); }catch(e){ return; }
  logCache=r.lines; renderLogs();
}
function renderLogs(){
  const el=document.getElementById('logstream');
  const f=logFilter==='all'?logCache:logCache.filter(l=>l.level===logFilter);
  document.getElementById('n-log').textContent=logCache.length+' LINES';
  const atBottom=el.scrollHeight-el.scrollTop-el.clientHeight<40;
  el.innerHTML=f.map(l=>`<div class="logline"><span class="lt">${esc(l.time)}</span>
    <span class="ll ${l.level}">${l.level}</span><span class="ls">${esc(l.source)}</span>
    <span class="lm">${esc(l.msg)}</span></div>`).join('')||'<div class="empty">無 log</div>';
  if(atBottom) el.scrollTop=el.scrollHeight;
}
document.querySelectorAll('.lf').forEach(b=>b.onclick=()=>{
  document.querySelectorAll('.lf').forEach(x=>x.classList.remove('active'));
  b.classList.add('active'); logFilter=b.dataset.f; renderLogs();
});
function toast(m){const t=document.getElementById('toast');t.textContent=m;t.classList.add('show');setTimeout(()=>t.classList.remove('show'),3500);}
async function dispatch(){
  const profile=document.getElementById('d-profile').value;
  const command=document.getElementById('d-cmd').value.trim(); if(!command)return;
  const r=await (await fetch('/api/dispatch',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({profile,command})})).json();
  toast(r.ok?'✓ '+r.msg:'✗ '+r.error); if(r.ok)document.getElementById('d-cmd').value='';
}
document.getElementById('d-send').onclick=dispatch;
document.getElementById('d-cmd').addEventListener('keydown',e=>{if(e.key==='Enter')dispatch();});

/* ── KB Explorer ── */
let kbLoaded=false, kbDomain='all', kbSort='name';
async function loadKB(){
  const idx=await (await fetch('/api/kb')).json();
  document.getElementById('kb-total').textContent=idx.total;
  const all=`<div class="dom active" data-d="all">📂 All Files<span class="c">${idx.total}</span></div>`;
  document.getElementById('domlist').innerHTML=all+idx.domains.map(d=>
    `<div class="dom" data-d="${esc(d.name)}">${d.icon} ${esc(d.name)}<span class="c">${d.count}</span></div>`).join('');
  document.querySelectorAll('.dom').forEach(el=>el.onclick=()=>{
    document.querySelectorAll('.dom').forEach(x=>x.classList.remove('active'));
    el.classList.add('active'); kbDomain=el.dataset.d; loadFiles();
  });
  loadFiles();
}
async function loadFiles(){
  const q=document.getElementById('kb-search').value.trim();
  const r=await (await fetch(`/api/kb/files?domain=${encodeURIComponent(kbDomain)}&q=${encodeURIComponent(q)}&sort=${kbSort}`)).json();
  document.getElementById('filelist').innerHTML=r.files.map(f=>`
    <div class="frow" data-p="${esc(f.path)}">
      <span class="fname">${esc(f.name)}</span>
      ${f.tags&&f.tags.length?`<span class="fbadge">${esc(f.tags[0])}</span>`:''}
      <span class="fdate">${esc(f.date||'')}</span>
    </div>`).join('')||'<div class="empty">無檔案</div>';
  document.querySelectorAll('.frow').forEach(el=>el.onclick=()=>{
    document.querySelectorAll('.frow').forEach(x=>x.classList.remove('active'));
    el.classList.add('active'); openFile(el.dataset.p);
  });
}
let st;
document.getElementById('kb-search').addEventListener('input',()=>{clearTimeout(st);st=setTimeout(loadFiles,200);});
document.getElementById('kb-sort-name').onclick=()=>{kbSort='name';setSort();loadFiles();};
document.getElementById('kb-sort-date').onclick=()=>{kbSort='date';setSort();loadFiles();};
function setSort(){document.getElementById('kb-sort-name').classList.toggle('active',kbSort==='name');
  document.getElementById('kb-sort-date').classList.toggle('active',kbSort==='date');}
async function openFile(path){
  document.getElementById('preview').innerHTML='<div class="empty">載入中…</div>';
  const r=await (await fetch('/api/file?path='+encodeURIComponent(path))).json();
  document.getElementById('prev-head').textContent=r.name||'檔案預覽';
  document.getElementById('preview').innerHTML=r.error?`<div class="empty">${esc(r.error)}</div>`:md(r.content);
}
/* 極簡 markdown 渲染 */
function md(src){
  src=src.replace(/^---\n[\s\S]*?\n---\n/,'');  // 去 frontmatter
  const lines=src.split('\n'); let out='',inCode=false,inList=false;
  const inline=t=>esc(t).replace(/\*\*(.+?)\*\*/g,'<b>$1</b>').replace(/`([^`]+)`/g,'<code>$1</code>')
    .replace(/\[([^\]]+)\]\(([^)]+)\)/g,'<a href="$2" target="_blank">$1</a>')
    .replace(/\[\[([^\]]+)\]\]/g,'<a>$1</a>');
  for(let ln of lines){
    if(ln.startsWith('```')){ if(inCode){out+='</code></pre>';inCode=false;}else{out+='<pre><code>';inCode=true;} continue;}
    if(inCode){ out+=esc(ln)+'\n'; continue; }
    if(/^\s*[-*]\s+/.test(ln)){ if(!inList){out+='<ul>';inList=true;} out+='<li>'+inline(ln.replace(/^\s*[-*]\s+/,''))+'</li>'; continue;}
    if(inList){out+='</ul>';inList=false;}
    let m;
    if(m=ln.match(/^(#{1,3})\s+(.*)/)){ out+=`<h${m[1].length}>${inline(m[2])}</h${m[1].length}>`; }
    else if(/^---+$/.test(ln.trim())){ out+='<hr>'; }
    else if(/^>\s?/.test(ln)){ out+='<blockquote>'+inline(ln.replace(/^>\s?/,''))+'</blockquote>'; }
    else if(ln.trim()===''){ out+=''; }
    else{ out+='<p>'+inline(ln)+'</p>'; }
  }
  if(inList)out+='</ul>'; if(inCode)out+='</code></pre>';
  return out;
}

/* ── Processes ── */
async function refreshProc(){
  let d; try{ d=await (await fetch('/api/processes')).json(); }catch(e){ return; }
  document.getElementById('n-proc').textContent=d.procs.length+' RUNNING';
  document.getElementById('proc-tbody').innerHTML=d.procs.map(p=>`
    <tr><td>${esc(p.label)}</td><td class="mono">${p.pid}</td>
    <td class="r">${p.cpu.toFixed(1)}</td><td class="r">${p.rss_mb} MB</td>
    <td><div class="bar"><i style="width:${Math.min(100,p.cpu)}%"></i></div></td>
    <td class="r">${esc(p.uptime)}</td></tr>`).join('')||'<tr><td colspan="6" class="empty">無</td></tr>';
  const en=d.jobs.filter(j=>j.enabled).length;
  document.getElementById('n-jobs').textContent=`${en} ENABLED · ${d.jobs.length} 總`;
  document.getElementById('jobs-tbody').innerHTML=d.jobs.map(j=>{
    const st=j.enabled?(j.last_status&&j.last_status!=='—'?j.last_status:'scheduled'):'disabled';
    return `<tr><td>${esc(j.name)}</td><td class="mono">${esc(j.schedule)}</td>
    <td><span class="sbadge ${esc(st)}">${esc(st)}</span></td>
    <td class="mono">${esc(j.last_run||'—')}</td>
    <td>${j.error?`<span class="sbadge error" title="${esc(j.error)}">error</span>`:esc(j.last_status)}</td>
    <td class="mono">${esc(j.next_run||'—')}</td></tr>`;
  }).join('')||'<tr><td colspan="6" class="empty">無排程</td></tr>';
  const ks=d.kanban; const cnt=Object.entries(ks.counts||{}).map(([k,v])=>`${k}:${v}`).join(' · ');
  document.getElementById('n-tasks').textContent=ks.tasks.length?cnt:'0 任務';
  document.getElementById('tasks-tbody').innerHTML=ks.tasks.map(t=>`
    <tr><td>${esc(t.title)}</td><td><span class="sbadge ${esc(t.status)}">${esc(t.status)}</span></td>
    <td>${esc(t.assignee||'—')}</td><td class="mono">${esc(t.current_step_key||'—')}</td>
    <td class="mono">${esc((t.last_heartbeat_at||'').slice(11,19)||'—')}</td>
    <td class="r">${t.consecutive_failures||0}</td></tr>`).join('')
    ||'<tr><td colspan="6" class="empty">目前沒有 kanban 任務(有任務時會即時顯示狀態與進度)</td></tr>';
}

/* ── Backtest ── */
let btLoaded=false;
const PLOT_LAYOUT=extra=>Object.assign({paper_bgcolor:'rgba(0,0,0,0)',plot_bgcolor:'rgba(0,0,0,0)',
  font:{color:'#9aa6b7',family:'Inter',size:11},margin:{t:10,r:50,b:30,l:50},showlegend:false},extra);
const PCFG={displayModeBar:false,responsive:true};
let btPending=null;
async function loadBtList(){
  const r=await (await fetch('/api/backtest/list')).json();
  const sel=document.getElementById('bt-sel');
  sel.innerHTML=(r.modules||[]).map(m=>`<option value="${esc(m)}">${esc(m.replace('core.signals.',''))}</option>`).join('');
  const opts=[...sel.options];
  if(btPending && opts.some(o=>o.value===btPending)){ sel.value=btPending; btPending=null; }
  else { const vwtp=opts.find(o=>o.value.includes('VWTP')); if(vwtp) vwtp.selected=true; }
  runBt();
}
function jumpToBacktest(module){
  if(!module) return;
  if(btLoaded){
    document.getElementById('bt-sel').value=module;
    show('bt'); runBt();
  } else {
    btPending=module; show('bt');   // loadBtList(由 show 觸發)會認 btPending 並自動跑
  }
}
async function runBt(){
  const m=document.getElementById('bt-sel').value; if(!m)return;
  const cost=document.getElementById('bt-cost').value||'10';
  const asset=document.getElementById('bt-asset').value||'BTC';
  const tf=document.getElementById('bt-tf').value||'1d';
  document.getElementById('bt-note').textContent='回測中…';
  let d; try{ d=await (await fetch('/api/backtest?module='+encodeURIComponent(m)+'&cost='+encodeURIComponent(cost)+'&asset='+encodeURIComponent(asset)+'&tf='+encodeURIComponent(tf))).json(); }catch(e){ return; }
  if(d.error){ document.getElementById('bt-note').textContent='✗ '+d.error; return; }
  const k=d.kpis, pct=v=>(v*100).toFixed(2)+'%';
  document.getElementById('bt-note').textContent='✓ '+m.replace('core.signals.','')+' · '+(d.asset||'BTC')+' '+(d.timeframe||'1d')+','+k.total_bars+' 根,'+k.n_trades+' 筆,成本 '+k.cost_bps+'bps/邊';
  document.getElementById('bt-kpis').innerHTML=[
    ['總報酬(net)',pct(k.total_return)],['最大回撤',pct(k.max_drawdown)],
    ['Sharpe(net)',k.sharpe],['Sharpe(gross)',k.sharpe_gross],
    ['年化(net)',pct(k.ann_return)],['勝率',pct(k.win_rate)],
    ['年換手',k.ann_turnover+'x'],['成本拖累',pct(k.cost_drag_annual)+'/yr'],['交易日',k.trading_days],
  ].map(([l,v])=>`<div class="kpi"><b>${v}</b><span>${l}</span></div>`).join('');
  // Equity(net 實線 + gross 淡線)+ Drawdown(雙軸)
  Plotly.newPlot('bt-eq',[
    {x:d.dates,y:d.equity_gross,type:'scatter',mode:'lines',line:{color:'#5b6878',width:1,dash:'dot'},name:'Equity(gross)'},
    {x:d.dates,y:d.equity,type:'scatter',mode:'lines',line:{color:'#5eb1ff',width:1.6},name:'Equity(net)'},
    {x:d.dates,y:d.drawdown,type:'scatter',mode:'lines',line:{color:'#e5484d',width:1},name:'Drawdown',yaxis:'y2'},
  ],PLOT_LAYOUT({xaxis:{gridcolor:'#1f2632'},yaxis:{title:'Equity',gridcolor:'#1f2632'},
    yaxis2:{title:'DD',overlaying:'y',side:'right',tickformat:'.0%',gridcolor:'rgba(0,0,0,0)'},
    showlegend:true,legend:{orientation:'h',y:1.12,font:{size:9}}}),PCFG);
  // Daily P&L
  Plotly.newPlot('bt-pnl',[{x:d.dates,y:d.pnl,type:'bar',
    marker:{color:d.pnl.map(v=>v>=0?'#4cc38a':'#e5484d')}}],
    PLOT_LAYOUT({xaxis:{gridcolor:'#1f2632'},yaxis:{tickformat:'.1%',gridcolor:'#1f2632'}}),PCFG);
  // Monthly heatmap
  const years=[...new Set(d.monthly.map(x=>x.year))].sort();
  const z=years.map(y=>Array.from({length:12},(_,mi)=>{
    const e=d.monthly.find(x=>x.year===y&&x.month===mi+1); return e?e.ret:null;}));
  Plotly.newPlot('bt-mon',[{type:'heatmap',z,x:['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'],
    y:years.map(String),colorscale:[[0,'#e5484d'],[0.5,'#161c25'],[1,'#4cc38a']],zmid:0,
    hovertemplate:'%{y}-%{x}: %{z:.2%}<extra></extra>',showscale:true,colorbar:{tickformat:'.0%',len:0.9}}],
    PLOT_LAYOUT({margin:{t:10,r:10,b:30,l:50},yaxis:{autorange:'reversed'}}),PCFG);
  // K線 + 進出場標記
  const longE=d.trades.filter(t=>t.dir>0), shortE=d.trades.filter(t=>t.dir<0);
  Plotly.newPlot('bt-kline',[
    {type:'candlestick',x:d.dates,open:d.open,high:d.high,low:d.low,close:d.close,name:'K',
     increasing:{line:{color:'#4cc38a'}},decreasing:{line:{color:'#e5484d'}}},
    {type:'scatter',mode:'markers',name:'多進',x:longE.map(t=>t.entry),y:longE.map(t=>t.entry_px),
     marker:{symbol:'triangle-up',color:'#4cc38a',size:9,line:{color:'#0a0e14',width:1}}},
    {type:'scatter',mode:'markers',name:'空進',x:shortE.map(t=>t.entry),y:shortE.map(t=>t.entry_px),
     marker:{symbol:'triangle-down',color:'#e5484d',size:9,line:{color:'#0a0e14',width:1}}},
    {type:'scatter',mode:'markers',name:'出場',x:d.trades.map(t=>t.exit),y:d.trades.map(t=>t.exit_px),
     marker:{symbol:'x',color:'#9aa6b7',size:7}},
  ],PLOT_LAYOUT({xaxis:{rangeslider:{visible:false},gridcolor:'#1f2632'},yaxis:{gridcolor:'#1f2632'},
    showlegend:true,legend:{orientation:'h',y:1.1,font:{size:9}}}),PCFG);
  // 交易明細表(新到舊)
  document.getElementById('bt-ntrade').textContent='('+d.trades.length+' 筆)';
  document.getElementById('bt-trades').innerHTML=d.trades.slice().reverse().map((t,i)=>{
    const dir=t.dir>0?'<span style="color:#4cc38a">多</span>':'<span style="color:#e5484d">空</span>';
    return `<tr><td>${d.trades.length-i}</td><td class="mono">${t.entry}</td><td class="mono">${t.exit}</td><td>${dir}</td>
      <td class="r">${t.entry_px}</td><td class="r">${t.exit_px}</td><td class="r">${t.bars}</td>
      <td class="num ${t.ret>=0?'pos':'neg'}">${pct(t.ret)}</td><td class="num ${t.cum>=0?'pos':'neg'}">${pct(t.cum)}</td></tr>`;
  }).join('')||'<tr><td colspan="9" class="empty">無交易</td></tr>';
}
document.getElementById('bt-run').onclick=runBt;
document.getElementById('bt-sel').addEventListener('change',runBt);
document.getElementById('bt-cost').addEventListener('change',runBt);
document.getElementById('bt-asset').addEventListener('change',runBt);
document.getElementById('bt-tf').addEventListener('change',runBt);

/* ── boot ── */
function tick(){document.getElementById('clock').textContent=new Date().toLocaleTimeString('zh-TW',{hour12:false});}
tick();setInterval(tick,1000);
refreshState();refreshLogs();setInterval(refreshState,5000);setInterval(refreshLogs,5000);
setInterval(()=>{ if(curView==='proc') refreshProc(); },5000);
const h=location.hash.slice(1); if(['kb','proc','bt'].includes(h)) show(h);
</script>
</body></html>"""


def main():
    server = http.server.ThreadingHTTPServer((HOST, PORT), Handler)
    print("🐋 Balaena Quant — Control Panel")
    print(f"   ▶ http://{HOST}:{PORT}   (Mission Control + KB Explorer)")
    print(f"   dispatch: {'ENABLED' if ENABLE_DISPATCH else 'disabled (--enable-dispatch 開啟)'}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n   停止。")
        server.shutdown()


if __name__ == "__main__":
    main()
