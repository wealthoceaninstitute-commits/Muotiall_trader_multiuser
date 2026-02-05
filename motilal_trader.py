# CT_FastAPI_webapp_v1.py
"""
CT_FastAPI WebApp (Clean V1) — single-file Motilal multi-user backend

Goal
- Convert the old "local machine" CT_FastAPI into a webapp like your router.
- Single file serves: auth integration (optional), multi-user storage, GitHub persistence,
  Motilal sessions, clients, groups, copy trading setups, orders, positions, holdings.

Identity / Multiuser
- V1 uses request header:  X-User-Id: <logged_in_username>
- If you have auth.auth_router, it will be mounted at /auth (optional).
  Your frontend should call /auth/login and then include X-User-Id on subsequent requests.

Storage
- Primary: GitHub (if env vars are set).
- Fallback: Local filesystem under ./data

GitHub env vars (same idea as router)
- GITHUB_TOKEN
- GITHUB_REPO_OWNER
- GITHUB_REPO_NAME
- GITHUB_BRANCH (default: main)

CORS
- Configure via FRONTEND_ORIGINS (comma separated)
  Example: FRONTEND_ORIGINS=https://multibroker-trader-multiuser.vercel.app,http://localhost:3000
"""

from __future__ import annotations

import os
import json
import glob
import math
import time
import base64
import sqlite3
import logging
import threading
from datetime import datetime
from collections import OrderedDict
from typing import Any, Dict, List, Optional, Tuple
from fastapi import FastAPI, Body, Header, HTTPException, Query
from fastapi.responses import JSONResponse
import requests
import pyotp
import pandas as pd
from fastapi import FastAPI, Body, Header, HTTPException, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware

# --- Motilal SDK (must exist in your deployment image) ---
from MOFSLOPENAPI import MOFSLOPENAPI  # type: ignore

# Optional auth router (same import as your router)
try:
    from auth.auth_router import router as auth_router  # type: ignore
except Exception:
    auth_router = None  # type: ignore


###############################################################################
# App + CORS
###############################################################################

app = FastAPI(title="CT FastAPI WebApp (Motilal Multi-User)", version="1.0")

# CORS
_frontend_origins = os.environ.get(
    "FRONTEND_ORIGINS",
    "https://multibroker-trader-multiuser.vercel.app,http://localhost:3000,http://127.0.0.1:3000",
)
allow_origins = [o.strip() for o in _frontend_origins.split(",") if o.strip()]
# If user set FRONTEND_ORIGINS="*", allow all
if len(allow_origins) == 1 and allow_origins[0] == "*":
    allow_origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount auth router if present
if auth_router is not None:
    app.include_router(auth_router)


###############################################################################
# Constants + globals
###############################################################################

logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

# Motilal constants
Base_Url = "https://openapi.motilaloswal.com"
SourceID = "Desktop"
browsername = "chrome"
browserversion = "104"

# Symbol DB
GITHUB_CSV_URL = "https://raw.githubusercontent.com/Pramod541988/Stock_List/main/security_id.csv"
SQLITE_DB = "symbols.db"
TABLE_NAME = "symbols"
symbol_db_lock = threading.Lock()

# Per-user runtime caches (sessions + computed metadata)
# mofsl_sessions[user_id][client_display_name] = (Mofsl, client_userid)
mofsl_sessions: Dict[str, Dict[str, Tuple[MOFSLOPENAPI, str]]] = {}
# client_capital_map[user_id][client_userid] = capital
client_capital_map: Dict[str, Dict[str, float]] = {}
# position_meta[(user_id, client_display_name, symbol)] = {exchange, symboltoken, producttype}
position_meta: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
# summary_data_global[user_id][client_display_name] = summary row
summary_data_global: Dict[str, Dict[str, Dict[str, Any]]] = {}

# Copy trading runtime (global, but each setup is user-scoped)
order_mapping: Dict[str, Dict[str, Dict[str, str]]] = {}              # {setup_id: {master_order_id: {child_id: child_order_id}}}
processed_order_ids_placed: Dict[str, set] = {}                        # {setup_id: set(orderids)}
processed_order_ids_canceled: Dict[str, set] = {}                      # {setup_id: set(orderids)}


###############################################################################
# Helpers: user identity
###############################################################################

def require_user(user_id: Optional[str]) -> str:
    uid = (user_id or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="Missing X-User-Id header")
    return uid


###############################################################################
# Helpers: sanitize / safe values
###############################################################################

def _safe(s: Optional[str]) -> str:
    s = (s or "").strip().replace(" ", "_")
    return "".join(ch for ch in s if ch.isalnum() or ch in ("_", "-"))

def _pick(*vals: Any) -> str:
    for v in vals:
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return ""


###############################################################################
# Storage: local paths
###############################################################################

BASE_DIR = os.path.abspath(os.environ.get("DATA_DIR", "./data"))
USERS_ROOT = os.path.join(BASE_DIR, "users")
os.makedirs(USERS_ROOT, exist_ok=True)

def _user_root(user_id: str) -> str:
    return os.path.join(USERS_ROOT, _safe(user_id))

def _user_clients_dir(user_id: str) -> str:
    p = os.path.join(_user_root(user_id), "clients", "motilal")
    os.makedirs(p, exist_ok=True)
    return p

def _user_groups_dir(user_id: str) -> str:
    p = os.path.join(_user_root(user_id), "groups")
    os.makedirs(p, exist_ok=True)
    return p

def _user_copy_dir(user_id: str) -> str:
    p = os.path.join(_user_root(user_id), "copy_setups")
    os.makedirs(p, exist_ok=True)
    return p

def _client_path(user_id: str, client_id: str) -> str:
    return os.path.join(_user_clients_dir(user_id), f"{_safe(client_id)}.json")

def _group_path(user_id: str, group_id_or_name: str) -> str:
    return os.path.join(_user_groups_dir(user_id), f"{_safe(group_id_or_name)}.json")

def _copy_path(user_id: str, setup_id: str) -> str:
    return os.path.join(_user_copy_dir(user_id), f"{_safe(setup_id)}.json")


###############################################################################
# Storage: GitHub mirroring (like router)
###############################################################################

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "").strip()
GITHUB_REPO_OWNER = os.environ.get("GITHUB_REPO_OWNER", "").strip()
GITHUB_REPO_NAME = os.environ.get("GITHUB_REPO_NAME", "").strip()
GITHUB_BRANCH = (os.environ.get("GITHUB_BRANCH", "main") or "main").strip()

def _github_enabled() -> bool:
    return bool(GITHUB_TOKEN and GITHUB_REPO_OWNER and GITHUB_REPO_NAME)

def _gh_headers() -> Dict[str, str]:
    h = {"Accept": "application/vnd.github+json", "User-Agent": "ct-fastapi-webapp"}
    if GITHUB_TOKEN:
        h["Authorization"] = f"token {GITHUB_TOKEN}"
    return h

def _gh_contents_url(path_in_repo: str) -> str:
    return f"https://api.github.com/repos/{GITHUB_REPO_OWNER}/{GITHUB_REPO_NAME}/contents/{path_in_repo}"

def _gh_path(rel_path: str) -> str:
    # Store everything under data/ in repo
    return ("data/" + rel_path.lstrip("/")).replace("\\", "/")

def _github_write(rel_path: str, content: str) -> None:
    if not _github_enabled():
        return
    path_in_repo = _gh_path(rel_path)
    url = _gh_contents_url(path_in_repo)

    sha: Optional[str] = None
    r0 = requests.get(url, headers=_gh_headers(), params={"ref": GITHUB_BRANCH}, timeout=25)
    if r0.status_code == 200:
        sha = (r0.json() or {}).get("sha")

    payload: Dict[str, Any] = {
        "message": f"Update {path_in_repo}",
        "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
        "branch": GITHUB_BRANCH,
    }
    if sha:
        payload["sha"] = sha

    r = requests.put(url, headers=_gh_headers(), json=payload, timeout=30)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"GitHub write failed {r.status_code}: {r.text}")

def _github_delete(rel_path: str) -> None:
    if not _github_enabled():
        return
    path_in_repo = _gh_path(rel_path)
    url = _gh_contents_url(path_in_repo)

    r0 = requests.get(url, headers=_gh_headers(), params={"ref": GITHUB_BRANCH}, timeout=25)
    if r0.status_code != 200:
        return
    sha = (r0.json() or {}).get("sha")
    if not sha:
        return

    payload = {"message": f"Delete {path_in_repo}", "sha": sha, "branch": GITHUB_BRANCH}
    r = requests.delete(url, headers=_gh_headers(), json=payload, timeout=30)
    if r.status_code not in (200, 204):
        raise RuntimeError(f"GitHub delete failed {r.status_code}: {r.text}")

def _github_read_json(rel_path: str) -> Dict[str, Any]:
    if not _github_enabled():
        return {}
    path_in_repo = _gh_path(rel_path)
    url = _gh_contents_url(path_in_repo)
    r = requests.get(url, headers=_gh_headers(), params={"ref": GITHUB_BRANCH}, timeout=30)
    if r.status_code != 200:
        return {}
    j = r.json() or {}
    content_b64 = j.get("content")
    if not content_b64:
        return {}
    raw = base64.b64decode(content_b64).decode("utf-8", errors="ignore")
    try:
        return json.loads(raw)
    except Exception:
        return {}

def _github_list_dir(rel_dir: str) -> List[Dict[str, Any]]:
    if not _github_enabled():
        return []
    path_in_repo = _gh_path(rel_dir.strip("/") + "/").rstrip("/")
    url = _gh_contents_url(path_in_repo)
    r = requests.get(url, headers=_gh_headers(), params={"ref": GITHUB_BRANCH}, timeout=30)
    if r.status_code != 200:
        return []
    j = r.json()
    return j if isinstance(j, list) else []

def _store_write_json(rel_path: str, doc: Dict[str, Any]) -> None:
    # Always write locally
    abs_path = os.path.join(BASE_DIR, rel_path)
    os.makedirs(os.path.dirname(abs_path), exist_ok=True)
    with open(abs_path, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=4)

    # Mirror to GitHub if enabled
    if _github_enabled():
        _github_write(rel_path, json.dumps(doc, indent=4))

def _store_delete(rel_path: str) -> None:
    abs_path = os.path.join(BASE_DIR, rel_path)
    try:
        if os.path.exists(abs_path):
            os.remove(abs_path)
    except Exception:
        pass
    if _github_enabled():
        _github_delete(rel_path)

def _store_read_json(rel_path: str) -> Dict[str, Any]:
    # Prefer GitHub (keeps stateless deployments consistent)
    if _github_enabled():
        d = _github_read_json(rel_path)
        if d:
            return d
    abs_path = os.path.join(BASE_DIR, rel_path)
    try:
        with open(abs_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _store_list_json(rel_dir: str) -> List[str]:
    # Return filenames (not full path)
    if _github_enabled():
        items = _github_list_dir(rel_dir)
        out: List[str] = []
        for it in items:
            if it.get("type") == "file" and (it.get("name") or "").endswith(".json"):
                out.append(it["name"])
        return out

    abs_dir = os.path.join(BASE_DIR, rel_dir)
    try:
        return [fn for fn in os.listdir(abs_dir) if fn.endswith(".json")]
    except Exception:
        return []


###############################################################################
# Symbol DB
###############################################################################

def recreate_sqlite_from_csv() -> None:
    """Recreate symbols.db from GitHub CSV."""
    r = requests.get(GITHUB_CSV_URL, timeout=30)
    r.raise_for_status()
    with open("security_id.csv", "wb") as f:
        f.write(r.content)
    if os.path.exists(SQLITE_DB):
        os.remove(SQLITE_DB)
    df = pd.read_csv("security_id.csv")
    conn = sqlite3.connect(SQLITE_DB)
    df.to_sql(TABLE_NAME, conn, index=False, if_exists="replace")
    conn.close()

@app.on_event("startup")
def _startup() -> None:
    # Build symbol DB at startup
    try:
        recreate_sqlite_from_csv()
    except Exception as e:
        print(f"[startup] symbols db init failed: {e}")


###############################################################################
# Helpers: sessions and clients
###############################################################################

def _sessions_for_user(user_id: str) -> Dict[str, Tuple[MOFSLOPENAPI, str]]:
    return mofsl_sessions.setdefault(user_id, {})

def _capitals_for_user(user_id: str) -> Dict[str, float]:
    return client_capital_map.setdefault(user_id, {})

def _safe_int_token(x: Any) -> int:
    try:
        if x is None:
            raise ValueError("symboltoken is None")
        s = str(x).strip()
        if s == "":
            raise ValueError("symboltoken empty")
        return int(float(s))
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid symboltoken: {x}")

def _get_client_doc(user_id: str, client_id: str) -> Dict[str, Any]:
    rel = f"users/{_safe(user_id)}/clients/motilal/{_safe(client_id)}.json"
    return _store_read_json(rel)

def _list_client_ids(user_id: str) -> List[str]:
    rel_dir = f"users/{_safe(user_id)}/clients/motilal"
    files = _store_list_json(rel_dir)
    return [os.path.splitext(fn)[0] for fn in files]

def _get_client_capital(user_id: str, client_id: str) -> float:
    caps = _capitals_for_user(user_id)
    if client_id in caps:
        return float(caps.get(client_id, 0) or 0)
    d = _get_client_doc(user_id, client_id) or {}
    try:
        cap = float(d.get("capital", 0) or d.get("base_amount", 0) or 0)
    except Exception:
        cap = 0.0
    caps[client_id] = cap
    return cap

def auto_qty(user_id: str, client_id: str, price: float) -> int:
    capital = _get_client_capital(user_id, client_id)
    try:
        qty = math.floor(float(capital) * 0.15 / float(price))
        return max(int(qty), 1)
    except Exception:
        return 1


def login_client(user_id: str, client_doc: Dict[str, Any]) -> None:
    """
    Login a Motilal client and cache the session under the correct user.
    Also updates session_active in storage.
    """
    uid = _safe(user_id)
    client_id = _pick(client_doc.get("userid"), client_doc.get("client_id"))
    name = (client_doc.get("name") or client_doc.get("display_name") or client_id or "").strip()
    password = client_doc.get("password", "")
    pan = str(client_doc.get("pan", "") or "")
    apikey = client_doc.get("apikey", "") or client_doc.get("api_key", "")
    totp_key = client_doc.get("totpkey", "") or client_doc.get("totp_key", "")
    capital = client_doc.get("capital", 0) or client_doc.get("base_amount", 0) or 0

    if not client_id:
        return

    # Cache capital
    _capitals_for_user(uid)[client_id] = float(capital or 0)

    session_status = False
    try:
        totp = pyotp.TOTP(totp_key).now() if totp_key else ""
        Mofsl = MOFSLOPENAPI(apikey, Base_Url, None, SourceID, browsername, browserversion)
        response = Mofsl.login(client_id, password, pan, totp, client_id)
        if isinstance(response, dict) and response.get("status") == "SUCCESS":
            _sessions_for_user(uid)[name] = (Mofsl, client_id)
            session_status = True
            print(f"[login] ✅ {uid} logged in client {name} ({client_id})")
        else:
            print(f"[login] ❌ {uid} login failed {name}: {response}")
    except Exception as e:
        print(f"[login] ❌ {uid} login error {name}: {e}")

    # Update stored doc session_active
    try:
        rel_path = f"users/{_safe(uid)}/clients/motilal/{_safe(client_id)}.json"
        stored = _store_read_json(rel_path) or {}
        stored.update(client_doc)
        stored["session_active"] = bool(session_status)
        stored["updated_at"] = datetime.utcnow().isoformat()
        _store_write_json(rel_path, stored)
    except Exception as e:
        print(f"[login] update session_active failed: {e}")


###############################################################################
# Copy-trading storage (user-scoped)
###############################################################################

def load_active_copy_setups(user_id: str) -> List[Dict[str, Any]]:
    rel_dir = f"users/{_safe(user_id)}/copy_setups"
    setups: List[Dict[str, Any]] = []
    for fn in _store_list_json(rel_dir):
        rel = f"{rel_dir}/{fn}"
        doc = _store_read_json(rel) or {}
        if isinstance(doc, dict) and doc.get("enabled", False):
            setups.append(doc)
    return setups

def get_session_by_userid(userid: str) -> Tuple[Optional[str], Optional[MOFSLOPENAPI], Optional[str], Optional[str]]:
    """
    Scan all users' sessions to find a given client userid.
    Returns (owner_user_id, client_display_name, session, userid)
    """
    for owner_uid, sessions in mofsl_sessions.items():
        for name, (Mofsl, client_uid) in sessions.items():
            if client_uid == userid:
                return owner_uid, name, Mofsl, client_uid
    return None, None, None, None

def normalize_ordertype_copytrade(s: str) -> str:
    s = (s or "").upper()
    collapsed = s.replace("_", "").replace(" ", "").replace("-", "")
    return "STOPLOSS" if collapsed == "STOPLOSS" else s


###############################################################################
# Basic endpoints
###############################################################################

@app.get("/health")
def health() -> Dict[str, Any]:
    return {"ok": True, "github": _github_enabled()}


###############################################################################
# Symbols search (for frontend select)
###############################################################################

@app.get("/search_symbols")
def search_symbols(
    q: str = Query("", alias="q"),
    exchange: str = Query("", alias="exchange"),
):
    query = (q or "").strip()
    exchange_filter = (exchange or "").strip().upper()

    if not query:
        return JSONResponse(content={"results": []})

    words = [w for w in query.lower().split() if w]
    if not words:
        return JSONResponse(content={"results": []})

    where_clauses = []
    params = []

    for w in words:
        where_clauses.append("LOWER([Stock Symbol]) LIKE ?")
        params.append(f"%{w}%")

    where_sql = " AND ".join(where_clauses)

    if exchange_filter:
        where_sql += " AND UPPER(Exchange) = ?"
        params.append(exchange_filter)

    sql = f"""
        SELECT Exchange, [Stock Symbol], [Security ID]
        FROM {TABLE_NAME}
        WHERE {where_sql}
        ORDER BY [Stock Symbol]
        LIMIT 20
    """

    with symbol_db_lock:
        conn = sqlite3.connect(SQLITE_DB)
        cur = conn.execute(sql, params)
        rows = cur.fetchall()
        conn.close()

    results = [
        {
            "id": f"{row[0]}|{row[1]}|{row[2]}",
            "text": f"{row[0]} | {row[1]}",
        }
        for row in rows
    ]

    return JSONResponse(content={"results": results})

###############################################################################
# Clients (user-scoped, GitHub persisted like router)
###############################################################################

@app.post("/clients/add")
async def clients_add(
    background_tasks: BackgroundTasks,
    payload: Dict[str, Any] = Body(...),
    user_id: str = Header(..., alias="X-User-Id"),
) -> Dict[str, Any]:
    uid = require_user(user_id)
    client_id = _pick(payload.get("userid"), payload.get("client_id"))
    if not client_id:
        raise HTTPException(status_code=400, detail="client_id/userid required")
    display_name = (payload.get("name") or payload.get("display_name") or client_id).strip()

    doc = dict(payload)
    doc["broker"] = "motilal"
    doc["userid"] = client_id
    doc["name"] = display_name
    doc.setdefault("created_at", datetime.utcnow().isoformat())
    doc["updated_at"] = datetime.utcnow().isoformat()
    doc.setdefault("session_active", False)

    rel = f"users/{_safe(uid)}/clients/motilal/{_safe(client_id)}.json"
    _store_write_json(rel, doc)

    # Background login for this client
    background_tasks.add_task(login_client, uid, doc)

    return {"success": True, "message": "Client saved. Login started.", "client_id": client_id}


@app.get("/get_clients")
def get_clients(
    user_id: str = Header(..., alias="X-User-Id"),
) -> Dict[str, Any]:
    uid = require_user(user_id)

    # Build a fast lookup of client_ids that are currently logged-in (runtime sessions)
    runtime_logged_in = set()
    try:
        for _nm, (_mofsl, cid) in _sessions_for_user(uid).items():
            runtime_logged_in.add(str(cid))
    except Exception:
        pass

    rel_dir = f"users/{_safe(uid)}/clients/motilal"
    out: List[Dict[str, Any]] = []

    for fn in _store_list_json(rel_dir):
        rel = f"{rel_dir}/{fn}"
        d = _store_read_json(rel) or {}
        if not isinstance(d, dict):
            continue

        client_id = str(d.get("userid", "") or d.get("client_id", "") or "").strip()

        # ✅ Prefer live runtime session (truthy if login already happened)
        if client_id and client_id in runtime_logged_in:
            session_status = "Logged in"
        else:
            # fallback to persisted flag
            session_status = "Logged in" if d.get("session_active") else "pending"

        out.append(
            {
                "name": d.get("name", "") or d.get("display_name", ""),
                "client_id": client_id,
                "capital": d.get("capital", "") or "",
                "session": session_status,
                "broker": "motilal",
            }
        )

    return {"clients": out}


@app.post("/clients/delete")
async def clients_delete(
    payload: Dict[str, Any] = Body(...),
    user_id: str = Header(..., alias="X-User-Id"),
) -> Dict[str, Any]:
    uid = require_user(user_id)
    items: List[Dict[str, Any]] = []
    if isinstance(payload.get("items"), list):
        items = payload["items"]
    elif isinstance(payload.get("clients"), list):
        items = payload["clients"]
    else:
        items = [payload]

    deleted: List[str] = []
    missing: List[str] = []

    for it in items:
        cid = _pick(it.get("client_id"), it.get("userid"))
        if not cid:
            continue
        rel = f"users/{_safe(uid)}/clients/motilal/{_safe(cid)}.json"
        # Remove in-memory sessions too
        # Find any session entry matching this userid
        sessions = _sessions_for_user(uid)
        to_drop = [nm for nm, (_s, u) in sessions.items() if u == cid]
        for nm in to_drop:
            sessions.pop(nm, None)

        # Delete stored
        before = _store_read_json(rel)
        if before:
            _store_delete(rel)
            deleted.append(cid)
        else:
            missing.append(cid)

    return {"deleted": deleted, "missing": missing}


###############################################################################
# Groups (user scoped)
###############################################################################

@app.post("/add_group")
async def add_group(
    payload: Dict[str, Any] = Body(...),
    user_id: str = Header(..., alias="X-User-Id"),
) -> Dict[str, Any]:
    uid = require_user(user_id)
    name = (payload.get("group_name") or payload.get("name") or "").strip()
    members = payload.get("clients") or payload.get("members") or []
    multiplier = int(payload.get("multiplier") or 1)

    if not name or not isinstance(members, list) or not members:
        raise HTTPException(status_code=400, detail="group_name and clients required")

    doc = {
        "id": _safe(name),
        "group_name": name,
        "name": name,
        "clients": [str(x) for x in members],
        "members": [str(x) for x in members],
        "multiplier": multiplier,
        "updated_at": datetime.utcnow().isoformat(),
    }
    rel = f"users/{_safe(uid)}/groups/{_safe(name)}.json"
    _store_write_json(rel, doc)
    return {"success": True, "message": f'Group "{name}" saved.'}


@app.get("/get_groups")
def get_groups(
    user_id: str = Header(..., alias="X-User-Id"),
) -> Dict[str, Any]:
    uid = require_user(user_id)

    # map client id to display name for nicer UI
    client_map: Dict[str, str] = {}
    for cid in _list_client_ids(uid):
        d = _get_client_doc(uid, cid) or {}
        client_map[str(cid)] = (d.get("name") or d.get("display_name") or cid)

    rel_dir = f"users/{_safe(uid)}/groups"
    groups: List[Dict[str, Any]] = []
    for fn in _store_list_json(rel_dir):
        d = _store_read_json(f"{rel_dir}/{fn}") or {}
        ids = d.get("clients") or d.get("members") or []
        ids = [str(x) for x in (ids or [])]
        names = [client_map.get(i, i) for i in ids]
        groups.append(
            {
                "group_name": d.get("group_name") or d.get("name") or "",
                "no_of_clients": len(ids),
                "multiplier": int(d.get("multiplier") or 1),
                "client_names": names,
            }
        )
    return {"groups": groups}


@app.post("/delete_group")
async def delete_group(
    payload: Dict[str, Any] = Body(...),
    user_id: str = Header(..., alias="X-User-Id"),
) -> Dict[str, Any]:
    uid = require_user(user_id)
    groups = payload.get("groups") or payload.get("group_names") or []
    if isinstance(groups, str):
        groups = [groups]
    if not isinstance(groups, list) or not groups:
        raise HTTPException(status_code=400, detail="groups required")
    results: List[str] = []
    for g in groups:
        rel = f"users/{_safe(uid)}/groups/{_safe(str(g))}.json"
        before = _store_read_json(rel)
        if before:
            _store_delete(rel)
            results.append(f"✅ Deleted group: {g}")
        else:
            results.append(f"⚠️ Not found: {g}")
    return {"message": "\n".join(results)}


###############################################################################
# Copy setups (user scoped)
###############################################################################

@app.post("/save_copytrading_setup")
async def save_copytrading_setup(
    payload: Dict[str, Any] = Body(...),
    user_id: str = Header(..., alias="X-User-Id"),
) -> Dict[str, Any]:
    uid = require_user(user_id)
    name = (payload.get("name") or "").strip()
    master = _pick(payload.get("master"), payload.get("master_id"))
    children = payload.get("children") or []
    multipliers = payload.get("multipliers") or {}

    if not name or not master or not isinstance(children, list) or not children:
        raise HTTPException(status_code=400, detail="name, master, children required")

    setup_id = _safe(payload.get("setup_id") or name) + "_" + datetime.utcnow().strftime("%Y%m%d%H%M%S")
    doc = dict(payload)
    doc["setup_id"] = setup_id
    doc["enabled"] = bool(doc.get("enabled", False))
    doc["updated_at"] = datetime.utcnow().isoformat()

    rel = f"users/{_safe(uid)}/copy_setups/{_safe(setup_id)}.json"
    _store_write_json(rel, doc)
    return {"success": True, "message": "Setup saved!", "setup_id": setup_id}


@app.get("/list_copytrading_setups")
def list_copytrading_setups(
    user_id: str = Header(..., alias="X-User-Id"),
) -> Dict[str, Any]:
    uid = require_user(user_id)
    rel_dir = f"users/{_safe(uid)}/copy_setups"
    setups: List[Dict[str, Any]] = []
    for fn in _store_list_json(rel_dir):
        doc = _store_read_json(f"{rel_dir}/{fn}") or {}
        setup_id = doc.get("setup_id") or os.path.splitext(fn)[0]
        setups.append(
            {
                "setup_id": setup_id,
                "name": doc.get("name", ""),
                "master": doc.get("master", ""),
                "children": doc.get("children", []),
                "enabled": bool(doc.get("enabled", False)),
            }
        )
    return {"setups": setups}


@app.post("/delete_copy_setup")
async def delete_copy_setup(
    payload: Dict[str, Any] = Body(...),
    user_id: str = Header(..., alias="X-User-Id"),
) -> Dict[str, Any]:
    uid = require_user(user_id)
    setup_id = _pick(payload.get("setup_id"), payload.get("id"))
    if not setup_id:
        raise HTTPException(status_code=400, detail="setup_id required")
    rel = f"users/{_safe(uid)}/copy_setups/{_safe(setup_id)}.json"
    before = _store_read_json(rel)
    if not before:
        raise HTTPException(status_code=404, detail="Setup not found")
    _store_delete(rel)
    return {"success": True, "message": "Setup deleted"}


@app.post("/enable_copy_setup")
async def enable_copy_setup(
    payload: Dict[str, Any] = Body(...),
    user_id: str = Header(..., alias="X-User-Id"),
) -> Dict[str, Any]:
    uid = require_user(user_id)
    setup_id = _pick(payload.get("setup_id"), payload.get("id"))
    rel = f"users/{_safe(uid)}/copy_setups/{_safe(setup_id)}.json"
    doc = _store_read_json(rel)
    if not doc:
        raise HTTPException(status_code=404, detail="Setup not found")
    doc["enabled"] = True
    doc["updated_at"] = datetime.utcnow().isoformat()
    _store_write_json(rel, doc)
    return {"success": True, "message": "Setup enabled."}


@app.post("/disable_copy_setup")
async def disable_copy_setup(
    payload: Dict[str, Any] = Body(...),
    user_id: str = Header(..., alias="X-User-Id"),
) -> Dict[str, Any]:
    uid = require_user(user_id)
    setup_id = _pick(payload.get("setup_id"), payload.get("id"))
    rel = f"users/{_safe(uid)}/copy_setups/{_safe(setup_id)}.json"
    doc = _store_read_json(rel)
    if not doc:
        raise HTTPException(status_code=404, detail="Setup not found")
    doc["enabled"] = False
    doc["updated_at"] = datetime.utcnow().isoformat()
    _store_write_json(rel, doc)
    return {"success": True, "message": "Setup disabled."}


###############################################################################
# Trading endpoints (user scoped)
###############################################################################

@app.post("/place_order")
async def place_order(
    payload: Dict[str, Any] = Body(...),
    user_id: str = Header(..., alias="X-User-Id"),
) -> Dict[str, Any]:
    uid = require_user(user_id)

    symbol = payload.get("symbol") or ""
    try:
        exchange, stock_symbol, symboltoken = symbol.split("|")
    except Exception:
        raise HTTPException(status_code=400, detail="symbol must be 'EXCHANGE|SYMBOL|TOKEN'")
    symboltoken = _safe_int_token(symboltoken)

    # Shared fields
    groupacc = bool(payload.get("groupacc", False))
    groups = payload.get("groups", []) or []
    clients = payload.get("clients", []) or []
    diffQty = bool(payload.get("diffQty", False))
    multiplier_flag = bool(payload.get("multiplier", False))
    qtySelection = payload.get("qtySelection", "manual")
    quantityinlot = int(payload.get("quantityinlot", 0) or 0)
    perClientQty = payload.get("perClientQty", {}) or {}
    perGroupQty = payload.get("perGroupQty", {}) or {}
    action = payload.get("action")
    ordertype = payload.get("ordertype")
    producttype = payload.get("producttype")
    orderduration = payload.get("orderduration")
    exchange_val = payload.get("exchange") or exchange
    price = float(payload.get("price", 0) or 0)
    triggerprice = float(payload.get("triggerprice", 0) or 0)
    disclosedquantity = int(payload.get("disclosedquantity", 0) or 0)
    amoorder = payload.get("amoorder", "N") or "N"

    responses: Dict[str, Any] = {}
    threads: List[threading.Thread] = []
    thread_lock = threading.Lock()

    sessions = _sessions_for_user(uid)

    def _session_for_clientid(client_id: str) -> Optional[Tuple[str, MOFSLOPENAPI, str]]:
        # find session by userid in this user's sessions
        for nm, (Mofsl, cid) in sessions.items():
            if cid == client_id:
                return nm, Mofsl, cid
        return None

    def place_order_for_client(tag: Optional[str], client_id: str, this_qty: int) -> None:
        sess = _session_for_clientid(client_id)
        if not sess:
            with thread_lock:
                responses[f"{tag}:{client_id}" if tag else client_id] = {"status": "ERROR", "message": "Session not found"}
            return
        _nm, Mofsl, _cid = sess
        order_payload = {
            "clientcode": client_id,
            "exchange": str(exchange_val).upper(),
            "symboltoken": symboltoken,
            "buyorsell": action,
            "ordertype": ordertype,
            "producttype": producttype,
            "orderduration": orderduration,
            "price": price,
            "triggerprice": triggerprice,
            "quantityinlot": int(this_qty),
            "disclosedquantity": disclosedquantity,
            "amoorder": amoorder,
            "algoid": "",
            "goodtilldate": "",
            "tag": tag or "",
        }
        try:
            resp = Mofsl.PlaceOrder(order_payload)
        except Exception as e:
            resp = {"status": "ERROR", "message": str(e)}
        with thread_lock:
            responses[f"{tag}:{client_id}" if tag else client_id] = resp

    if groupacc:
        for group_name in groups:
            g_rel = f"users/{_safe(uid)}/groups/{_safe(str(group_name))}.json"
            gdoc = _store_read_json(g_rel)
            if not gdoc:
                responses[str(group_name)] = {"status": "ERROR", "message": f"Group not found: {group_name}"}
                continue
            group_clients = gdoc.get("clients") or gdoc.get("members") or []
            group_multiplier = int(gdoc.get("multiplier") or 1)
            for client_id in group_clients:
                client_id = str(client_id)
                if qtySelection == "auto":
                    qty = auto_qty(uid, client_id, price)
                elif diffQty:
                    qty = int(perGroupQty.get(str(group_name), 0) or 0)
                elif multiplier_flag:
                    qty = quantityinlot * group_multiplier
                else:
                    qty = quantityinlot
                threads.append(threading.Thread(target=place_order_for_client, args=(str(group_name), client_id, qty)))
    else:
        for client_id in clients:
            client_id = str(client_id)
            if qtySelection == "auto":
                qty = auto_qty(uid, client_id, price)
            elif diffQty:
                qty = int(perClientQty.get(str(client_id), 0) or 0)
            else:
                qty = quantityinlot
            threads.append(threading.Thread(target=place_order_for_client, args=(None, client_id, qty)))

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    return {"status": "completed", "order_responses": responses}


@app.get("/get_orders")
def get_orders(
    user_id: str = Header(..., alias="X-User-Id"),
) -> Dict[str, Any]:
    uid = require_user(user_id)
    orders_data = OrderedDict({"pending": [], "traded": [], "rejected": [], "cancelled": [], "others": []})

    for name, (Mofsl, client_id) in _sessions_for_user(uid).items():
        try:
            today_stamp = datetime.now().strftime("%d-%b-%Y 09:00:00")
            order_book_info = {"clientcode": client_id, "datetimestamp": today_stamp}
            response = Mofsl.GetOrderBook(order_book_info)
            orders = (response or {}).get("data", []) if isinstance(response, dict) else []
            if not isinstance(orders, list):
                orders = []
            for order in orders:
                od = {
                    "name": name,
                    "symbol": order.get("symbol", ""),
                    "transaction_type": order.get("buyorsell", ""),
                    "quantity": order.get("orderqty", ""),
                    "price": order.get("price", ""),
                    "status": order.get("orderstatus", ""),
                    "order_id": order.get("uniqueorderid", ""),
                }
                status = (order.get("orderstatus", "") or "").lower()
                if "confirm" in status:
                    orders_data["pending"].append(od)
                elif "traded" in status:
                    orders_data["traded"].append(od)
                elif "rejected" in status or "error" in status:
                    orders_data["rejected"].append(od)
                elif "cancel" in status:
                    orders_data["cancelled"].append(od)
                else:
                    orders_data["others"].append(od)
        except Exception as e:
            print(f"[orders] error for {uid}:{name} {e}")

    return dict(orders_data)


@app.get("/get_positions")
def get_positions(
    user_id: str = Header(..., alias="X-User-Id"),
) -> Dict[str, Any]:
    uid = require_user(user_id)
    positions_data = {"open": [], "closed": []}

    # clear only this user's meta
    for k in [k for k in position_meta.keys() if k[0] == uid]:
        position_meta.pop(k, None)

    for name, (Mofsl, client_id) in _sessions_for_user(uid).items():
        try:
            response = Mofsl.GetPosition()
            if isinstance(response, dict) and response.get("status") != "SUCCESS":
                continue
            positions = (response or {}).get("data", []) if isinstance(response, dict) else []
            if not isinstance(positions, list):
                positions = []
            for pos in positions:
                quantity = (pos.get("buyquantity", 0) or 0) - (pos.get("sellquantity", 0) or 0)
                booked_profit = pos.get("bookedprofitloss", 0) or 0
                buy_avg = (pos.get("buyamount", 0) / max(1, (pos.get("buyquantity", 1) or 1))) if (pos.get("buyquantity", 0) or 0) > 0 else 0
                sell_avg = (pos.get("sellamount", 0) / max(1, (pos.get("sellquantity", 1) or 1))) if (pos.get("sellquantity", 0) or 0) > 0 else 0

                ltp = pos.get("LTP", 0) or 0
                net_profit = (
                    (ltp - buy_avg) * quantity if quantity > 0
                    else (sell_avg - buy_avg) * abs(quantity) if quantity < 0
                    else booked_profit
                )

                symbol = pos.get("symbol", "") or ""
                exchange = pos.get("exchange", "") or ""
                symboltoken = pos.get("symboltoken", "") or ""
                producttype = pos.get("productname", "") or ""

                if quantity != 0:
                    position_meta[(uid, name, symbol)] = {"exchange": exchange, "symboltoken": symboltoken, "producttype": producttype}

                row = {"name": name, "symbol": symbol, "quantity": quantity, "buy_avg": round(buy_avg, 2), "sell_avg": round(sell_avg, 2), "net_profit": round(net_profit, 2)}
                if quantity == 0:
                    positions_data["closed"].append(row)
                else:
                    positions_data["open"].append(row)
        except Exception as e:
            print(f"[positions] error for {uid}:{name} {e}")

    return positions_data


@app.post("/cancel_order")
async def cancel_order(
    payload: Dict[str, Any] = Body(...),
    user_id: str = Header(..., alias="X-User-Id"),
) -> Dict[str, Any]:
    uid = require_user(user_id)
    orders = payload.get("orders", []) or []
    if not orders:
        raise HTTPException(status_code=400, detail="No orders received for cancellation.")

    response_messages: List[str] = []
    threads: List[threading.Thread] = []
    thread_lock = threading.Lock()

    sessions = _sessions_for_user(uid)

    def cancel_single(order: Dict[str, Any]) -> None:
        name = order.get("name")
        order_id = order.get("order_id")
        if not name or not order_id:
            with thread_lock:
                response_messages.append(f"❌ Missing order fields: {order}")
            return
        sess = sessions.get(name)
        if not sess:
            with thread_lock:
                response_messages.append(f"❌ Session not found for: {name}")
            return
        Mofsl, client_id = sess
        try:
            resp = Mofsl.CancelOrder(order_id, client_id)
            msg = (resp.get("message", "") or "") if isinstance(resp, dict) else str(resp)
            if "cancel order request sent" in msg.lower():
                out = f"✅ Cancelled Order {order_id} for {name}"
            else:
                out = f"❌ Failed to cancel Order {order_id} for {name}: {msg}"
        except Exception as e:
            out = f"❌ Error cancelling {order_id} for {name}: {e}"
        with thread_lock:
            response_messages.append(out)

    for od in orders:
        t = threading.Thread(target=cancel_single, args=(od,))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

    return {"message": response_messages}


@app.post("/close_position")
async def close_position(
    payload: Dict[str, Any] = Body(...),
    user_id: str = Header(..., alias="X-User-Id"),
) -> Dict[str, Any]:
    uid = require_user(user_id)
    positions = payload.get("positions", []) or []
    if not isinstance(positions, list) or not positions:
        raise HTTPException(status_code=400, detail="positions list required")

    messages: List[str] = []
    threads: List[threading.Thread] = []
    thread_lock = threading.Lock()

    # min qty map
    min_qty_map: Dict[str, int] = {}
    try:
        conn = sqlite3.connect(SQLITE_DB)
        cur = conn.cursor()
        cur.execute("SELECT [Security ID], [Min Qty] FROM symbols")
        for sid, qty in cur.fetchall():
            if sid:
                min_qty_map[str(sid)] = int(qty) if qty else 1
        conn.close()
    except Exception:
        pass

    sessions = _sessions_for_user(uid)

    def close_one(pos: Dict[str, Any]) -> None:
        name = pos.get("name")
        symbol = pos.get("symbol")
        quantity = float(pos.get("quantity", 0) or 0)
        transaction_type = (pos.get("transaction_type") or "").upper()

        meta = position_meta.get((uid, str(name), str(symbol)))
        sess = sessions.get(str(name)) if name else None
        if not meta or not sess:
            with thread_lock:
                messages.append(f"❌ Missing data for {name} - {symbol}")
            return

        Mofsl, client_id = sess
        symboltoken = str(meta.get("symboltoken") or "")
        min_qty = min_qty_map.get(symboltoken, 1)
        lots = max(1, int(quantity // min_qty)) if min_qty > 0 else int(quantity)

        order = {
            "clientcode": client_id,
            "exchange": meta.get("exchange", "NSE"),
            "symboltoken": _safe_int_token(symboltoken),
            "buyorsell": transaction_type,
            "ordertype": "MARKET",
            "producttype": meta.get("producttype", "CNC"),
            "orderduration": "DAY",
            "price": 0,
            "triggerprice": 0,
            "quantityinlot": lots,
            "disclosedquantity": 0,
            "amoorder": "N",
            "algoid": "",
            "goodtilldate": "",
            "tag": "",
        }
        try:
            resp = Mofsl.PlaceOrder(order)
            ok = isinstance(resp, dict) and resp.get("status") == "SUCCESS"
            with thread_lock:
                messages.append(f"✅ Closed: {name} - {symbol}" if ok else f"❌ Failed: {name} - {symbol} - {resp}")
        except Exception as e:
            with thread_lock:
                messages.append(f"❌ Error for {name} - {symbol}: {e}")

    for p in positions:
        t = threading.Thread(target=close_one, args=(p,))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

    return {"message": messages}


@app.post("/convert_position")
async def convert_position(
    payload: Dict[str, Any] = Body(...),
    user_id: str = Header(..., alias="X-User-Id"),
) -> Dict[str, Any]:
    uid = require_user(user_id)
    items = payload.get("positions", []) or []
    if not isinstance(items, list) or not items:
        raise HTTPException(status_code=400, detail="No positions received for conversion.")

    messages: List[str] = []
    threads: List[threading.Thread] = []
    lock = threading.Lock()
    sessions = _sessions_for_user(uid)

    def convert_one(pos: Dict[str, Any]) -> None:
        name = (pos.get("name") or "").strip()
        symbol = (pos.get("symbol") or "").strip()
        quantity = int(pos.get("quantity") or 0)
        req_exchange = (pos.get("exchange") or "NSE").upper()
        oldproduct = (pos.get("oldproduct") or "NORMAL").upper()
        newproduct = (pos.get("newproduct") or "DELIVERY").upper()

        meta = position_meta.get((uid, name, symbol))
        sess = sessions.get(name)
        if not meta or not sess:
            with lock:
                messages.append(f"❌ Missing data for {name} - {symbol} (no session/meta)")
            return

        Mofsl, client_id = sess
        scripcode = _safe_int_token(meta.get("symboltoken"))
        exchange = (meta.get("exchange") or req_exchange).upper()

        info = {"clientcode": client_id, "exchange": exchange, "scripcode": int(scripcode), "quantity": int(quantity), "oldproduct": oldproduct, "newproduct": newproduct}
        try:
            resp = Mofsl.PositionConversion(info)
            ok = isinstance(resp, dict) and resp.get("status") == "SUCCESS"
            with lock:
                messages.append(f"✅ Converted {name} · {symbol} · {oldproduct}→{newproduct} · qty {quantity}" if ok else f"❌ Failed {name} · {symbol}: {resp}")
        except Exception as e:
            with lock:
                messages.append(f"❌ Error {name} · {symbol}: {e}")

    for p in items:
        t = threading.Thread(target=convert_one, args=(p,))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

    return {"message": messages}


def get_available_margin(Mofsl: MOFSLOPENAPI, clientcode: str) -> float:
    try:
        resp = Mofsl.GetReportMarginSummary(clientcode)
        if not isinstance(resp, dict) or resp.get("status") != "SUCCESS":
            return 0.0
        for item in resp.get("data", []) or []:
            if item.get("particulars") == "Total Available Margin for Cash":
                return float(item.get("amount", 0) or 0)
    except Exception:
        pass
    return 0.0


@app.get("/get_holdings")
def get_holdings(
    user_id: str = Header(..., alias="X-User-Id"),
) -> Dict[str, Any]:
    uid = require_user(user_id)
    holdings_data: List[Dict[str, Any]] = []
    summary_data: Dict[str, Dict[str, Any]] = {}

    sessions = _sessions_for_user(uid)

    for name, (Mofsl, client_id) in sessions.items():
        try:
            resp = Mofsl.GetDPHolding(client_id)
            if not isinstance(resp, dict) or resp.get("status") != "SUCCESS":
                continue

            holdings = resp.get("data", []) or []
            invested = 0.0
            total_pnl = 0.0

            for h in holdings:
                symbol = (h.get("scripname", "") or "").strip()
                quantity = float(h.get("dpquantity", 0) or 0)
                buy_avg = float(h.get("buyavgprice", 0) or 0)
                scripcode = h.get("nsesymboltoken")
                if not scripcode or quantity <= 0:
                    continue

                ltp_req = {"clientcode": client_id, "exchange": "NSE", "scripcode": int(scripcode)}
                ltp_resp = Mofsl.GetLtp(ltp_req) or {}
                ltp = float(((ltp_resp.get("data") or {}).get("ltp", 0) or 0)) / 100.0

                pnl = round((ltp - buy_avg) * quantity, 2)
                invested += quantity * buy_avg
                total_pnl += pnl

                holdings_data.append({"name": name, "symbol": symbol, "quantity": quantity, "buy_avg": round(buy_avg, 2), "ltp": round(ltp, 2), "pnl": pnl})

            capital = _get_client_capital(uid, client_id)
            current_value = invested + total_pnl
            available_margin = get_available_margin(Mofsl, client_id)
            net_gain = round((current_value + available_margin) - capital, 2)

            summary_data[name] = {
                "name": name,
                "capital": round(capital, 2),
                "invested": round(invested, 2),
                "pnl": round(total_pnl, 2),
                "current_value": round(current_value, 2),
                "available_margin": round(available_margin, 2),
                "net_gain": net_gain,
            }
        except Exception as e:
            print(f"[holdings] error for {uid}:{name} {e}")

    summary_data_global[uid] = summary_data
    return {"holdings": holdings_data, "summary": list(summary_data.values())}


@app.get("/get_summary")
def get_summary(
    user_id: str = Header(..., alias="X-User-Id"),
) -> Dict[str, Any]:
    uid = require_user(user_id)
    data = summary_data_global.get(uid, {}) or {}
    return {"summary": list(data.values())}



