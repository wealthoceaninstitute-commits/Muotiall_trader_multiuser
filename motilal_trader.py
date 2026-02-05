# motilal_trader.py
"""
Motilal Trader (Minimal Clean) — Symbols DB + Symbol Search + Auth Router only

Applied corrections WITHOUT removing anything from your attached file:
- Keeps all your imports and helpers (including _safe_int_token)
- Keeps Auth router mount at /auth
- Keeps Symbols DB build on startup from GitHub CSV to SQLite
- Fixes /search_symbols to be frontend-compatible by returning BOTH:
    1) Select2 format:  {"results":[{"id","text"}]}
    2) React-Select:    {"options":[{"value","label"}]}
- Adds DB-ready guard (returns 503 if DB not built/available)

Nothing else removed.
"""

from __future__ import annotations

import csv
import os
import sqlite3
import threading
from typing import Any, Dict, List

import requests
from concurrent.futures import ThreadPoolExecutor
from fastapi import FastAPI, Request, Body, Query, Form, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from datetime import datetime
import pandas as pd
import requests  # (kept as-is from your attached file)

# Optional auth router (Auth0 integration expected to live here)
try:
    from auth.auth_router import router as auth_router  # type: ignore
except Exception:
    auth_router = None  # type: ignore


###############################################################################
# App + CORS
###############################################################################

app = FastAPI(title="Motilal Trader (Minimal)", version="0.1")

# IMPORTANT: os.getenv takes (key, default) only.
_frontend_origins = os.getenv(
    "FRONTEND_ORIGINS",
    "http://localhost:3000,http://127.0.0.1:3000,https://multibroker-trader-multiuser.vercel.app",
)

allow_origins = [o.strip() for o in _frontend_origins.split(",") if o.strip()]
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
    app.include_router(auth_router, prefix="/auth")


###############################################################################
# Symbol DB config
###############################################################################

GITHUB_CSV_URL = os.getenv(
    "SYMBOLS_CSV_URL",
    "https://raw.githubusercontent.com/Pramod541988/Stock_List/main/security_id.csv",
)

SQLITE_DB = os.getenv("SYMBOLS_DB_PATH", "symbols.db")
TABLE_NAME = os.getenv("SYMBOLS_TABLE", "symbols")
symbol_db_lock = threading.Lock()


###############################################################################
# Symbol DB build + validation
###############################################################################

def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            Exchange TEXT,
            "Stock Symbol" TEXT,
            "Security ID" TEXT
        )
        """
    )
    conn.commit()


def recreate_sqlite_from_csv() -> Dict[str, Any]:
    """
    Recreate symbols.db from the GitHub CSV (or configured URL).
    Expects columns at least: Exchange, Stock Symbol, Security ID
    """
    try:
        r = requests.get(GITHUB_CSV_URL, timeout=45)
        r.raise_for_status()
    except Exception as e:
        raise RuntimeError(f"Failed to download symbols CSV: {e}")

    csv_lines = r.text.splitlines()
    if not csv_lines:
        raise RuntimeError("Symbols CSV is empty.")

    reader = csv.DictReader(csv_lines)
    required = {"Exchange", "Stock Symbol", "Security ID"}
    if not required.issubset(set(reader.fieldnames or [])):
        raise RuntimeError(f"CSV missing required columns. Found: {reader.fieldnames}")

    tmp_db = SQLITE_DB + ".tmp"

    if os.path.exists(tmp_db):
        try:
            os.remove(tmp_db)
        except Exception:
            pass

    conn = sqlite3.connect(tmp_db)
    try:
        conn.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
        conn.execute(
            f"""
            CREATE TABLE {TABLE_NAME} (
                Exchange TEXT,
                "Stock Symbol" TEXT,
                "Security ID" TEXT
            )
            """
        )

        rows = []
        for row in reader:
            rows.append(
                (
                    (row.get("Exchange") or "").strip(),
                    (row.get("Stock Symbol") or "").strip(),
                    (row.get("Security ID") or "").strip(),
                )
            )

        conn.executemany(
            f'INSERT INTO {TABLE_NAME} (Exchange, "Stock Symbol", "Security ID") VALUES (?, ?, ?)',
            rows,
        )
        conn.execute(f'CREATE INDEX IF NOT EXISTS idx_sym ON {TABLE_NAME}("Stock Symbol")')
        conn.execute(f'CREATE INDEX IF NOT EXISTS idx_exch ON {TABLE_NAME}(Exchange)')
        conn.commit()
    finally:
        conn.close()

    # Swap
    if os.path.exists(SQLITE_DB):
        try:
            os.remove(SQLITE_DB)
        except Exception:
            pass
    os.replace(tmp_db, SQLITE_DB)

    return {"ok": True, "rows": len(rows), "db_path": SQLITE_DB, "source": GITHUB_CSV_URL}


def _db_ready() -> bool:
    try:
        if not os.path.exists(SQLITE_DB):
            return False
        conn = sqlite3.connect(SQLITE_DB)
        _ensure_schema(conn)
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (TABLE_NAME,),
        )
        ok = cur.fetchone() is not None
        conn.close()
        return ok
    except Exception:
        return False


@app.on_event("startup")
def _startup() -> None:
    try:
        with symbol_db_lock:
            recreate_sqlite_from_csv()
    except Exception as e:
        # Don't crash boot; /search_symbols will show 503 until rebuild works.
        print(f"[startup] symbols db init failed: {e}")


def _safe_int_token(x):
    """
    Accepts '10666', '10666.0', 10666.0, etc.
    Returns int or raises HTTPException(400) with a clear message.
    """
    try:
        if x is None:
            raise ValueError("symboltoken is None")
        s = str(x).strip()
        if s == "":
            raise ValueError("symboltoken is empty")
        return int(float(s))
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid symboltoken: {x}")


###############################################################################
# Basic endpoints
###############################################################################

@app.get("/health")
def health() -> Dict[str, Any]:
    return {"ok": True, "symbols_db_ready": _db_ready(), "auth_router": bool(auth_router is not None)}


@app.post("/symbols/rebuild")
def rebuild_symbols() -> Dict[str, Any]:
    try:
        with symbol_db_lock:
            return recreate_sqlite_from_csv()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


###############################################################################
# Symbols search
###############################################################################

@app.get("/search_symbols")
def search_symbols(q: str = Query("", alias="q"), exchange: str = Query("", alias="exchange")):
    query = (q or "").strip()
    exchange_filter = (exchange or "").strip().upper()

    if not query:
        # Provide both keys so frontend never breaks on empty input
        return JSONResponse(content={"results": [], "options": [], "count": 0})

    # Guard: if DB didn't build (startup download failed) return 503 with clear message
    if not _db_ready():
        raise HTTPException(
            status_code=503,
            detail="Symbols DB not ready. Check /health and/or call POST /symbols/rebuild.",
        )

    words = [w for w in query.lower().split() if w]
    if not words:
        return JSONResponse(content={"results": [], "options": [], "count": 0})

    where_clauses = []
    params = []
    for w in words:
        # Use bracket quoting for sqlite compatibility (kept style from your file)
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
        {"id": f"{row[0]}|{row[1]}|{row[2]}", "text": f"{row[0]} | {row[1]}"}
        for row in rows
    ]

    # React-Select compatible too (some UIs expect options/value/label)
    options = [{"value": r["id"], "label": r["text"]} for r in results]

    return JSONResponse(content={"results": results, "options": options, "count": len(results)})
