# motilal_trader_minimal.py
"""
Motilal Trader (Minimal Clean) — Symbols DB + Symbol Search + Auth Router only

Kept from your v1.1 backend:
- SQLite symbols DB created from GitHub CSV on startup
- /search_symbols endpoint (used by frontend dropdown / search)
- Auth router mounted (your Auth0 integration via auth.auth_router), if present

Removed:
- Clients, groups, copy trading, orders, positions, holdings, any Motilal SDK session logic

Notes
- This backend still expects the symbol master CSV at:
    https://raw.githubusercontent.com/Pramod541988/Stock_List/main/security_id.csv
- If your auth_router already validates Auth0 and returns the logged-in user,
  you can keep using it unchanged.
"""

from __future__ import annotations

import csv
import os
import sqlite3
import threading
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# Optional auth router (Auth0 integration expected to live here)
try:
    from auth.auth_router import router as auth_router  # type: ignore
except Exception:
    auth_router = None  # type: ignore


###############################################################################
# App + CORS
###############################################################################

app = FastAPI(title="Motilal Trader (Minimal)", version="0.1")

_frontend_origins = os.environ.get(
    "FRONTEND_ORIGINS",
    "http://localhost:3000,http://127.0.0.1:3000",
    "https://multibroker-trader-multiuser.vercel.app"
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
# (Keeps your Auth0 login endpoints exactly as-is)
if auth_router is not None:
    app.include_router(auth_router, prefix="/auth")


###############################################################################
# Symbol DB config
###############################################################################

GITHUB_CSV_URL = os.environ.get(
    "SYMBOLS_CSV_URL",
    "https://raw.githubusercontent.com/Pramod541988/Stock_List/main/security_id.csv",
)

SQLITE_DB = os.environ.get("SYMBOLS_DB_PATH", "symbols.db")
TABLE_NAME = os.environ.get("SYMBOLS_TABLE", "symbols")
symbol_db_lock = threading.Lock()


###############################################################################
# Symbol DB build + validation
###############################################################################

def _ensure_schema(conn: sqlite3.Connection) -> None:
    # We create a fresh table each rebuild; but this helps validate an existing DB.
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
    Uses stdlib csv to keep deps minimal.
    Expects columns at least: Exchange, Stock Symbol, Security ID
    """
    try:
        r = requests.get(GITHUB_CSV_URL, timeout=45)
        r.raise_for_status()
    except Exception as e:
        raise RuntimeError(f"Failed to download symbols CSV: {e}")

    csv_text = r.text.splitlines()
    if not csv_text:
        raise RuntimeError("Symbols CSV is empty.")

    reader = csv.DictReader(csv_text)
    required = {"Exchange", "Stock Symbol", "Security ID"}
    if not required.issubset(set(reader.fieldnames or [])):
        raise RuntimeError(f"CSV missing required columns. Found: {reader.fieldnames}")

    # Build new DB atomically-ish: write to temp then replace
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
        cur = conn.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", (TABLE_NAME,))
        ok = cur.fetchone() is not None
        conn.close()
        return ok
    except Exception:
        return False


@app.on_event("startup")
def _startup() -> None:
    # Build symbol DB at startup. If it fails, search endpoint will return a friendly error.
    try:
        with symbol_db_lock:
            recreate_sqlite_from_csv()
    except Exception as e:
        print(f"[startup] symbols db init failed: {e}")


###############################################################################
# Basic endpoints
###############################################################################

@app.get("/health")
def health() -> Dict[str, Any]:
    return {"ok": True, "symbols_db_ready": _db_ready(), "auth_router": bool(auth_router is not None)}


@app.post("/symbols/rebuild")
def rebuild_symbols() -> Dict[str, Any]:
    """
    Manual rebuild endpoint (useful after you update the CSV in GitHub).
    """
    try:
        with symbol_db_lock:
            return recreate_sqlite_from_csv()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


###############################################################################
# Symbols search (for frontend select)
###############################################################################

@app.get("/search_symbols")
def search_symbols(
    q: str = Query("", alias="q"),
    exchange: str = Query("", alias="exchange"),
) -> JSONResponse:
    """
    Returns:
      { "results": [ { "id": "NSE|TCS|11536", "text": "NSE | TCS" }, ... ] }
    """
    query = (q or "").strip()
    exchange_filter = (exchange or "").strip().upper()

    if not query:
        return JSONResponse(content={"results": []})

    if not _db_ready():
        raise HTTPException(
            status_code=503,
            detail="Symbols DB not ready. Check /health and/or call POST /symbols/rebuild.",
        )

    words = [w for w in query.lower().split() if w]
    if not words:
        return JSONResponse(content={"results": []})

    where_clauses: List[str] = []
    params: List[Any] = []

    for w in words:
        where_clauses.append('LOWER("Stock Symbol") LIKE ?')
        params.append(f"%{w}%")

    where_sql = " AND ".join(where_clauses)
    if exchange_filter:
        where_sql += " AND UPPER(Exchange) = ?"
        params.append(exchange_filter)

    sql = f"""
        SELECT Exchange, "Stock Symbol", "Security ID"
        FROM {TABLE_NAME}
        WHERE {where_sql}
        ORDER BY "Stock Symbol"
        LIMIT 20
    """

    with symbol_db_lock:
        conn = sqlite3.connect(SQLITE_DB)
        cur = conn.execute(sql, params)
        rows = cur.fetchall()
        conn.close()

    results = [{"id": f"{row[0]}|{row[1]}|{row[2]}", "text": f"{row[0]} | {row[1]}"} for row in rows]
    return JSONResponse(content={"results": results})
