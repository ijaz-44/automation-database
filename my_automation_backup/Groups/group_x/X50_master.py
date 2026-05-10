#!/usr/bin/env python3
"""
X50_master.py – Master Aggregator (SQLite → TSV, always returns string)
- Exports all tables to a single TSV file (with section headers).
- Returns the TSV content as a string.
- Never returns None; always returns a string (error message or TSV).
"""

import os
import sys
import sqlite3
import time
import threading
import traceback

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SYMBOLS_DIR = os.path.join(BASE_DIR, "market_data", "binance", "symbols")
os.makedirs(SYMBOLS_DIR, exist_ok=True)

LOG_FILE = os.path.join(SYMBOLS_DIR, "x50_master.log")
_lock = threading.Lock()

def log(msg, level="INFO"):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"{timestamp} [{level}] [X50] {msg}"
    print(line)
    try:
        with _lock:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(line + "\n")
    except Exception:
        pass

def get_db_path(symbol, module_name):
    if module_name == "candles":
        return os.path.join(SYMBOLS_DIR, f"{symbol.lower()}.db")
    else:
        return os.path.join(SYMBOLS_DIR, f"{symbol.lower()}_{module_name}.db")

def table_exists(conn, table_name):
    try:
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        return cur.fetchone() is not None
    except Exception:
        return False

def get_table_schema(conn, table_name):
    try:
        cur = conn.execute(f"PRAGMA table_info({table_name})")
        return [row[1] for row in cur.fetchall()]
    except Exception:
        return []

def copy_table_safe(src_conn, dst_conn, src_table, dst_table):
    try:
        if not table_exists(src_conn, src_table):
            return
        if src_table in ("meta", "run_checksums", "sqlite_sequence"):
            return
        columns = get_table_schema(src_conn, src_table)
        if not columns:
            return
        col_names = ", ".join(columns)
        placeholders = ", ".join(["?"] * len(columns))
        text_cols = {'symbol', 'timeframe', 'timestamp_ms', 'swing_type', 'direction',
                     'volatility_regime', 'trend_structure', 'type', 'level_name',
                     'side', 'key', 'value', 'date', 'status', 'message'}
        create_cols = []
        for c in columns:
            if c in text_cols:
                create_cols.append(f"{c} TEXT")
            else:
                create_cols.append(f"{c} REAL")
        create_sql = f"CREATE TABLE IF NOT EXISTS {dst_table} ({', '.join(create_cols)})"
        dst_conn.execute(create_sql)
        cur = src_conn.execute(f"SELECT {col_names} FROM {src_table}")
        rows = cur.fetchall()
        if rows:
            dst_conn.executemany(f"INSERT INTO {dst_table} ({col_names}) VALUES ({placeholders})", rows)
            dst_conn.commit()
        log(f"  Copied {len(rows)} rows from {src_table} to {dst_table}")
    except Exception as e:
        log(f"Error copying {src_table}: {e}", "ERROR")

def export_db_to_tsv(db_path, output_tsv):
    """Export all tables in SQLite DB to a TSV file (with table headers)."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").fetchall()
    with open(output_tsv, "w", encoding="utf-8") as f:
        for table in tables:
            table_name = table[0]
            rows = conn.execute(f"SELECT * FROM {table_name}").fetchall()
            if not rows:
                continue
            f.write(f"#TABLE: {table_name}\n")
            col_names = list(rows[0].keys())
            f.write("\t".join(col_names) + "\n")
            for row in rows:
                values = [str(v) if v is not None else "" for v in row]
                f.write("\t".join(values) + "\n")
            f.write("\n")
    conn.close()
    log(f"Exported to {output_tsv}")

def generate_master(symbol):
    """Returns TSV content as string (never None)."""
    log(f"Starting generate_master for {symbol}")
    try:
        master_db = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}_master.db")
        tsv_file = os.path.join(SYMBOLS_DIR, f"{symbol.lower()}_master.tsv")
        if os.path.exists(master_db):
            os.remove(master_db)
        modules = [
            ("candles", "candles"),
            ("cvd", "cvd"),
            ("depth", "depth"),
            ("derivative", "derivative"),
            ("correlation", "correlation"),
            ("macro", "macro"),
            ("liquidations", "liquidations"),
            ("sessions", "sessions"),
            ("sentiment", "sentiment"),
            ("volProfile", "volProfile"),
            ("mstructure", "mstructure"),
            ("onchain", "onchain"),
            ("tick", "tick"),
        ]
        master_conn = sqlite3.connect(master_db)
        master_conn.execute("PRAGMA journal_mode=WAL")
        for src_name, prefix in modules:
            src_db = get_db_path(symbol, src_name)
            if not os.path.exists(src_db):
                log(f"Warning: {src_db} not found, skipping")
                continue
            log(f"Reading {src_db}")
            src_conn = sqlite3.connect(src_db)
            src_conn.row_factory = sqlite3.Row
            tables = src_conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").fetchall()
            for tbl in tables:
                src_table = tbl[0]
                dst_table = f"{prefix}_{src_table}"
                copy_table_safe(src_conn, master_conn, src_table, dst_table)
            src_conn.close()
        master_conn.close()
        export_db_to_tsv(master_db, tsv_file)
        with open(tsv_file, "r", encoding="utf-8") as f:
            content = f.read()
        if not content.strip():
            content = "No data found for this symbol."
        log(f"✅ Master TSV for {symbol} generated ({len(content)} chars).")
        return content
    except Exception as e:
        log(f"❌ Exception: {e}", "ERROR")
        log(traceback.format_exc(), "ERROR")
        return f"Error generating master: {str(e)}"

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python X50_master.py SYMBOL")
        sys.exit(1)
    symbol = sys.argv[1].upper()
    result = generate_master(symbol)
    print(result[:500] if result else "None")