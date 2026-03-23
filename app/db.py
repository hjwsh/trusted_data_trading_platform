from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Iterable

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / 'platform.db'


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA foreign_keys = ON;')
    return conn


def init_db() -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.executescript(
        '''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            role TEXT NOT NULL,
            org TEXT,
            reputation REAL DEFAULT 0.70,
            is_active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS attribute_applications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            requested_attrs TEXT NOT NULL,
            purpose TEXT,
            status TEXT NOT NULL DEFAULT 'PENDING',
            reviewed_by INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            reviewed_at TEXT,
            FOREIGN KEY(user_id) REFERENCES users(id),
            FOREIGN KEY(reviewed_by) REFERENCES users(id)
        );

        CREATE TABLE IF NOT EXISTS user_attributes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            attr_name TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'ACTIVE',
            issued_by INTEGER,
            issued_at TEXT DEFAULT CURRENT_TIMESTAMP,
            revoked_at TEXT,
            UNIQUE(user_id, attr_name),
            FOREIGN KEY(user_id) REFERENCES users(id),
            FOREIGN KEY(issued_by) REFERENCES users(id)
        );

        CREATE TABLE IF NOT EXISTS assets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            seller_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            category TEXT NOT NULL,
            description TEXT,
            scenario TEXT NOT NULL,
            quality_json TEXT NOT NULL,
            trust_score REAL NOT NULL,
            encrypted_path TEXT NOT NULL,
            plain_preview TEXT,
            file_hash TEXT NOT NULL,
            required_attrs TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(seller_id) REFERENCES users(id)
        );

        CREATE TABLE IF NOT EXISTS auth_templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            duration_days INTEGER NOT NULL,
            download_limit INTEGER NOT NULL,
            scope_factor REAL NOT NULL,
            scenario_factor REAL NOT NULL,
            price REAL NOT NULL,
            is_active INTEGER DEFAULT 1,
            FOREIGN KEY(asset_id) REFERENCES assets(id)
        );

        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            buyer_id INTEGER NOT NULL,
            seller_id INTEGER NOT NULL,
            asset_id INTEGER NOT NULL,
            template_id INTEGER NOT NULL,
            price REAL NOT NULL,
            status TEXT NOT NULL,
            download_count INTEGER DEFAULT 0,
            integrity_ok INTEGER,
            note TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(buyer_id) REFERENCES users(id),
            FOREIGN KEY(seller_id) REFERENCES users(id),
            FOREIGN KEY(asset_id) REFERENCES assets(id),
            FOREIGN KEY(template_id) REFERENCES auth_templates(id)
        );

        CREATE TABLE IF NOT EXISTS feedback (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER NOT NULL,
            buyer_id INTEGER NOT NULL,
            seller_id INTEGER NOT NULL,
            rating REAL NOT NULL,
            comment TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(order_id) REFERENCES orders(id)
        );

        CREATE TABLE IF NOT EXISTS audit_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            actor_id INTEGER,
            action TEXT NOT NULL,
            target_type TEXT NOT NULL,
            target_id INTEGER,
            result TEXT NOT NULL,
            details_json TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(actor_id) REFERENCES users(id)
        );

        CREATE TABLE IF NOT EXISTS ledger (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            prev_hash TEXT,
            block_hash TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        '''
    )
    conn.commit()
    conn.close()


def reset_db() -> None:
    if DB_PATH.exists():
        DB_PATH.unlink()
    init_db()


def q(query: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
    conn = get_conn()
    try:
        return conn.execute(query, params).fetchall()
    finally:
        conn.close()


def execute(query: str, params: Iterable[Any] = ()) -> int:
    conn = get_conn()
    try:
        cur = conn.execute(query, params)
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


def execute_many(statements: list[tuple[str, tuple[Any, ...]]]) -> None:
    conn = get_conn()
    try:
        for query, params in statements:
            conn.execute(query, params)
        conn.commit()
    finally:
        conn.close()


def row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {k: row[k] for k in row.keys()}


def json_load(text: str | None, default: Any) -> Any:
    if not text:
        return default
    return json.loads(text)
