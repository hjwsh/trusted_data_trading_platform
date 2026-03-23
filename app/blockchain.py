from __future__ import annotations

import hashlib
import json
from typing import Any

from .db import execute, q


def _digest(prev_hash: str, event_type: str, payload_json: str, created_hint: str = '') -> str:
    seed = f'{prev_hash}|{event_type}|{payload_json}|{created_hint}'.encode('utf-8')
    return hashlib.sha256(seed).hexdigest()


def append_block(event_type: str, payload: dict[str, Any]) -> str:
    prev_row = q('SELECT block_hash FROM ledger ORDER BY id DESC LIMIT 1')
    prev_hash = prev_row[0]['block_hash'] if prev_row else 'GENESIS'
    payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    block_hash = _digest(prev_hash, event_type, payload_json)
    execute(
        'INSERT INTO ledger (event_type, payload_json, prev_hash, block_hash) VALUES (?, ?, ?, ?)',
        (event_type, payload_json, prev_hash, block_hash),
    )
    return block_hash


def verify_chain() -> dict[str, Any]:
    rows = q('SELECT * FROM ledger ORDER BY id ASC')
    prev_hash = 'GENESIS'
    for row in rows:
        expected = _digest(prev_hash, row['event_type'], row['payload_json'])
        if row['prev_hash'] != prev_hash or row['block_hash'] != expected:
            return {'ok': False, 'broken_at': row['id']}
        prev_hash = row['block_hash']
    return {'ok': True, 'blocks': len(rows)}
