from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from .blockchain import append_block, verify_chain
from .db import BASE_DIR, execute, get_conn, init_db, json_load, q, row_to_dict
from .pricing import compute_price
from .security import can_access, decrypt_bytes, encrypt_bytes, sha256_bytes

STORAGE_DIR = BASE_DIR / 'storage' / 'encrypted'
STORAGE_DIR.mkdir(parents=True, exist_ok=True)


def log_action(actor_id: int | None, action: str, target_type: str, target_id: int | None, result: str, details: dict[str, Any]) -> None:
    execute(
        'INSERT INTO audit_logs (actor_id, action, target_type, target_id, result, details_json) VALUES (?, ?, ?, ?, ?, ?)',
        (actor_id, action, target_type, target_id, result, json.dumps(details, ensure_ascii=False)),
    )


def get_user(user_id: int) -> dict[str, Any] | None:
    rows = q('SELECT * FROM users WHERE id = ?', (user_id,))
    return row_to_dict(rows[0]) if rows else None


def get_user_by_name(username: str) -> dict[str, Any] | None:
    rows = q('SELECT * FROM users WHERE username = ?', (username,))
    return row_to_dict(rows[0]) if rows else None


def register_user(username: str, password: str, role: str, org: str = '') -> dict[str, Any]:
    user_id = execute(
        'INSERT INTO users (username, password, role, org) VALUES (?, ?, ?, ?)',
        (username, password, role, org),
    )
    append_block('USER_REGISTERED', {'user_id': user_id, 'username': username, 'role': role})
    log_action(user_id, 'register', 'user', user_id, 'SUCCESS', {'role': role})
    return get_user(user_id)  # type: ignore[return-value]


def apply_attributes(user_id: int, attrs: list[str], purpose: str) -> dict[str, Any]:
    app_id = execute(
        'INSERT INTO attribute_applications (user_id, requested_attrs, purpose) VALUES (?, ?, ?)',
        (user_id, json.dumps(attrs, ensure_ascii=False), purpose),
    )
    append_block('ATTRIBUTE_APPLIED', {'application_id': app_id, 'user_id': user_id, 'attrs': attrs})
    log_action(user_id, 'apply_attributes', 'attribute_application', app_id, 'SUCCESS', {'attrs': attrs})
    rows = q('SELECT * FROM attribute_applications WHERE id = ?', (app_id,))
    return row_to_dict(rows[0])  # type: ignore[return-value]


def approve_attributes(application_id: int, reviewer_id: int) -> dict[str, Any]:
    conn = get_conn()
    try:
        app_row = conn.execute('SELECT * FROM attribute_applications WHERE id = ?', (application_id,)).fetchone()
        if app_row is None:
            raise ValueError('application not found')
        attrs = json.loads(app_row['requested_attrs'])
        conn.execute(
            "UPDATE attribute_applications SET status='APPROVED', reviewed_by=?, reviewed_at=CURRENT_TIMESTAMP WHERE id=?",
            (reviewer_id, application_id),
        )
        for attr in attrs:
            conn.execute(
                "INSERT INTO user_attributes (user_id, attr_name, status, issued_by) VALUES (?, ?, 'ACTIVE', ?) "
                "ON CONFLICT(user_id, attr_name) DO UPDATE SET status='ACTIVE', issued_by=excluded.issued_by, revoked_at=NULL",
                (app_row['user_id'], attr, reviewer_id),
            )
        conn.commit()
    finally:
        conn.close()
    append_block('ATTRIBUTE_APPROVED', {'application_id': application_id, 'reviewer_id': reviewer_id})
    log_action(reviewer_id, 'approve_attributes', 'attribute_application', application_id, 'SUCCESS', {'reviewer_id': reviewer_id})
    rows = q('SELECT * FROM attribute_applications WHERE id = ?', (application_id,))
    return row_to_dict(rows[0])  # type: ignore[return-value]


def revoke_user_attribute(user_id: int, attr_name: str, reviewer_id: int, reason: str) -> None:
    execute(
        "UPDATE user_attributes SET status='REVOKED', revoked_at=CURRENT_TIMESTAMP, issued_by=? WHERE user_id=? AND attr_name=?",
        (reviewer_id, user_id, attr_name),
    )
    append_block('ATTRIBUTE_REVOKED', {'user_id': user_id, 'attr_name': attr_name, 'reason': reason})
    log_action(reviewer_id, 'revoke_attribute', 'user_attribute', user_id, 'SUCCESS', {'attr_name': attr_name, 'reason': reason})


def active_attributes(user_id: int) -> list[str]:
    rows = q("SELECT attr_name FROM user_attributes WHERE user_id = ? AND status = 'ACTIVE'", (user_id,))
    return [r['attr_name'] for r in rows]


def seller_history(seller_id: int) -> tuple[list[float], float]:
    ratings_rows = q('SELECT rating FROM feedback WHERE seller_id = ?', (seller_id,))
    ratings = [float(r['rating']) for r in ratings_rows]
    total = q('SELECT COUNT(*) AS c FROM orders WHERE seller_id=?', (seller_id,))[0]['c']
    success = q("SELECT COUNT(*) AS c FROM orders WHERE seller_id=? AND status='COMPLETED'", (seller_id,))[0]['c']
    success_rate = float(success / total) if total else 1.0
    return ratings, success_rate


def publish_asset(
    seller_id: int,
    title: str,
    category: str,
    description: str,
    scenario: str,
    quality_metrics: dict[str, float],
    required_attrs: list[str],
    templates: list[dict[str, Any]],
    plain_bytes: bytes,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    metadata = metadata or {}
    seller = get_user(seller_id)
    if not seller or seller['role'] != 'seller':
        raise ValueError('seller invalid')
    encrypted = encrypt_bytes(plain_bytes)
    file_hash = sha256_bytes(plain_bytes)
    file_name = f'asset_{int(time.time()*1000)}_{seller_id}.bin'
    enc_path = STORAGE_DIR / file_name
    enc_path.write_bytes(encrypted)

    ratings, success_rate = seller_history(seller_id)
    base_price = float(metadata.get('base_price', 100.0))
    first_template = templates[0]
    price_probe = compute_price(
        base_price=base_price,
        quality_metrics=quality_metrics,
        reputation=float(seller['reputation']),
        ratings=ratings,
        success_rate=success_rate,
        scenario=scenario,
        duration_days=int(first_template['duration_days']),
        download_limit=int(first_template['download_limit']),
        scope_factor=float(first_template['scope_factor']),
    )
    asset_id = execute(
        '''INSERT INTO assets
        (seller_id, title, category, description, scenario, quality_json, trust_score, encrypted_path, plain_preview, file_hash, required_attrs, metadata_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        (
            seller_id,
            title,
            category,
            description,
            scenario,
            json.dumps(quality_metrics, ensure_ascii=False),
            price_probe['trust_score'],
            str(enc_path),
            plain_bytes.decode('utf-8', errors='ignore')[:120],
            file_hash,
            json.dumps(required_attrs, ensure_ascii=False),
            json.dumps(metadata, ensure_ascii=False),
        ),
    )
    created_templates = []
    for t in templates:
        price_info = compute_price(
            base_price=base_price,
            quality_metrics=quality_metrics,
            reputation=float(seller['reputation']),
            ratings=ratings,
            success_rate=success_rate,
            scenario=scenario,
            duration_days=int(t['duration_days']),
            download_limit=int(t['download_limit']),
            scope_factor=float(t['scope_factor']),
        )
        template_id = execute(
            '''INSERT INTO auth_templates
            (asset_id, name, duration_days, download_limit, scope_factor, scenario_factor, price)
            VALUES (?, ?, ?, ?, ?, ?, ?)''',
            (
                asset_id,
                t['name'],
                int(t['duration_days']),
                int(t['download_limit']),
                float(t['scope_factor']),
                price_info['scenario_factor'],
                price_info['price'],
            ),
        )
        created_templates.append({'id': template_id, **t, **price_info})
    append_block('ASSET_PUBLISHED', {'asset_id': asset_id, 'seller_id': seller_id, 'title': title})
    log_action(seller_id, 'publish_asset', 'asset', asset_id, 'SUCCESS', {'title': title})
    return {'asset_id': asset_id, 'templates': created_templates, 'file_hash': file_hash, 'trust_score': price_probe['trust_score']}


def list_assets() -> list[dict[str, Any]]:
    rows = q('SELECT * FROM assets ORDER BY id DESC')
    assets = []
    for r in rows:
        asset = row_to_dict(r) or {}
        asset['required_attrs'] = json_load(asset['required_attrs'], [])
        asset['quality'] = json_load(asset['quality_json'], {})
        asset['metadata'] = json_load(asset['metadata_json'], {})
        templates = q('SELECT * FROM auth_templates WHERE asset_id = ? AND is_active = 1 ORDER BY price ASC', (asset['id'],))
        asset['templates'] = [row_to_dict(t) for t in templates]
        assets.append(asset)
    return assets


def get_asset(asset_id: int) -> dict[str, Any] | None:
    assets = [a for a in list_assets() if a['id'] == asset_id]
    return assets[0] if assets else None


def create_order(buyer_id: int, asset_id: int, template_id: int) -> dict[str, Any]:
    buyer = get_user(buyer_id)
    if not buyer or buyer['role'] != 'buyer':
        raise ValueError('buyer invalid')
    asset = get_asset(asset_id)
    if not asset:
        raise ValueError('asset not found')
    templates = [t for t in asset['templates'] if t['id'] == template_id]
    if not templates:
        raise ValueError('template not found')
    template = templates[0]
    order_id = execute(
        '''INSERT INTO orders (buyer_id, seller_id, asset_id, template_id, price, status, note)
        VALUES (?, ?, ?, ?, ?, 'PAID_PENDING_AUTH', ?)''',
        (buyer_id, asset['seller_id'], asset_id, template_id, template['price'], 'auto-paid in research prototype'),
    )
    append_block('ORDER_CREATED', {'order_id': order_id, 'buyer_id': buyer_id, 'asset_id': asset_id, 'template_id': template_id, 'price': template['price']})
    log_action(buyer_id, 'create_order', 'order', order_id, 'SUCCESS', {'asset_id': asset_id, 'template_id': template_id})
    return get_order(order_id)  # type: ignore[return-value]


def get_order(order_id: int) -> dict[str, Any] | None:
    rows = q('SELECT * FROM orders WHERE id = ?', (order_id,))
    return row_to_dict(rows[0]) if rows else None


def authorize_order_access(order_id: int, buyer_id: int) -> dict[str, Any]:
    order = get_order(order_id)
    if not order:
        raise ValueError('order not found')
    asset = get_asset(order['asset_id'])
    user_attrs = active_attributes(buyer_id)
    allowed = can_access(user_attrs, asset['required_attrs'])
    if allowed:
        execute("UPDATE orders SET status='AUTHORIZED', updated_at=CURRENT_TIMESTAMP WHERE id = ?", (order_id,))
        result = 'SUCCESS'
        details = {'authorized': True, 'attrs': user_attrs}
    else:
        execute("UPDATE orders SET status='AUTH_FAILED', updated_at=CURRENT_TIMESTAMP WHERE id = ?", (order_id,))
        result = 'DENIED'
        details = {'authorized': False, 'attrs': user_attrs, 'required': asset['required_attrs']}
    append_block('ORDER_AUTH_CHECK', {'order_id': order_id, **details})
    log_action(buyer_id, 'authorize_access', 'order', order_id, result, details)
    return {'authorized': allowed, 'order': get_order(order_id)}


def download_and_verify(order_id: int, buyer_id: int) -> dict[str, Any]:
    order = get_order(order_id)
    if not order:
        raise ValueError('order not found')
    if order['buyer_id'] != buyer_id:
        raise ValueError('buyer mismatch')
    asset = get_asset(order['asset_id'])
    if order['status'] != 'AUTHORIZED':
        log_action(buyer_id, 'download_asset', 'order', order_id, 'DENIED', {'reason': 'order not authorized'})
        raise PermissionError('order not authorized')
    # dynamic recheck on every access
    if not can_access(active_attributes(buyer_id), asset['required_attrs']):
        execute("UPDATE orders SET status='REVOKED', updated_at=CURRENT_TIMESTAMP WHERE id = ?", (order_id,))
        log_action(buyer_id, 'download_asset', 'order', order_id, 'DENIED', {'reason': 'attributes revoked'})
        raise PermissionError('attributes revoked')
    template_rows = q('SELECT * FROM auth_templates WHERE id = ?', (order['template_id'],))
    template = row_to_dict(template_rows[0]) if template_rows else None
    if template is None:
        raise ValueError('template not found')
    if int(order['download_count']) >= int(template['download_limit']):
        execute("UPDATE orders SET status='REVOKED', updated_at=CURRENT_TIMESTAMP WHERE id = ?", (order_id,))
        raise PermissionError('download limit exceeded')
    enc_path = Path(asset['encrypted_path'])
    plain = decrypt_bytes(enc_path.read_bytes())
    local_hash = sha256_bytes(plain)
    integrity_ok = int(local_hash == asset['file_hash'])
    new_status = 'COMPLETED' if integrity_ok else 'INTEGRITY_FAILED'
    conn = get_conn()
    try:
        conn.execute(
            "UPDATE orders SET download_count=download_count+1, integrity_ok=?, status=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
            (integrity_ok, new_status, order_id),
        )
        conn.commit()
    finally:
        conn.close()
    append_block('ASSET_DOWNLOADED', {'order_id': order_id, 'buyer_id': buyer_id, 'integrity_ok': bool(integrity_ok)})
    log_action(buyer_id, 'download_asset', 'order', order_id, 'SUCCESS' if integrity_ok else 'FAIL', {'hash_match': bool(integrity_ok)})
    return {
        'content': plain.decode('utf-8', errors='ignore'),
        'hash_match': bool(integrity_ok),
        'local_hash': local_hash,
        'chain_hash': asset['file_hash'],
    }


def leave_feedback(order_id: int, buyer_id: int, rating: float, comment: str = '') -> None:
    order = get_order(order_id)
    if not order:
        raise ValueError('order not found')
    execute(
        'INSERT INTO feedback (order_id, buyer_id, seller_id, rating, comment) VALUES (?, ?, ?, ?, ?)',
        (order_id, buyer_id, order['seller_id'], rating, comment),
    )
    # small reputation update
    seller = get_user(order['seller_id'])
    current = float(seller['reputation']) if seller else 0.7
    updated = round(0.85 * current + 0.15 * (rating / 5.0), 4)
    execute('UPDATE users SET reputation = ? WHERE id = ?', (updated, order['seller_id']))
    append_block('FEEDBACK_POSTED', {'order_id': order_id, 'rating': rating, 'seller_id': order['seller_id']})
    log_action(buyer_id, 'leave_feedback', 'order', order_id, 'SUCCESS', {'rating': rating})


def revoke_order(order_id: int, actor_id: int, reason: str) -> None:
    execute("UPDATE orders SET status='REVOKED', updated_at=CURRENT_TIMESTAMP, note=? WHERE id=?", (reason, order_id))
    append_block('ORDER_REVOKED', {'order_id': order_id, 'reason': reason, 'actor_id': actor_id})
    log_action(actor_id, 'revoke_order', 'order', order_id, 'SUCCESS', {'reason': reason})


def audit_logs(limit: int = 200) -> list[dict[str, Any]]:
    rows = q('SELECT * FROM audit_logs ORDER BY id DESC LIMIT ?', (limit,))
    return [{**row_to_dict(r), 'details': json_load(r['details_json'], {})} for r in rows]


def dashboard() -> dict[str, Any]:
    return {
        'users': q('SELECT COUNT(*) AS c FROM users')[0]['c'],
        'assets': q('SELECT COUNT(*) AS c FROM assets')[0]['c'],
        'orders': q('SELECT COUNT(*) AS c FROM orders')[0]['c'],
        'authorized_orders': q("SELECT COUNT(*) AS c FROM orders WHERE status IN ('AUTHORIZED', 'COMPLETED')")[0]['c'],
        'chain': verify_chain(),
    }


def seed_demo_data() -> dict[str, Any]:
    init_db()
    if get_user_by_name('seller_alice'):
        return {'seeded': False}
    seller = register_user('seller_alice', 'pass', 'seller', 'SEU-Lab')
    buyer = register_user('buyer_bob', 'pass', 'buyer', 'SEU-Lab')
    outsider = register_user('buyer_eve', 'pass', 'buyer', 'OutsideOrg')
    aa = register_user('authority_admin', 'pass', 'authority', 'Platform-AA')
    admin = register_user('platform_admin', 'pass', 'admin', 'Platform')
    app = apply_attributes(buyer['id'], ['researcher', 'medical_reader'], 'research purchase')
    approve_attributes(app['id'], aa['id'])
    plain = b'id,value,time\n1,82,2026-03-21\n2,79,2026-03-22\n3,91,2026-03-23\n'
    asset = publish_asset(
        seller_id=seller['id'],
        title='Medical Sensor Quality Dataset',
        category='healthcare',
        description='Encrypted medical sensor dataset for controlled research and analytics.',
        scenario='research',
        quality_metrics={'completeness': 0.93, 'accuracy': 0.91, 'timeliness': 0.88, 'consistency': 0.9, 'availability': 0.95},
        required_attrs=['researcher', 'medical_reader'],
        templates=[
            {'name': 'Research-Short', 'duration_days': 30, 'download_limit': 1, 'scope_factor': 0.95},
            {'name': 'Research-Extended', 'duration_days': 180, 'download_limit': 10, 'scope_factor': 1.15},
        ],
        plain_bytes=plain,
        metadata={'base_price': 120.0},
    )
    order = create_order(buyer['id'], asset['asset_id'], asset['templates'][0]['id'])
    authorize_order_access(order['id'], buyer['id'])
    leave_feedback(order['id'], buyer['id'], 4.7, 'quality good')
    return {'seeded': True, 'seller': seller, 'buyer': buyer, 'outsider': outsider, 'authority': aa, 'admin': admin, 'asset': asset}
