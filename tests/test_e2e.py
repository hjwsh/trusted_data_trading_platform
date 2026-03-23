from __future__ import annotations

import sys
from pathlib import Path

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db import reset_db  # noqa: E402
from app.main import app  # noqa: E402


def test_end_to_end_flow():
    reset_db()
    client = TestClient(app)
    seed = client.post('/api/seed-demo')
    assert seed.status_code == 200
    data = seed.json()
    seller = data['seller']
    buyer = data['buyer']
    outsider = data['outsider']
    authority = data['authority']

    assets = client.get('/api/assets').json()
    assert len(assets) >= 1
    asset = assets[0]
    template_id = asset['templates'][0]['id']

    order = client.post('/api/orders', json={'buyer_id': buyer['id'], 'asset_id': asset['id'], 'template_id': template_id})
    assert order.status_code == 200
    order_id = order.json()['id']

    auth = client.post(f'/api/orders/{order_id}/authorize', params={'buyer_id': buyer['id']})
    assert auth.status_code == 200
    assert auth.json()['authorized'] is True

    download = client.get(f'/api/orders/{order_id}/download', params={'buyer_id': buyer['id']})
    assert download.status_code == 200
    assert download.json()['hash_match'] is True

    outsider_order = client.post('/api/orders', json={'buyer_id': outsider['id'], 'asset_id': asset['id'], 'template_id': template_id}).json()
    outsider_auth = client.post(f"/api/orders/{outsider_order['id']}/authorize", params={'buyer_id': outsider['id']})
    assert outsider_auth.json()['authorized'] is False

    revoke = client.post(f"/api/users/{buyer['id']}/revoke-attribute", json={'actor_id': authority['id'], 'attr_name': 'researcher', 'reason': 'test revoke'})
    assert revoke.status_code == 200
    denied = client.get(f'/api/orders/{order_id}/download', params={'buyer_id': buyer['id']})
    assert denied.status_code == 403

    ledger = client.get('/api/ledger/verify').json()
    assert ledger['ok'] is True
