from __future__ import annotations

import csv
import json
import statistics
import sys
import time
from pathlib import Path

import matplotlib.pyplot as plt
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db import reset_db, q  # noqa: E402
from app.main import app  # noqa: E402

OUT = ROOT / 'outputs'
OUT.mkdir(parents=True, exist_ok=True)


def timed(fn, n=5):
    vals = []
    last = None
    for _ in range(n):
        t0 = time.perf_counter()
        last = fn()
        vals.append(time.perf_counter() - t0)
    return statistics.mean(vals), last


def run() -> dict:
    reset_db()
    client = TestClient(app)

    seed = client.post('/api/seed-demo').json()
    buyer = seed['buyer']
    outsider = seed['outsider']
    authority = seed['authority']
    admin = seed['admin']
    seller = seed['seller']

    publish_counter = 0
    order_counter = 0
    # publish a fresh asset for benchmark stability
    base_publish_payload = {
        'seller_id': seller['id'],
        'title': 'IoT Equipment Telemetry Dataset',
        'category': 'industrial',
        'description': 'Encrypted telemetry package for research and analytics.',
        'scenario': 'analytics',
        'quality_metrics': {
            'completeness': 0.89,
            'accuracy': 0.92,
            'timeliness': 0.87,
            'consistency': 0.90,
            'availability': 0.94,
        },
        'required_attrs': ['researcher', 'medical_reader'],
        'templates': [
            {'name': 'Basic-30D', 'duration_days': 30, 'download_limit': 1, 'scope_factor': 0.95},
            {'name': 'Extended-180D', 'duration_days': 180, 'download_limit': 10, 'scope_factor': 1.15},
        ],
        'plain_text': 'device_id,temp,ts\n1,35.2,2026-03-20\n2,36.8,2026-03-21\n3,34.9,2026-03-22\n',
        'metadata': {'base_price': 150.0},
    }

    def publish_once():
        nonlocal publish_counter
        publish_counter += 1
        payload = dict(base_publish_payload)
        payload['title'] = f"IoT Equipment Telemetry Dataset #{publish_counter}"
        return client.post('/api/assets/publish', json=payload)

    publish_mean, publish_resp = timed(publish_once)
    publish_json = publish_resp.json()
    asset_id = publish_json['asset_id']
    template_id = publish_json['templates'][0]['id']

    # dynamic pricing validation by changing boundary
    asset_detail = client.get(f'/api/assets/{asset_id}').json()
    pricing_points = [(t['name'], t['price']) for t in asset_detail['templates']]

    # create orders for authorized and unauthorized users
    def create_order_once():
        return client.post('/api/orders', json={'buyer_id': buyer['id'], 'asset_id': asset_id, 'template_id': template_id})

    order_mean, order_resp = timed(create_order_once)
    order = order_resp.json()
    auth_ok = client.post(f"/api/orders/{order['id']}/authorize", params={'buyer_id': buyer['id']}).json()

    outsider_order = client.post('/api/orders', json={'buyer_id': outsider['id'], 'asset_id': asset_id, 'template_id': template_id}).json()
    auth_fail = client.post(f"/api/orders/{outsider_order['id']}/authorize", params={'buyer_id': outsider['id']}).json()

    def download_flow_once():
        nonlocal order_counter
        order_counter += 1
        fresh_order = client.post('/api/orders', json={'buyer_id': buyer['id'], 'asset_id': asset_id, 'template_id': template_id}).json()
        client.post(f"/api/orders/{fresh_order['id']}/authorize", params={'buyer_id': buyer['id']})
        return client.get(f"/api/orders/{fresh_order['id']}/download", params={'buyer_id': buyer['id']})

    download_mean, download_resp = timed(download_flow_once)
    download_json = download_resp.json()

    # revoke authorized user and verify denied
    client.post(f"/api/users/{buyer['id']}/revoke-attribute", json={'actor_id': authority['id'], 'attr_name': 'researcher', 'reason': 'experiment revoke'})
    denied_after_revoke = client.get(f"/api/orders/{order['id']}/download", params={'buyer_id': buyer['id']})

    ledger = client.get('/api/ledger/verify').json()
    audits = client.get('/api/audit?limit=20').json()

    results = {
        'functional': {
            'seed_demo': True,
            'publish_success': publish_resp.status_code == 200,
            'order_create_success': order_resp.status_code == 200,
            'authorized_access': auth_ok['authorized'],
            'unauthorized_blocked': auth_fail['authorized'] is False,
            'download_hash_match': download_json['hash_match'],
            'revocation_blocked': denied_after_revoke.status_code == 403,
            'ledger_ok': ledger['ok'],
            'audit_log_count': len(audits),
        },
        'pricing_points': pricing_points,
        'performance': {
            'publish_mean_s': round(publish_mean, 4),
            'order_mean_s': round(order_mean, 4),
            'download_mean_s': round(download_mean, 4),
        },
        'artifact_refs': {
            'asset_id': asset_id,
            'order_id': order['id'],
            'outsider_order_id': outsider_order['id'],
        },
    }

    (OUT / 'demo_results.json').write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding='utf-8')
    with (OUT / 'performance_results.csv').open('w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['metric', 'seconds'])
        writer.writerow(['publish', results['performance']['publish_mean_s']])
        writer.writerow(['order_create', results['performance']['order_mean_s']])
        writer.writerow(['download_and_verify', results['performance']['download_mean_s']])

    # figures
    plt.figure(figsize=(7,4))
    names = ['Publish', 'Order', 'Download+Verify']
    values = [results['performance']['publish_mean_s'], results['performance']['order_mean_s'], results['performance']['download_mean_s']]
    plt.bar(names, values)
    plt.ylabel('Seconds')
    plt.title('Platform Key Operation Latency')
    plt.tight_layout()
    plt.savefig(OUT / 'fig_performance.png', dpi=180)
    plt.close()

    plt.figure(figsize=(7,4))
    pn = [x[0] for x in pricing_points]
    pv = [x[1] for x in pricing_points]
    plt.bar(pn, pv)
    plt.ylabel('Price')
    plt.title('Price-Template Binding Result')
    plt.tight_layout()
    plt.savefig(OUT / 'fig_price_template.png', dpi=180)
    plt.close()

    report = f"""# Prototype Validation Summary

## Functional results
- Demo seed: {results['functional']['seed_demo']}
- Publish success: {results['functional']['publish_success']}
- Order create success: {results['functional']['order_create_success']}
- Authorized access allowed: {results['functional']['authorized_access']}
- Unauthorized access blocked: {results['functional']['unauthorized_blocked']}
- Download hash match: {results['functional']['download_hash_match']}
- Revocation blocked subsequent access: {results['functional']['revocation_blocked']}
- Ledger verification: {results['functional']['ledger_ok']}
- Audit log entries sampled: {results['functional']['audit_log_count']}

## Performance
- Publish mean latency: {results['performance']['publish_mean_s']} s
- Order create mean latency: {results['performance']['order_mean_s']} s
- Download and verify mean latency: {results['performance']['download_mean_s']} s

## Price-template binding
"""
    for name, price in pricing_points:
        report += f"- {name}: {price}\n"
    (OUT / 'VALIDATION_REPORT.md').write_text(report, encoding='utf-8')
    return results


if __name__ == '__main__':
    print(json.dumps(run(), ensure_ascii=False, indent=2))
