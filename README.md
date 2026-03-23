# Trusted Data Trading Platform Prototype

A runnable research prototype for a blockchain-inspired trusted data trading platform.

## What it implements
- Multi-role users: seller, buyer, attribute authority, admin
- Dynamic pricing based on quality, trust, scenario, and usage boundary
- Price-template binding
- Encrypted off-chain storage
- Attribute-policy controlled access
- Order lifecycle and audit logs
- Revocation and post-trade governance
- Append-only hash-chained ledger to emulate blockchain state anchoring

## Important note
This is a **research prototype**. It provides executable attribute-policy enforcement and encrypted off-chain delivery, but it does **not** implement production-grade CP-ABE over bilinear pairings. The access-control module is structured so a true CP-ABE engine can replace the current policy-gated key release layer later.

## Run locally
```bash
cd trusted_data_trading_platform
python scripts/run_server.py
```
Then open `http://127.0.0.1:8000`.

## Run demo validation
```bash
cd trusted_data_trading_platform
python scripts/run_demo.py
pytest -q
```

## Main files
- `app/main.py` - FastAPI application
- `app/services.py` - business logic
- `app/pricing.py` - dynamic pricing engine
- `app/blockchain.py` - append-only ledger
- `app/security.py` - encryption, hashing, policy enforcement
- `scripts/run_demo.py` - real end-to-end validation and performance measurement
- `tests/test_e2e.py` - automated functional tests

