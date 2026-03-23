from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Iterable

from cryptography.fernet import Fernet

BASE_DIR = Path(__file__).resolve().parents[1]
KEY_PATH = BASE_DIR / 'master.key'


def get_fernet() -> Fernet:
    if not KEY_PATH.exists():
        KEY_PATH.write_bytes(Fernet.generate_key())
    return Fernet(KEY_PATH.read_bytes())


def encrypt_bytes(data: bytes) -> bytes:
    return get_fernet().encrypt(data)


def decrypt_bytes(data: bytes) -> bytes:
    return get_fernet().decrypt(data)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def can_access(user_attrs: Iterable[str], required_attrs: Iterable[str]) -> bool:
    ua = set(user_attrs)
    ra = set(required_attrs)
    return ra.issubset(ua)
