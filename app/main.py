from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from .db import init_db, q
from .services import (
    active_attributes,
    apply_attributes,
    approve_attributes,
    audit_logs,
    authorize_order_access,
    create_order,
    dashboard,
    download_and_verify,
    get_asset,
    get_order,
    get_user,
    leave_feedback,
    list_assets,
    publish_asset,
    register_user,
    revoke_order,
    revoke_user_attribute,
    seed_demo_data,
)
from .blockchain import verify_chain

BASE_DIR = Path(__file__).resolve().parents[1]
TEMPLATES_DIR = BASE_DIR / 'app' / 'templates'
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

app = FastAPI(title='Trusted Data Trading Platform Prototype')
init_db()


class RegisterIn(BaseModel):
    username: str
    password: str
    role: str
    org: str = ''


class AttrApplyIn(BaseModel):
    user_id: int
    attrs: list[str]
    purpose: str


class PublishTemplate(BaseModel):
    name: str
    duration_days: int
    download_limit: int
    scope_factor: float


class PublishIn(BaseModel):
    seller_id: int
    title: str
    category: str
    description: str
    scenario: str
    quality_metrics: dict[str, float]
    required_attrs: list[str]
    templates: list[PublishTemplate]
    plain_text: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class OrderIn(BaseModel):
    buyer_id: int
    asset_id: int
    template_id: int


class FeedbackIn(BaseModel):
    buyer_id: int
    rating: float
    comment: str = ''


class RevokeAttrIn(BaseModel):
    actor_id: int
    attr_name: str
    reason: str


class RevokeOrderIn(BaseModel):
    actor_id: int
    reason: str


@app.get('/', response_class=HTMLResponse)
def home(request: Request):
    data = dashboard()
    assets = list_assets()[:6]
    return templates.TemplateResponse('dashboard.html', {'request': request, 'stats': data, 'assets': assets})


@app.get('/assets/{asset_id}', response_class=HTMLResponse)
def asset_page(request: Request, asset_id: int):
    asset = get_asset(asset_id)
    if not asset:
        raise HTTPException(status_code=404, detail='asset not found')
    return templates.TemplateResponse('asset_detail.html', {'request': request, 'asset': asset})


@app.post('/api/seed-demo')
def seed_demo():
    return seed_demo_data()


@app.get('/api/dashboard')
def api_dashboard():
    return dashboard()


@app.post('/api/users/register')
def api_register(payload: RegisterIn):
    return register_user(payload.username, payload.password, payload.role, payload.org)


@app.get('/api/users/{user_id}')
def api_user(user_id: int):
    user = get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail='not found')
    user['attributes'] = active_attributes(user_id)
    return user


@app.post('/api/attributes/apply')
def api_apply_attrs(payload: AttrApplyIn):
    return apply_attributes(payload.user_id, payload.attrs, payload.purpose)


@app.post('/api/attributes/{application_id}/approve')
def api_approve(application_id: int, reviewer_id: int):
    return approve_attributes(application_id, reviewer_id)


@app.post('/api/users/{user_id}/revoke-attribute')
def api_revoke_attr(user_id: int, payload: RevokeAttrIn):
    revoke_user_attribute(user_id, payload.attr_name, payload.actor_id, payload.reason)
    return {'ok': True}


@app.post('/api/assets/publish')
def api_publish(payload: PublishIn):
    return publish_asset(
        seller_id=payload.seller_id,
        title=payload.title,
        category=payload.category,
        description=payload.description,
        scenario=payload.scenario,
        quality_metrics=payload.quality_metrics,
        required_attrs=payload.required_attrs,
        templates=[t.model_dump() for t in payload.templates],
        plain_bytes=payload.plain_text.encode('utf-8'),
        metadata=payload.metadata,
    )


@app.get('/api/assets')
def api_assets():
    return list_assets()


@app.get('/api/assets/{asset_id}')
def api_asset(asset_id: int):
    asset = get_asset(asset_id)
    if not asset:
        raise HTTPException(status_code=404, detail='asset not found')
    return asset


@app.post('/api/orders')
def api_order(payload: OrderIn):
    return create_order(payload.buyer_id, payload.asset_id, payload.template_id)


@app.get('/api/orders/{order_id}')
def api_order_get(order_id: int):
    order = get_order(order_id)
    if not order:
        raise HTTPException(status_code=404, detail='order not found')
    return order


@app.post('/api/orders/{order_id}/authorize')
def api_authorize(order_id: int, buyer_id: int):
    return authorize_order_access(order_id, buyer_id)


@app.get('/api/orders/{order_id}/download')
def api_download(order_id: int, buyer_id: int):
    try:
        return download_and_verify(order_id, buyer_id)
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))


@app.post('/api/orders/{order_id}/feedback')
def api_feedback(order_id: int, payload: FeedbackIn):
    leave_feedback(order_id, payload.buyer_id, payload.rating, payload.comment)
    return {'ok': True}


@app.post('/api/orders/{order_id}/revoke')
def api_revoke_order(order_id: int, payload: RevokeOrderIn):
    revoke_order(order_id, payload.actor_id, payload.reason)
    return {'ok': True}


@app.get('/api/audit')
def api_audit(limit: int = 200):
    return audit_logs(limit)


@app.get('/api/ledger/verify')
def api_verify_ledger():
    return verify_chain()


@app.exception_handler(Exception)
async def generic_exception_handler(_: Request, exc: Exception):
    return JSONResponse(status_code=500, content={'detail': str(exc)})
