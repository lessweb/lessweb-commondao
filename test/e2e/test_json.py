import logging
from datetime import datetime
from typing import Annotated, List

import orjson
import pytest
from aiohttp import web
from pydantic import BaseModel

from commondao.mapper import EXECUTE, Mapper, Mysql, RowDict, validate_row
from lessweb import Bridge, TypeCast
from lessweb.annotation import Post


class OrderDetail(BaseModel):
    status: str
    price: float


class OrderLog(BaseModel):
    msg: str


class CreateOrderInput(BaseModel):
    detail: OrderDetail | None = None
    logs: List[OrderLog]


class Order(BaseModel):
    id: int
    detail: OrderDetail | None
    logs: List[OrderLog]
    created_at: datetime
    updated_at: datetime


async def init_db(db: Mapper) -> Annotated[None, Post('/init_db')]:
    await db.execute(EXECUTE, '''CREATE TABLE IF NOT EXISTS t_order (
    id INT PRIMARY KEY AUTO_INCREMENT,
    detail JSON default NULL,
    logs JSON NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
)''')


async def create_order(post_input: CreateOrderInput, /, db: Mapper) -> Annotated[Order, Post('/order')]:
    data = {}
    if post_input.detail:
        data['detail'] = TypeCast.dumps(
            post_input.detail).decode()  # Tested: must use decode()
    data['logs'] = TypeCast.dumps(post_input.logs).decode()
    await db.save('t_order', data=data)
    pet_id: int = db.lastrowid()
    row = await db.get_by_key('t_order', key={'id': pet_id})
    assert row
    order = validate_row(row, Order)
    logging.info('create order: %s', order)
    return order


@pytest.mark.asyncio
async def test_crud(aiohttp_client):
    app = web.Application()
    bridge = Bridge(app=app, config='config.toml')
    bridge.scan(Mysql, Mapper, init_db, create_order)
    client = await aiohttp_client(app)
    resp = await client.post('/init_db')
    assert resp.status == 204
    resp = await client.post('/order', json={
        'detail': {'status': 'pending', 'price': 100.0},
        'logs': [{'msg': 'hello'}, {'msg': 'world'}]
    })
    assert resp.status == 200
    logging.info('resp json #1: %s', orjson.dumps(await resp.json()).decode())
    resp = await client.post('/order', json={'detail': None, 'logs': []})
    assert resp.status == 200
    logging.info('resp json #2: %s', orjson.dumps(await resp.json()).decode())
