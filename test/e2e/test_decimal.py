from datetime import timedelta
from decimal import Decimal
from typing import Annotated

import pytest
from aiohttp import web

from commondao.mapper import Mapper, Mysql
from lessweb import Bridge
from lessweb.annotation import Get


async def check_type(db: Mapper) -> Annotated[None, Get('/')]:
    row = await db.select_one('SELECT CAST(1234.56 AS DECIMAL(10,2)) AS dec_val')
    assert row
    assert isinstance(row['dec_val'], Decimal)
    assert row['dec_val'] == Decimal('1234.56')
    row = await db.select_one("SELECT TIMEDIFF('12:00:00', '11:30:00') AS time_diff")
    assert row
    assert isinstance(row['time_diff'], timedelta)
    assert row['time_diff'] == timedelta(minutes=30)


@pytest.mark.asyncio
async def test_crud(aiohttp_client):
    app = web.Application()
    bridge = Bridge(app=app, config='config.toml')
    bridge.scan(Mysql, Mapper, check_type)
    client = await aiohttp_client(app)
    resp = await client.get('/')
    assert resp.status == 204
