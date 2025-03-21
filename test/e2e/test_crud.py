import logging
import random
from datetime import date, datetime
from typing import Annotated, List, TypedDict

import orjson
import pytest
from aiohttp import web
from aiohttp.web import Request
from pydantic import BaseModel, computed_field

from commondao.mapper import (EXECUTE, Mapper, Mysql, Paged, RawSql,
                              is_query_dict, is_row_dict, validate_row)
from lessweb import Bridge
from lessweb.annotation import Delete, Get, Post, Put


class CreatePetInput(TypedDict):
    name: str
    color: str
    birthday: date


class UpdatePetInput(TypedDict, total=False):
    name: str
    color: str
    birthday: date


class Pet(BaseModel):
    id: int
    name: str
    color: str
    birthday: date
    age: Annotated[int | None, RawSql(
        'TIMESTAMPDIFF(YEAR, birthday, CURDATE())')] = None
    created_at: datetime
    updated_at: datetime

    @computed_field  # type: ignore[misc]
    @property
    def days(self) -> int:
        return (datetime.now().date() - self.birthday).days


async def init_db(db: Mapper) -> Annotated[None, Post('/init_db')]:
    await db.execute(EXECUTE, '''CREATE TABLE IF NOT EXISTS t_pet (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255),
    color VARCHAR(50),
    birthday DATE,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
)''')


async def create_pet(db: Mapper, request: Request) -> Annotated[Pet, Post('/pet')]:
    data: CreatePetInput = orjson.loads(await request.read())
    assert is_row_dict(data)
    await db.save('t_pet', data=data)
    pet_id: int = db.lastrowid()
    row = await db.get_by_key('t_pet', key={'id': pet_id})
    assert row
    pet = validate_row(row, Pet)
    logging.info('create pet: %s', pet)
    return pet


async def update_pet(db: Mapper, request: Request, *, id: int) -> Annotated[None, Put('/pet/{id}')]:
    data: UpdatePetInput = orjson.loads(await request.read())
    assert is_row_dict(data)
    await db.update_by_key('t_pet', key={'id': id}, data=data)


async def delete_pet(db: Mapper, *, id: int) -> Annotated[None, Delete('/pet/{id}')]:
    await db.delete_by_key('t_pet', key={'id': id})


async def query_pet(db: Mapper, *, color: List[str] = None, name: str = None, birthday: date = None, birthday_end: date = None, page: int = 1, size: int = 10) -> Annotated[Paged[Pet], Get('/pet')]:
    if name:
        name = '%' + name + '%'
    result = await db.select_paged(
        '''select * from t_pet
        where (:not_color or color in :color)
        and (:name is null or name like :name)
        and (:birthday is null or birthday between :birthday and :birthday_end)
        order by id desc''',
        select=Pet,
        data={
            'color': color or [''],
            'not_color': not color,
            'name': name,
            'birthday': birthday,
            'birthday_end': birthday_end,
        },
        page=page,
        size=size or 'ALL',
    )
    logging.info('query pet: %s', result)
    return result


@pytest.mark.asyncio
async def test_crud(aiohttp_client):
    app = web.Application()
    bridge = Bridge(app=app, config='config.toml')
    bridge.scan(Mysql, Mapper, init_db, create_pet,
                update_pet, delete_pet, query_pet)
    client = await aiohttp_client(app)
    resp = await client.post('/init_db')
    assert resp.status == 204
    resp = await client.post('/pet', json={'name': f'WangCai{random.randint(0, 9999)}', 'color': 'black', 'birthday': '2023-01-31'})
    assert resp.status == 200
    pet = await resp.json()
    pet_id = pet['id']
    assert pet_id > 0
    resp = await client.get('/pet')
    assert resp.status == 200
    resp = await client.put(f'/pet/{pet_id}', json={'name': f'HuiHui{random.randint(0, 9999)}', 'color': 'gray'})
    assert resp.status == 204
    resp = await client.get('/pet?name=ui&color=black,gray&birthday=2000-01-01&birthday_end=2025-12-31&page=1&size=0')
    assert resp.status == 200
    logging.info('resp json: %s', orjson.dumps(await resp.json()).decode())
    resp = await client.delete(f'/pet/{pet_id}')
    assert resp.status == 204
