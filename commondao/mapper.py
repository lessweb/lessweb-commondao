import inspect
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from re import Match
from types import MappingProxyType
from typing import (Annotated, Any, Callable, Generic, List, Literal, Mapping,
                    Optional, Type, TypeGuard, TypeVar, Union)

import aiomysql  # type: ignore
import orjson
from aiohttp.web import Application, Request
from aiomysql import Connection, Cursor, DictCursor, Pool
from pydantic import BaseModel

from lessweb import Middleware, Module, TypeCast, load_module_config
from lessweb.typecast import is_list_type

T = TypeVar('T')
U = TypeVar("U", bound=BaseModel)

SELECT_ONE = 1
SELECT_ALL = 2
EXECUTE = 3

RowValueNotNull = Union[
    str,
    int,
    # For NULL values in the database, it will return Python's None.
    float,
    # For MySQL fields of type DECIMAL and NUMERIC, it will return a decimal.Decimal object. e.g. "SELECT CAST(1234.56 AS DECIMAL(10,2)) AS dec_val"
    Decimal,
    # For fields of type BINARY, VARBINARY, or BLOB, it might return a bytes object.
    bytes,
    # For fields of type DATETIME or TIMESTAMP, it returns a Python datetime.datetime object.
    datetime,
    date,
    time,
    # e.g. "SELECT TIMEDIFF('12:00:00', '11:30:00') AS time_diff"
    timedelta,
]
RowDict = Mapping[str, RowValueNotNull | None]
# e.g. "... WHERE id in :ids" ids=[1, 2, 3] or (1, 2, 3)
QueryDict = Mapping[str, RowValueNotNull | None | list | tuple]


def is_row_dict(data: Mapping, /) -> TypeGuard[RowDict]:
    for value in data.values():
        if value is None:
            continue
        elif isinstance(value, (str, int, float, Decimal, bytes, datetime, date, time, timedelta)):
            continue
        else:
            return False
    return True


def is_query_dict(data: Mapping, /) -> TypeGuard[QueryDict]:
    for value in data.values():
        if value is None:
            continue
        elif isinstance(value, (str, int, float, Decimal, bytes, datetime, date, time, timedelta, list, tuple)):
            continue
        else:
            return False
    return True


@dataclass
class Paged(Generic[T]):
    items: List[T]
    page: Optional[int] = None
    size: Optional[int] = None
    total: Optional[int] = None


@dataclass
class RawSql:
    sql: str


def script(*segs) -> str:
    return ' '.join(seg or '' for seg in segs)


def join(*segs) -> str:
    return ','.join(seg or '' for seg in segs)


def and_(*segs) -> str:
    ret = ' and '.join(seg or '' for seg in segs).strip()
    return f'({ret})' if ret else ''


def where(*segs) -> str:
    sql = and_(*segs)
    return f'where {sql} ' if sql else ''


def validate_row(row: RowDict, model: Type[U]) -> U:
    """
    - 如果item_type是list|BaseModel，则orjson.loads
    - 如果item是字符串，则TypeCase.validate_query
    - 最后再.model_validate
    """
    data = {}
    model_fields = model.model_fields
    for row_key, row_value in row.items():
        tp = model_fields[row_key].annotation
        if isinstance(row_value, str) and tp is not str and tp is not None:
            if is_list_type(tp) or (inspect.isclass(tp) and issubclass(tp, BaseModel)):
                data[row_key] = orjson.loads(row_value)
            else:
                data[row_key] = TypeCast.validate_query(row_value, tp)
        else:
            data[row_key] = row_value
    return model.model_validate(data)


class MysqlConfig(BaseModel):
    pool_recycle: int = 60
    host: str
    port: int = 3306
    user: str
    password: str
    db: str
    echo: bool = True
    autocommit: bool = True
    maxsize: int = 20
    page_size: int = 10


class Mysql(Module):
    pool: Pool
    page_size: int

    async def on_startup(self, app: Application):
        config = self.load_config(app)
        self.page_size = config.page_size
        config_dict = config.model_dump()
        del config_dict['page_size']
        self.pool = await aiomysql.create_pool(**config_dict)
        logging.debug('add middlewares-> mapper_on_request')

    async def on_cleanup(self, app: Application):
        self.pool.close()
        await self.pool.wait_closed()

    def load_config(self, app: Application) -> Annotated[MysqlConfig, 'mysql']:
        return load_module_config(app, 'mysql', MysqlConfig)


class RegexCollect:
    words: list

    def __init__(self):
        self.words = []

    def repl(self, m: Match):
        word = m.group()
        self.words.append(word[2:])
        return word[0] + '%s'

    def build(self, sql: str, params: Mapping[str, Any]) -> tuple:
        pattern = r"[^:]:[a-zA-Z][\w.]*"
        pg_sql = re.sub(pattern, self.repl, sql)
        pg_params = tuple(params[k] for k in self.words)
        return pg_sql, pg_params


class Mapper(Middleware):
    mysql: Mysql
    conn: Connection
    cur: Cursor

    def __init__(self, mysql: Mysql):
        self.mysql = mysql

    async def on_request(self, request: Request, handler: Callable):
        async with self.mysql.pool.acquire() as conn:
            self.conn = conn
            async with conn.cursor(DictCursor) as cur:
                self.cur = cur
                return await handler(request)

    async def commit(self):
        await self.conn.commit()

    def lastrowid(self) -> int:
        return self.cur.lastrowid

    async def execute(self, mode, sql: str, data: Mapping[str, Any] = MappingProxyType({})):
        cursor = self.cur
        logging.debug(sql)
        pg_sql, pg_params = RegexCollect().build(sql, data)
        logging.debug('execute: %s => %s', pg_sql, pg_params)
        await cursor.execute(pg_sql, pg_params)
        if mode == SELECT_ONE:
            return await cursor.fetchone()
        elif mode == SELECT_ALL:
            return await cursor.fetchall() or []
        else:
            return cursor.rowcount

    async def select_one(self, sql: str, data: QueryDict = MappingProxyType({})) -> Optional[RowDict]:
        return await self.execute(SELECT_ONE, sql, data)

    async def select_all(self, sql: str, data: QueryDict = MappingProxyType({})) -> List[RowDict]:
        return await self.execute(SELECT_ALL, sql, data)

    async def insert(self, sql: str, data: QueryDict) -> int:
        return await self.execute(EXECUTE, sql, data)

    async def update(self, sql: str, data: QueryDict) -> int:
        return await self.execute(EXECUTE, sql, data)

    async def delete(self, sql: str, data: QueryDict) -> int:
        return await self.execute(EXECUTE, sql, data)

    async def save(self, tablename: str, *, data: RowDict) -> int:
        selected_data = {
            key: value
            for key, value in data.items() if value is not None
        }
        sql = script(
            'insert into',
            tablename,
            '(',
            join(*[f'`{key}`' for key in selected_data.keys()]),
            ') values (',
            join(*[f':{key}' for key in selected_data.keys()]),
            ')',
        )
        return await self.insert(sql, selected_data)

    async def update_by_key(self, tablename, *, key: QueryDict, data: RowDict) -> int:
        selected_data = {
            key: value
            for key, value in data.items() if value is not None
        }
        if not selected_data:
            return 0
        sql = script(
            'update',
            tablename,
            'set',
            join(*[f'`{k}`=:{k}' for k in selected_data.keys()], ),
            'where',
            and_(*[f'`{k}`=:{k}' for k in key.keys()]),
        )
        return await self.update(sql, {**data, **key})

    async def delete_by_key(self, tablename, *, key: QueryDict) -> int:
        sql = script(
            'delete from',
            tablename,
            'where',
            and_(*[f'`{k}`=:{k}' for k in key.keys()]),
        )
        return await self.delete(sql, key)

    async def get_by_key(self, tablename, *, key: QueryDict) -> Optional[RowDict]:
        sql = script('select * from', tablename,
                     where(and_(*[f'`{k}`=:{k}' for k in key.keys()])),
                     'limit 1')
        return await self.select_one(sql, key)

    async def select_paged(
            self,
            sql: str,
            select: Type[U],
            data: QueryDict,
            page: int = 1,
            size: Union[int, Literal['DEFAULT', 'ALL']] = 'DEFAULT',
    ) -> Paged[U]:
        assert sql.lower().startswith('select * from')
        headless_sql = sql[13:]
        page = max(1, page)
        if size == 'DEFAULT':
            size = self.mysql.page_size
        select_items: List[str] = []
        for name, info in select.model_fields.items():
            for metadata in info.metadata:
                if isinstance(metadata, RawSql):
                    select_items.append(f'({metadata.sql}) as `{name}`')
                    break
            else:  # else-for
                select_items.append(f'`{name}`')
            # end-for
        select_clause = 'select %s from ' % ', '.join(select_items)
        sql = f'{select_clause} {headless_sql}'
        if size == 'ALL':
            rows = await self.select_all(sql, data)
            models = [validate_row(row, select) for row in rows]
            return Paged(items=models)
        assert isinstance(size, int) and size > 0
        count_sql = f'select count(*) as total from {headless_sql}'
        count_result = await self.select_one(count_sql, data)
        assert count_result is not None, "count result should not be None"
        total: int = count_result['total']  # type: ignore
        offset = (page - 1) * size
        limit_clause = 'limit %d' % size
        if offset:
            limit_clause += ' offset %d' % offset
        sql = f'{select_clause} {headless_sql} {limit_clause}'
        rows = await self.select_all(sql, data)
        models = [validate_row(row, select) for row in rows]
        return Paged(models, page, size, total)
