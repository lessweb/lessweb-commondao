from aiohttp.web import Application, Request
import aiomysql  # type: ignore
from aiomysql import Pool, Connection, Cursor, DictCursor
from lessweb.bridge import service
import logging
import re
from re import Match
from typing import TypeVar
from commondao.utils.common import script, and_, join, where

T = TypeVar('T')

SELECT_ONE = 1
SELECT_ALL = 2
EXECUTE = 3


@service
class Mysql:
    app: Application
    pool: Pool

    def __init__(self, app: Application):
        self.app = app


async def mysql_startup(app: Application, mysql: Mysql):
    mysql.pool = await aiomysql.create_pool(**app['config']['mysql'])


async def mysql_cleanup(mysql: Mysql):
    mysql.pool.close()
    await mysql.pool.wait_closed()


@service
class MysqlConn:
    request: Request
    mysql: Mysql
    conn: Connection
    cur: Cursor

    def __init__(self, request: Request, mysql: Mysql):
        self.request = request
        self.mysql = mysql


async def mysql_connect(handler, mysql_conn: MysqlConn):
    async with mysql_conn.mysql.pool.acquire() as conn:
        mysql_conn.conn = conn
        async with conn.cursor(DictCursor) as cur:
            mysql_conn.cur = cur
            return await handler()


class RegexCollect:
    words: list

    def __init__(self):
        self.words = []

    def repl(self, m: Match):
        word = m.group()
        self.words.append(word[2:])
        return word[0] + '%s'

    def build(self, sql: str, params: dict) -> tuple:
        pattern = r"[^:]:[a-zA-Z][\w.]*"
        pg_sql = re.sub(pattern, self.repl, sql)
        pg_params = tuple(params[k] for k in self.words)
        return pg_sql, pg_params


def make_sub_clause(query) -> tuple:
    """
    query: Dict | MutableMapping
    return: (value_map, limit_map, where_clause, order_clause, limit_clause)
    """
    eq_filter = '<field>=:<field>.value'
    filter_map = {}  # filter_map[query_key] = filter
    limit_map = {'limit': 1000}
    orderby_rules = []
    value_map = {}
    for query_key, query_val in query.items():
        if query_key == 'limit':
            limit_map['limit'] = int(query_val)
        elif query_key == 'offset':
            limit_map['offset'] = int(
                query_val
            )  # todo 如果传入的limit=0，则只查询总数；否则正常分页查询。如果不传limit则默认1000。
        elif '.' not in query_key:
            filter_map.setdefault(query_key, eq_filter.replace('<field>', query_key))
            value_map[f'{query_key}.value'] = query_val
        elif query_key.endswith('.value'):
            filter_map.setdefault(query_key[:-6], eq_filter.replace('<field>', query_key[:-6]))
            value_map[query_key] = query_val
        elif query_key.endswith('.filter'):
            filter_map[query_key[:-7]] = query_val.replace('<field>', query_key[:-7])
        elif query_key.endswith('.order'):
            assert query_val in 'asc' or query_val in 'desc'
            orderby_rules.append(query_key[:-6] + ' ' + query_val)
        else:
            value_map[query_key] = query_val
    where_clause = ''
    order_clause = ', '.join(orderby_rules)
    limit_clause = 'limit %d' % int(limit_map['limit'])
    for filter_val in filter_map.values():
        where_clause += ('WHERE' if not where_clause else 'AND')\
                        + f' ({filter_val}) '
    if 'offset' in limit_map:
        limit_clause += ' offset %d' % int(limit_map['offset'])
    return value_map, limit_map, where_clause, order_clause, limit_clause


@service
class Mapper:
    conn: MysqlConn

    def __init__(self, conn: MysqlConn):
        self.conn = conn

    async def commit(self):
        await self.conn.conn.commit()

    async def execute(self, mode, sql: str, data: dict):
        cursor = self.conn.cur
        logging.info(sql)
        pg_sql, pg_params = RegexCollect().build(sql, data)
        logging.info('%s => %s', pg_sql, pg_params)
        await cursor.execute(pg_sql, pg_params)
        if mode == SELECT_ONE:
            return await cursor.fetchone() or {}
        elif mode == SELECT_ALL:
            return await cursor.fetchall()
        else:
            return cursor.rowcount

    async def select_one(self, sql: str, data: dict):
        return await self.execute(SELECT_ONE, sql, data)

    async def select_all(self, sql: str, data: dict):
        return await self.execute(SELECT_ALL, sql, data)

    async def insert(self, sql: str, data: dict):
        return await self.execute(EXECUTE, sql, data)

    async def update(self, sql: str, data: dict):
        return await self.execute(EXECUTE, sql, data)

    async def delete(self, sql: str, data: dict):
        return await self.execute(EXECUTE, sql, data)

    async def save(self, tablename: str, *, data: dict):
        selected_data = {
            key: value
            for key, value in data.items() if value is not None
        }
        sql = script(
            'insert into',
            tablename,
            '(',
            join(*selected_data.keys()),
            ') values (',
            join(*[f':{key}' for key in selected_data.keys()]),
            ')',
        )
        return await self.insert(sql, selected_data)

    async def update_by_key(self, tablename, *, key: dict, data: dict):
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
            join(*[f'{k}=:{k}' for k in selected_data.keys()], ),
            'where',
            and_(*[f'{k}=:{k}' for k in key.keys()]),
        )
        return await self.update(sql, {**data, **key})

    async def delete_by_key(self, tablename, *, key: dict):
        sql = script(
            'delete from',
            tablename,
            'where',
            and_(*[f'{k}=:{k}' for k in key.keys()]),
        )
        return await self.delete(sql, key)

    async def get_by_key(self, tablename, *, key: dict):
        sql = script('select * from', tablename,
                     where(and_(*[f'{k}=:{k}' for k in key.keys()])),
                     'limit 1')
        return await self.select_one(sql, key)

    async def select_by_query(self,
                              tablename,
                              query,
                              select_clause: str = '*',
                              extra: dict = None):
        """
        输入：
            query: Dict | MutableMapping
            extra: Dict
        保留字：limit, offset, .value, .field, .order, <field>
        <field>.value: 可以省略.value
        <field>.filter: 语法例如"<field> between <value> and <end>"或者"<filter> >= <value>"
        <field>.order: 值可取"asc|desc"(不支持大写)
        tablename分为表名模式和完整模式，完整模式包含最多一个<select>, <where>, <order>, <limit>
        如果传入的limit=0，则只查询总数；如果不传limit或limit is None，则不分页；否则正常分页查询。
        """
        value_map, limit_map, where_clause, order_clause, limit_clause = make_sub_clause(
            query)
        count_sql = script(
            'SELECT COUNT(*) AS total FROM',
            tablename,
            where_clause,
        )
        if extra:
            value_map.update(extra)
        count_ret = await self.select_one(count_sql, value_map)
        assert select_clause == '*' or re.match(
            r'[\w,` ]+',
            select_clause), f'Invalid select_clause: {select_clause}'
        records_sql = script(
            'SELECT',
            select_clause,
            'FROM',
            tablename,
            where_clause,
            f'ORDER BY {order_clause}' if order_clause else '',
            limit_clause,
        )
        records = await self.select_all(records_sql, value_map)
        return {**count_ret, **limit_map, 'list': records}
