import asyncio
import os
import re

from lesscli import add_argument
from lessweb import Bridge

from commondao.mapper import Mysql, mysql_cleanup, mysql_startup
from commondao.utils.grammar import (guess_py_type, is_column_sql,
                                     is_unique_key_sql, parse_column_sql,
                                     parse_unique_key_sql)
from commondao.utils.templates import render_class_frame, render_class_methods


def parse_create_table_sql(sql_content, entity_filename):
    """
    :return 形如table_name, col_items, unique_keys
       * col_items: dict[name, (py_type, comment)]
       * unique_keys: list[list[str]]
    """
    col_items = {}
    unique_keys = []
    seed = re.compile(r'CREATE TABLE *`?(\w+)`? *\((.*)\)',
                      flags=re.I | re.DOTALL)
    match_size = len(find_ret := seed.findall(sql_content))
    assert match_size != 0, f'cannot parse the create table SQL (No "CREATE TABLE ..." in {entity_filename})'
    assert match_size == 1, f'cannot parse the create table SQL (Too many "CREATE TABLE ..." in {entity_filename})'
    table_name, sql_body = find_ret[0]
    for sql_line in sql_body.splitlines():
        if sql_line and is_column_sql(sql_line):
            col_name, sql_type, comment = parse_column_sql(sql_line)
            py_type = guess_py_type(sql_type)
            col_items[col_name] = py_type, comment
        elif sql_line and is_unique_key_sql(sql_line):
            col_names = parse_unique_key_sql(sql_line)
            unique_keys.append(col_names)
    return table_name, col_items, unique_keys


async def get_create_table_sqls(confpath, prefix: str):
    result = []  # list[table_name, sql_content]
    os.environ['LOGGER_STREAM'] = 'stdout'
    bridge = Bridge(config=confpath)
    mysql = Mysql(bridge.app)
    await mysql_startup(bridge.app, mysql)
    async with mysql.pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute('show tables')
            rows = await cur.fetchall()
            for table_name, *_ in rows:
                if table_name.startswith(prefix) and table_name != 'alembic_version':
                    await cur.execute(f'show create table `{table_name}`')
                    result.append(await cur.fetchone())
    await mysql_cleanup(mysql)
    return result


@add_argument('--confpath',
              default='config.toml',
              help='config file path, default: config.toml',
              required=False)
@add_argument('--prefix', default='tbl_', help='table name prefix, default: "tbl_"', required=False)
@add_argument('--output', help='mapper file for output', dest='outfile')
def run_codegen(confpath, prefix, outfile):
    """
    Generate CommonDao source code
    """
    # list[table_name, entity_name, col_items, unique_keys]
    #   col_items: dict[name, (py_type, comment)]
    #   unique_keys: list[list[str]]
    class_body_blocks = []
    create_table_sqls = asyncio.run(get_create_table_sqls(confpath, prefix))
    for table_name, sql_content in create_table_sqls:
        entity_name = table_name.removeprefix(prefix)
        table_name, col_items, unique_keys = parse_create_table_sql(
            sql_content, table_name)
        class_methods = render_class_methods(table_name, entity_name,
                                             col_items, unique_keys)
        class_body_blocks.append(class_methods)
    class_body = ''.join(class_body_blocks)
    class_frame = render_class_frame(class_body)
    with open(outfile, 'w') as fout:
        fout.write(class_frame)
