from lesscli import add_argument
import os
from pathlib import Path
import re
from commondao.utils.templates import render_class_frame, render_class_methods
from commondao.utils.grammar import is_column_sql, parse_column_sql, guess_py_type, is_unique_key_sql, parse_unique_key_sql


def parse_create_table_sql(sql_content):
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
    assert match_size == 1, 'cannot parse the create table SQL'
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


@add_argument('--schemadir',
              default='schema',
              help='schema direcotry, default: schema',
              required=False)
@add_argument('--output', help='mapper file for output', dest='outfile')
def run_codegen(schemadir, outfile):
    """
    Generate CommonDao source code
    """
    # list[table_name, entity_name, col_items, unique_keys]
    #   col_items: dict[name, (py_type, comment)]
    #   unique_keys: list[list[str]]
    class_body_blocks = []
    entity_filenames = os.listdir(schemadir)
    schema_path = Path(schemadir)
    for entity_filename in entity_filenames:
        entity_name = entity_filename.split('.', 1)[0]
        sql_content = (schema_path / entity_filename).open().read()
        table_name, col_items, unique_keys = parse_create_table_sql(
            sql_content)
        class_methods = render_class_methods(table_name, entity_name,
                                             col_items, unique_keys)
        class_body_blocks.append(class_methods)
    class_body = ''.join(class_body_blocks)
    class_frame = render_class_frame(class_body)
    with open(outfile, 'w') as fout:
        fout.write(class_frame)
