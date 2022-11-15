def render_class_frame(class_body):
    return f"""from commondao.mapper import Mapper
from datetime import datetime, date
from lessweb.bridge import service


@service
class CommonDao(Mapper):
{class_body}
"""


def render_class_methods(table_name, entity_name, col_items, unique_keys):
    """
    col_items: 形如dict[name, (py_type, comment)]
    unique_keys: 形如list[list[str]]
    """
    indent = '    '
    kwargs = ''.join(f'{indent * 2}{name}: {py_type} = None,  # {comment}\n'
                     for name, (py_type, comment) in col_items.items())
    data_rows = ''.join(f"{indent * 3}'{name}': {name},\n"
                        for name in col_items.keys())
    by_key_methods = ''
    for unique_key in unique_keys:
        by_key_methods += render_by_key_method(table_name, entity_name,
                                               col_items, unique_key)
    return f"""    async def insert_{entity_name}(
        self,
{kwargs}
    ):
        data = {{
{data_rows}
        }}
        return await self.save('{table_name}', data=data)
    
    async def select_{entity_name}(self, query, select_clause: str = '*', extra: dict = None):
        return await self.select_by_query('{table_name}', query, select_clause, extra)
    {by_key_methods}
"""


def render_by_key_method(table_name, entity_name, col_items, unique_key: list):
    indent = '    '
    key_name = '_'.join(unique_key)
    data_items = {
        key: value
        for key, value in col_items.items() if key not in unique_key
    }
    key_items = {
        key: value
        for key, value in col_items.items() if key in unique_key
    }
    kwargs = ''.join(f'{indent * 2}{name}: {py_type} = None,  # {comment}\n'
                     for name, (py_type, comment) in data_items.items())
    data_rows = ''.join(f"{indent * 3}'{name}': {name},\n"
                        for name in data_items.keys())
    args = ''.join(f'{indent * 2}{name}: {py_type},  # {comment}\n'
                   for name, (py_type, comment) in key_items.items())
    key_rows = ''.join(f"{indent * 3}'{name}': {name},\n"
                       for name in unique_key)
    return f"""
    async def update_{entity_name}_by_{key_name}(
        self,
{args}{kwargs}
    ):
        data = {{
{data_rows}
        }}
        key = {{
{key_rows}
        }}
        return await self.update_by_key('{table_name}', key=key, data=data)
    
    async def delete_{entity_name}_by_{key_name}(
        self,
{args}
    ):
        key = {{
{key_rows}
        }}
        return await self.delete_by_key('{table_name}', key=key)
    
    async def get_{entity_name}_by_{key_name}(
        self,
{args}
    ):
        key = {{
{key_rows}
        }}
        return await self.get_by_key('{table_name}', key=key)
"""
