import ast
import re


def is_unique_key_sql(sql_line: str):
    return sql_line.strip().upper().startswith(
        ('PRIMARY ', 'UNIQUE ')) and parse_unique_key_sql(sql_line)


def parse_unique_key_sql(sql_line: str):
    """
    返回：list[col_name]
    例子：
        PRIMARY KEY (`id`),
        UNIQUE KEY `uni_phone` (`phone`, `openid`)
    """
    return re.findall(r'\w+', sql_line.split('(', 1)[-1].split(')', 1)[0])


def is_column_sql(sql_line: str):
    """
    判断一个SQL行是否是合法的列定义SQL
    """
    return not sql_line.strip().upper().startswith(
        ('PRIMARY ', 'KEY ', 'CONSTRAINT ', 'UNIQUE '))


def parse_column_sql(sql_line: str):
    """
    返回：col_name, sql_type, comment
    """
    sql_line = sql_line.strip().rstrip(',')
    if ' COMMENT ' in sql_line:
        sql_line, comment_part = sql_line.split(' COMMENT ', 1)
        comment = ast.literal_eval(comment_part)
    else:
        comment = ''
    col_name, sql_type = sql_line.split(None, 2)[:2]
    col_name = col_name.strip('`')
    return col_name, sql_type, comment


def guess_py_type(sql_type):
    sql_type = sql_type.lower()
    if sql_type == 'date':
        return 'date'
    elif sql_type == 'datetime':
        return 'datetime'
    elif sql_type == 'json':
        return 'str'
    elif sql_type.startswith('bigint'):
        return 'int'
    elif sql_type.startswith('int'):
        return 'int'
    elif sql_type.startswith('enum'):
        return 'str'  # 后续改进
    elif sql_type.startswith('tinyint'):
        return 'bool'
    else:
        return 'str'
