## How to init?
```python
from commondao.mapper import mysql_startup, mysql_cleanup, mysql_connect

bridge.add_mod_ctx(mysql_startup, mysql_cleanup)
bridge.add_middleware(mysql_connect)

```

## How to codegen?
`commondao --help`

`commondao codegen --output my_proj/mapper.py`
