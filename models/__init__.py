from pydantic import BaseModel as _BaseModelPydantic
import orjson

def orjson_dumps(v, *, default=None):
    # orjson.dumps returns bytes, to match standard json.dumps we need to decode
    return orjson.dumps(v, default=default).decode()

class BaseModel(_BaseModelPydantic):
    # rust powered json library 🦀
    class Config:
        json_loads = orjson.loads
        json_dumps = orjson_dumps