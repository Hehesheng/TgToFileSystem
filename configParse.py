import toml

import functools
from pydantic import BaseModel


class TgToFileSystemParameter(BaseModel):
    class BaseParameter(BaseModel):
        salt: str
        port: int
        timeit_enable: bool
    base: BaseParameter

    class ApiParameter(BaseModel):
        api_id: int
        api_hash: str
    tgApi: ApiParameter

    class TgProxyParameter(BaseModel):
        enable: bool
        proxy_type: str
        addr: str
        port: int
    proxy: TgProxyParameter

@functools.lru_cache
def get_TgToFileSystemParameter(path: str = "./config.toml", force_reload: bool = False) -> TgToFileSystemParameter:
    if force_reload:
        get_TgToFileSystemParameter.cache_clear()
    return TgToFileSystemParameter.model_validate(toml.load(path))
