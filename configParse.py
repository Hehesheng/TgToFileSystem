import toml
import os

import functools
from pydantic import BaseModel


class TgToFileSystemParameter(BaseModel):
    class BaseParameter(BaseModel):
        salt: str = ""
        exposed_url: str = "http://127.0.0.1:7777"
        port: int = 7777
        timeit_enable: bool = False
    base: BaseParameter

    class ClientConfigPatameter(BaseModel):
        token: str = ""
        interval: float = 0.1
        whitelist_chat: list[int] = []
    clients: list[ClientConfigPatameter]

    class ApiParameter(BaseModel):
        api_id: int
        api_hash: str
    tgApi: ApiParameter

    class TgProxyParameter(BaseModel):
        enable: bool = False
        proxy_type: str = "socks5"
        addr: str = ""
        port: int = ""
    proxy: TgProxyParameter
    
    class TgWebParameter(BaseModel):
        enable: bool = False
        token: str = ""
        port: int = 2000
        chat_id: list[int] = []
    web: TgWebParameter

@functools.lru_cache
def get_TgToFileSystemParameter(path: str = f"{os.path.dirname(__file__)}/config.toml", force_reload: bool = False) -> TgToFileSystemParameter:
    if force_reload:
        get_TgToFileSystemParameter.cache_clear()
    return TgToFileSystemParameter.model_validate(toml.load(path))

if __name__ == "__main__":
    print(get_TgToFileSystemParameter().model_dump())
