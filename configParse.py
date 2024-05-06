import toml
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

__cache_res = None
def get_TgToFileSystemParameter(path: str = "./config.toml", force_reload: bool = False) -> TgToFileSystemParameter:
    global __cache_res
    if __cache_res is not None and not force_reload:
        return __cache_res
    __cache_res = TgToFileSystemParameter.model_validate(toml.load(path))
    return __cache_res
