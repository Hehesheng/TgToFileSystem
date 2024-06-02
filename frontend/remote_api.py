import sys
import os
import json
import logging

import requests

sys.path.append(os.getcwd() + "/../")
import configParse

logger = logging.getLogger(__file__.split("/")[-1])

param = configParse.get_TgToFileSystemParameter()

background_server_url = f"{param.base.exposed_url}"
search_api_route = "/tg/api/v1/file/search"
status_api_route = "/tg/api/v1/client/status"
login_api_route = "/tg/api/v1/client/login"


def login_client_by_qr_code_url() -> str:
    request_url = background_server_url + login_api_route
    response = requests.get(request_url)
    if response.status_code != 200:
        logger.warning(f"Could not login, err:{response.status_code}, {response.content.decode('utf-8')}")
        return None
    url_info = json.loads(response.content.decode("utf-8"))
    return url_info.get("url")


def get_backend_client_status() -> dict[str, any]:
    request_url = background_server_url + status_api_route
    response = requests.get(request_url)
    if response.status_code != 200:
        logger.warning(f"get_status, backend is running? err:{response.status_code}, {response.content.decode('utf-8')}")
        return None
    return json.loads(response.content.decode("utf-8"))


def search_database_by_keyword(keyword: str, offset: int, limit: int, is_order: bool) -> list[any] | None:
    request_url = background_server_url + search_api_route
    req_body = {
        "token": param.web.token,
        "search": keyword,
        "chat_id": param.web.chat_id[0],
        "index": offset,
        "length": limit,
        "refresh": False,
        "inner": False,
        "inc": is_order,
    }

    response = requests.post(request_url, data=json.dumps(req_body))
    if response.status_code != 200:
        logger.warning(f"search_database_by_keyword err:{response.status_code}, {response.content.decode('utf-8')}")
        return None
    search_res = json.loads(response.content.decode("utf-8"))
    return search_res
