import sys
import os
import json
import traceback
from urllib.parse import quote

import requests

sys.path.append(os.getcwd() + "/../")
import configParse

param = configParse.get_TgToFileSystemParameter()

background_server_url = f"{param.base.exposed_url}"


login_api_route = "/tg/api/v1/client/login"


def login_client_by_qr_code_url() -> str:
    request_url = background_server_url + login_api_route
    response = requests.get(request_url)
    if response.status_code != 200:
        print(f"Could not login, err:{response.status_code}, {response.content.decode('utf-8')}")
        return None
    url_info = json.loads(response.content.decode("utf-8"))
    return url_info.get("url")


status_api_route = "/tg/api/v1/client/status"


def get_backend_client_status(flag: bool = False) -> dict[str, any]:
    request_url = f"{background_server_url}{status_api_route}?flag={flag}"
    response = requests.get(request_url)
    if response.status_code != 200:
        print(f"get_status, backend is running? err:{response.status_code}, {response.content.decode('utf-8')}")
        return None
    return json.loads(response.content.decode("utf-8"))


def get_white_list_chat_dict() -> dict[str, any]:
    backend_status = get_backend_client_status(True)
    try:
        return backend_status["clist"]
    except Exception as err:
        print(f"{err=},{traceback.format_exc()}")
    return {}


search_api_route = "/tg/api/v1/file/search"


def search_database_by_keyword(keyword: str, chat_list: list[int], offset: int, limit: int, is_order: bool) -> list[any] | None:
    request_url = background_server_url + search_api_route
    req_body = {
        "token": param.web.token,
        "search": keyword,
        "chat_ids": chat_list,
        "index": offset,
        "length": limit,
        "refresh": False,
        "inner": False,
        "inc": is_order,
    }

    response = requests.post(request_url, data=json.dumps(req_body))
    if response.status_code != 200:
        print(f"search_database_by_keyword err:{response.status_code}, {response.content.decode('utf-8')}")
        return None
    search_res = json.loads(response.content.decode("utf-8"))
    return search_res


link_convert_api_route = "/tg/api/v1/client/link_convert"


def convert_tg_link_to_proxy_link(link: str) -> str:
    link = quote(link, safe="")
    request_url = background_server_url + link_convert_api_route + f"?link={link}"
    response = requests.get(request_url)
    if response.status_code != 200:
        print(f"link convert fail: {response.status_code}, {response.content.decode('utf-8')}")
        return ""
    response_js = json.loads(response.content.decode("utf-8"))
    return response_js["url"]
