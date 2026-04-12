import sys
import os
import threading

import httpx

sys.path.append(os.getcwd() + "/../")
import configParse

param = configParse.get_TgToFileSystemParameter()
BASE_URL = param.base.exposed_url

# Httpx client with connection pooling
_client: httpx.Client | None = None
_client_lock = threading.Lock()


def _get_client() -> httpx.Client:
    """Get or create httpx client with connection pooling (thread-safe)."""
    global _client
    with _client_lock:
        if _client is None or _client.is_closed:
            _client = httpx.Client(
                base_url=BASE_URL,
                timeout=30.0,
                follow_redirects=True,
            )
    return _client


def close_client():
    """Close httpx client. Call on app shutdown."""
    global _client
    with _client_lock:
        if _client is not None:
            _client.close()
            _client = None


def _request(method: str, path: str, **kwargs) -> dict | None:
    """
    Unified request helper with error handling.

    Args:
        method: HTTP method (GET/POST)
        path: API route path
        **kwargs: httpx request kwargs (params, json, etc.)

    Returns:
        Response JSON dict, or None on error
    """
    client = _get_client()
    try:
        response = client.request(method, path, **kwargs)
        if response.status_code != 200:
            print(f"API error {path}: {response.status_code} - {response.text}")
            return None
        return response.json()
    except httpx.RequestError as err:
        print(f"Request error {path}: {err}")
        return None


# === API Routes ===

LOGIN_ROUTE = "/tg/api/v1/client/login"


def login_client_by_qr_code_url() -> str | None:
    """Get QR login URL from backend."""
    res = _request("GET", LOGIN_ROUTE)
    if res is None:
        return None
    return res.get("url")


STATUS_ROUTE = "/tg/api/v1/client/status"


def get_backend_client_status(flag: bool = False) -> dict | None:
    """Get backend client status."""
    return _request("GET", STATUS_ROUTE, params={"flag": flag})


def get_white_list_chat_dict() -> dict:
    """Get whitelist chat dict with chat_id -> chat_info mapping."""
    status = get_backend_client_status(flag=True)
    if status is None:
        return {}
    return status.get("clist", {})


SEARCH_ROUTE = "/tg/api/v1/file/search"


def search_database_by_keyword(
    sign: str,
    keyword: str,
    chat_ids: list[int],
    offset: int,
    limit: int,
    inc: bool,
) -> dict | None:
    """Search messages by keyword in specified chats."""
    body = {
        "sign": sign,
        "search": keyword,
        "chat_ids": chat_ids,
        "index": offset,
        "length": limit,
        "refresh": False,
        "inner": False,
        "inc": inc,
    }
    return _request("POST", SEARCH_ROUTE, json=body)


LINK_CONVERT_ROUTE = "/tg/api/v1/client/link_convert"


def convert_tg_link_to_proxy_link(link: str) -> str:
    """Convert Telegram message link to proxy download URL."""
    res = _request("GET", LINK_CONVERT_ROUTE, params={"link": link})
    if res is None:
        return ""
    return res.get("url", "")


def get_config_default_name() -> str:
    """Get default client name from config."""
    return param.web.name