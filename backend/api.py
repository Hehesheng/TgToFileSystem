import asyncio
import json
import os
import sys
import logging
import traceback
from typing import Annotated
from urllib.parse import quote
from datetime import datetime

import uvicorn
from fastapi import FastAPI, status, Request, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, StreamingResponse
from telethon import types, hints, utils
from pydantic import BaseModel

import configParse
from backend import apiutils
from backend import api_implement as api
from backend.TgFileSystemClientManager import TgFileSystemClientManager, EnumSignLevel
from backend.UserManager import UserManager

logger = logging.getLogger(__file__.split("/")[-1])


async def lifespan(app: FastAPI):
    clients_mgr = TgFileSystemClientManager.get_instance()
    res = await clients_mgr.get_status()
    logger.info(f"init clients manager:{res}")
    yield


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class TgToFileListRequestBody(BaseModel):
    sign: str
    search: str = ""
    chat_ids: list[int] = []
    index: int = 0
    length: int = 10
    refresh: bool = False
    inner: bool = False
    inc: bool = False


async def verify_post_sign(body: TgToFileListRequestBody):
    clients_mgr = TgFileSystemClientManager.get_instance()
    if not clients_mgr.verify_sign(body.sign):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"{body}")


async def verify_get_sign(sign: str):
    clients_mgr = TgFileSystemClientManager.get_instance()
    if not clients_mgr.verify_sign(sign):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"{sign}")
    return sign


@app.post("/tg/api/v1/file/search", dependencies=[Depends(verify_post_sign)])
@apiutils.atimeit
async def search_tg_file_list(body: TgToFileListRequestBody):
    try:
        clients_mgr = TgFileSystemClientManager.get_instance()
        param = configParse.get_TgToFileSystemParameter()
        res = hints.TotalList()
        res_type = "msg"
        sign_info = clients_mgr.parse_sign(body.sign)
        client_id = TgFileSystemClientManager.get_sign_client_id(sign_info)
        client = await clients_mgr.get_client_force(client_id)
        res_dict = []
        res = await client.get_messages_by_search_db(
            body.chat_ids, body.search, limit=body.length, inc=body.inc, offset=body.index
        )
        for item in res:
            msg_info = json.loads(item)
            file_name = apiutils.get_message_media_name_from_dict(msg_info)
            chat_id = apiutils.get_message_chat_id_from_dict(msg_info)
            msg_id = apiutils.get_message_msg_id_from_dict(msg_info)
            msg_info["file_name"] = file_name
            msg_info["download_url"] = f"{param.base.exposed_url}/tg/api/v1/file/get/{chat_id}/{msg_id}/{quote(file_name)}"
            msg_info["src_tg_link"] = f"https://t.me/c/{chat_id}/{msg_id}"
            res_dict.append(msg_info)

        client_dict = json.loads(client.to_json())
        client_dict["sign"] = body.sign

        response_dict = {
            "client": client_dict,
            "type": res_type,
            "length": len(res_dict),
            "list": res_dict,
        }
        return Response(json.dumps(response_dict), status_code=status.HTTP_200_OK)
    except Exception as err:
        logger.error(f"{err=},{traceback.format_exc()}")
        return Response(json.dumps({"detail": f"{err=}"}), status_code=status.HTTP_404_NOT_FOUND)


@app.post("/tg/api/v1/file/list")
@apiutils.atimeit
async def get_tg_file_list(body: TgToFileListRequestBody):
    try:
        clients_mgr = TgFileSystemClientManager.get_instance()
        param = configParse.get_TgToFileSystemParameter()
        res = hints.TotalList()
        res_type = "chat"
        sign_info = clients_mgr.parse_sign(body.sign)
        client_id = TgFileSystemClientManager.get_sign_client_id(sign_info)
        client = await clients_mgr.get_client_force(client_id)
        res_dict = []
        if body.search != "":
            res = await client.get_messages_by_search(
                int(sign_info.get("chat_id", 0)), search_word=body.search, limit=body.length, offset=body.index, inner_search=body.inner
            )
        else:
            res = await client.get_messages(int(sign_info.get("chat_id", 0)), limit=body.length, offset=body.index)
        res_type = "msg"
        for item in res:
            file_name = apiutils.get_message_media_name(item)
            if file_name == "":
                file_name = "unknown.tmp"
            msg_info = json.loads(item.to_json())
            msg_info["file_name"] = file_name
            msg_info["download_url"] = (
                f"{param.base.exposed_url}/tg/api/v1/file/get/{item.chat_id}/{item.id}/{quote(file_name)}?sign={body.sign}"
            )
            res_dict.append(msg_info)

        client_dict = json.loads(client.to_json())
        client_dict["sign"] = body.sign

        response_dict = {
            "client": client_dict,
            "type": res_type,
            "length": len(res_dict),
            "list": res_dict,
        }
        return Response(json.dumps(response_dict), status_code=status.HTTP_200_OK)
    except Exception as err:
        logger.error(f"{err=},{traceback.format_exc()}")
        return Response(json.dumps({"detail": f"{err=}"}), status_code=status.HTTP_404_NOT_FOUND)


@app.get("/tg/api/v1/file/msg", deprecated=[Depends(verify_get_sign)])
@apiutils.atimeit
async def get_tg_file_media_stream(sign: str, cid: int, mid: int, request: Request):
    try:
        sign = sign.replace(" ", "+")
        return await api.get_media_file_stream(sign, cid, mid, request)
    except Exception as err:
        logger.error(f"{err=},{traceback.format_exc()}")
        return Response(json.dumps({"detail": f"{err=}"}), status_code=status.HTTP_404_NOT_FOUND)


@app.get("/tg/api/v1/file/get/{chat_id}/{msg_id}/{file_name}", dependencies=[Depends(verify_get_sign)])
@apiutils.atimeit
async def get_tg_file_media(chat_id: int | str, msg_id: int, file_name: str, sign: str, req: Request):
    try:
        if isinstance(chat_id, str):
            chat_id = int(chat_id)
        return await get_tg_file_media_stream(sign, chat_id, msg_id, req)
    except Exception as err:
        logger.error(f"{err=},{traceback.format_exc()}")
        return Response(json.dumps({"detail": f"{err=}"}), status_code=status.HTTP_404_NOT_FOUND)


@app.get("/tg/api/v1/client/login")
@apiutils.atimeit
async def login_new_tg_file_client():
    clients_mgr = TgFileSystemClientManager.get_instance()
    url = await clients_mgr.login_clients()
    return {"url": url}


@app.get("/tg/api/v1/client/status")
async def get_tg_file_client_status(flag: bool = False, request: Request = None):
    return await api.get_clients_manager_status(flag)


@app.get("/tg/api/v1/client/link_convert")
@apiutils.atimeit
async def convert_tg_msg_link_media_stream(link: str):
    try:
        url = await api.link_convert(link)
        logger.info(f"{link}: link convert to: {url}")
        return Response(json.dumps({"url": url}), status_code=status.HTTP_200_OK)
    except Exception as err:
        logger.error(f"{err=},{traceback.format_exc()}")
        return Response(json.dumps({"detail": "link invalid", "err": f"{err}"}), status_code=status.HTTP_404_NOT_FOUND)


@app.get("/tg/api/v1/client/profile_photo")
@apiutils.atimeit
async def get_tg_chat_profile_photo(chat_id: int, sign: str):
    raise NotImplementedError


class TgToChatListRequestBody(BaseModel):
    token: str
    search: str = ""
    index: int = 0
    length: int = 0
    refresh: bool = False


@app.post("/tg/api/v1/client/chat")
@apiutils.atimeit
async def get_tg_client_chat_list(body: TgToChatListRequestBody, request: Request):
    try:
        clients_mgr = TgFileSystemClientManager.get_instance()
        res = hints.TotalList()
        res_type = "chat"
        client = await clients_mgr.get_client_force(body.token)
        res_dict = {}

        res = await client.get_dialogs(limit=body.length, offset=body.index, refresh=body.refresh)
        res_dict = [
            {
                "id": item.id,
                "is_channel": item.is_channel,
                "is_group": item.is_group,
                "is_user": item.is_user,
                "name": item.name,
            }
            for item in res
        ]

        response_dict = {
            "client": json.loads(client.to_json()),
            "type": res_type,
            "length": len(res_dict),
            "list": res_dict,
        }
        return Response(json.dumps(response_dict), status_code=status.HTTP_200_OK)
    except Exception as err:
        logger.error(f"{err=}")
        return Response(json.dumps({"detail": f"{err=}"}), status_code=status.HTTP_404_NOT_FOUND)


async def get_verify(id: str = None):
    if id is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"{id=}")
    client_mgr = TgFileSystemClientManager.get_instance()
    client = await client_mgr.get_client_force(id)
    if not client.is_valid():
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"{id=}")


@app.get("/tg/api/v1/test", dependencies=[Depends(get_verify)])
async def test_get_depends_verify_method(id: str, other: str = ""):
    client_mgr = TgFileSystemClientManager.get_instance()
    client = await client_mgr.get_client_force(id)
    return Response((await client.client.get_me()).stringify())


async def post_verify(body: TgToChatListRequestBody | None = None):
    if not body or not body.token:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST)
    return body


@app.post("/tg/api/v1/test", dependencies=[Depends(post_verify)])
async def test_get_depends_verify_method(body: TgToChatListRequestBody):
    return Response()


@app.get("/tg/api/v1/rss/search")
@apiutils.atimeit
async def rss_search(keyword: str, sign: str, limit: int = 50):
    """
    RSS search endpoint for ani player.

    Returns RSS XML format with search results.
    Uses client's whitelist_chat from sign for search scope.
    """
    try:
        # Verify sign and get client
        clients_mgr = TgFileSystemClientManager.get_instance()
        if not clients_mgr.verify_sign(sign):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid sign")

        param = configParse.get_TgToFileSystemParameter()

        # Get client_id from sign
        sign_info = clients_mgr.parse_sign(sign)
        client_id = TgFileSystemClientManager.get_sign_client_id(sign_info)
        client = await clients_mgr.get_client_force(client_id)

        # Use client's whitelist_chat for search scope
        chat_ids = client.client_param.whitelist_chat

        # Search from database
        db = UserManager()
        results = db.get_msg_by_chat_id_and_keyword(
            chat_ids=chat_ids,
            keyword=keyword,
            limit=limit,
        )

        # Build RSS XML
        rss_items = []
        for row in results:
            # row is tuple: (unique_id, user_id, chat_id, msg_id, msg_type, msg_ctx, mime_type, file_name, msg_js, date_time)
            chat_id = row[2]
            msg_id = row[3]
            file_name = row[7] or "unknown.tmp"
            msg_js = row[8]
            date_time = row[9]

            # Parse msg_js for additional info
            try:
                msg_info = json.loads(msg_js) if msg_js else {}
            except:
                msg_info = {}

            download_url = f"{param.base.exposed_url}/tg/api/v1/file/get/{chat_id}/{msg_id}/{quote(file_name)}?sign={sign}"

            # Format date
            # Format date (Telegram stores nanoseconds, convert to seconds)
            pub_date = ""
            if date_time:
                try:
                    # date_time is in nanoseconds, convert to seconds
                    ts_seconds = date_time / 1_000_000_000
                    pub_date = datetime.fromtimestamp(ts_seconds).strftime("%a, %d %b %Y %H:%M:%S +0000")
                except (OSError, ValueError):
                    pub_date = ""

            # Get file size from msg_js
            file_size = 0
            media = msg_info.get("media", {})
            if isinstance(media, dict):
                file_size = media.get("size", 0)
            size_str = f"{file_size / 1024 / 1024:.1f} MB" if file_size > 0 else ""

            rss_items.append(f"""
    <item>
      <title>{file_name}</title>
      <link>{download_url}</link>
      <description>Size: {size_str}</description>
      <pubDate>{pub_date}</pubDate>
    </item>""")

        rss_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>TgToFileSystem Search: {keyword}</title>
    <link>{param.base.exposed_url}</link>
    <description>Telegram media search results</description>
    <language>zh-CN</language>
{"".join(rss_items)}
  </channel>
</rss>"""

        return Response(rss_xml, media_type="application/xml", status_code=status.HTTP_200_OK)

    except Exception as err:
        logger.error(f"{err=},{traceback.format_exc()}")
        return Response(f"<error>{err}</error>", media_type="application/xml", status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)


@app.get("/tg/api/v1/ani/search")
@apiutils.atimeit
async def ani_search(keyword: str, sign: str, limit: int = 50):
    """
    Ani player web-selector search endpoint.

    Returns HTML page with search results that ani player can parse with CSS selectors.
    """
    try:
        # Verify sign and get client
        clients_mgr = TgFileSystemClientManager.get_instance()
        if not clients_mgr.verify_sign(sign):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid sign")

        param = configParse.get_TgToFileSystemParameter()

        # Get client_id from sign
        sign_info = clients_mgr.parse_sign(sign)
        client_id = TgFileSystemClientManager.get_sign_client_id(sign_info)
        client = await clients_mgr.get_client_force(client_id)

        # Use client's whitelist_chat for search scope
        chat_ids = client.client_param.whitelist_chat

        # Search from database
        db = UserManager()
        results = db.get_msg_by_chat_id_and_keyword(
            chat_ids=chat_ids,
            keyword=keyword,
            limit=limit,
        )

        # Build HTML page for web-selector parsing
        # Structure: each result is a module-card-item with download link
        html_items = []
        for row in results:
            chat_id = row[2]
            msg_id = row[3]
            file_name = row[7] or "unknown.tmp"
            msg_js = row[8]

            # Generate unique file_id for this result
            file_id = f"{chat_id}_{msg_id}"

            # Detail page URL - animeko will fetch this as the "subject detail"
            detail_url = f"{param.base.exposed_url}/tg/api/v1/ani/detail/{file_id}?sign={sign}"

            # Parse msg_js for size info
            try:
                msg_info = json.loads(msg_js) if msg_js else {}
            except:
                msg_info = {}

            media = msg_info.get("media", {})
            if isinstance(media, dict):
                file_size = media.get("size", 0)
            else:
                file_size = 0

            size_str = f"{file_size / 1024 / 1024:.1f}MB" if file_size > 0 else ""

            # HTML item: link to detail page (not directly to video)
            # title attribute contains filename for animeko's subject name extraction
            html_items.append(f"""    <div class="module-card-item">
      <div class="module-card-item-info">
        <div class="module-card-item-title">
          <a href="{detail_url}" title="{file_name}">{file_name}</a>
        </div>
        <div class="module-card-item-desc">{size_str}</div>
      </div>
    </div>""")

        # Return HTML directly with links to detail pages
        # animeko flow: search → extract subject links → fetch detail page → extract episode links
        search_html = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>TgToFileSystem Search: {keyword}</title>
</head>
<body>
  <div class="module-search">
    <div class="module-card-list">
{"".join(html_items)}
    </div>
  </div>
</body>
</html>"""

        return Response(search_html, media_type="text/html", status_code=status.HTTP_200_OK)

    except Exception as err:
        logger.error(f"{err=},{traceback.format_exc()}")
        return Response(f"<html><body><error>{err}</error></body></html>", media_type="text/html", status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)


@app.get("/tg/api/v1/ani/detail/{file_id}")
@apiutils.atimeit
async def ani_detail(file_id: str, sign: str):
    """
    Ani player detail page endpoint.

    Returns HTML page with the video link for a specific file.
    animeko treats this as the "subject detail page" and extracts episode links.
    """
    try:
        # Verify sign
        clients_mgr = TgFileSystemClientManager.get_instance()
        if not clients_mgr.verify_sign(sign):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid sign")

        param = configParse.get_TgToFileSystemParameter()

        # Parse file_id (format: chat_id_msg_id)
        parts = file_id.split("_")
        if len(parts) != 2:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid file_id format")
        chat_id = int(parts[0])
        msg_id = int(parts[1])

        # Get file info from database
        db = UserManager()
        results = db.get_msg_by_chat_id_and_msg_id(chat_id, msg_id)

        # Find the message
        file_name = "unknown.tmp"
        if results:
            row = results[0]
            file_name = row[7] or "unknown.tmp"

        # Build video download URL
        download_url = f"{param.base.exposed_url}/tg/api/v1/file/get/{chat_id}/{msg_id}/{quote(file_name)}?sign={sign}"

        # Return HTML with single episode link
        # animeko will use selectorChannelFormatNoChannel.selectEpisodes to extract this
        detail_html = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>TgToFileSystem Detail: {file_name}</title>
</head>
<body>
  <div class="module-play">
    <div class="module-card-list">
      <div class="module-card-item">
        <div class="module-card-item-info">
          <div class="module-card-item-title">
            <a href="{download_url}" title="{file_name}">{file_name}</a>
          </div>
        </div>
      </div>
    </div>
  </div>
</body>
</html>"""

        return Response(detail_html, media_type="text/html", status_code=status.HTTP_200_OK)

    except Exception as err:
        logger.error(f"{err=},{traceback.format_exc()}")
        return Response(f"<html><body><error>{err}</error></body></html>", media_type="text/html", status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)


@app.get("/tg/api/v1/ani/source/{api_key}")
@apiutils.atimeit
async def ani_source(api_key: str):
    """
    Generate ani player media source config (web-selector format).

    Requires api_key in path (random string configured in config.toml).
    Returns JSON with 24h valid sign for search.
    """
    try:
        param = configParse.get_TgToFileSystemParameter()

        # Verify api_key
        if not param.base.ani_api_key or api_key != param.base.ani_api_key:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid API key")

        clients_mgr = TgFileSystemClientManager.get_instance()

        # Get first available client
        mgr_status = await clients_mgr.get_status()
        clients_list = mgr_status.get("clients", [])
        if not clients_list:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="No available clients")

        target_client_name = clients_list[0].get("name", "")

        # Generate 24h valid sign
        sign = clients_mgr.generate_sign(
            client_id=target_client_name,
            sign_type=EnumSignLevel.NORMAL,
            valid_seconds=-1,  # 24h default
        )
        logger.info(f"Generated 24h sign for ani player, client: {target_client_name}")

        # Build search URL template with {keyword} placeholder for ani player
        search_url_template = f"{param.base.exposed_url}/tg/api/v1/ani/search?keyword={{keyword}}&sign={sign}&limit=50"

        source_config = {
            "exportedMediaSourceDataList": {
                "mediaSources": [
                    {
                        "factoryId": "web-selector",
                        "version": 2,
                        "arguments": {
                            "name": "TgToFileSystem",
                            "description": "Telegram media from TgToFileSystem (sign valid for 24h)",
                            "iconUrl": f"{param.base.exposed_url}/favicon.ico",
                            "searchConfig": {
                                "searchUrl": search_url_template,
                                "searchUseOnlyFirstWord": False,
                                "searchRemoveSpecial": False,
                                "rawBaseUrl": "",
                                "requestInterval": 3000,
                                "subjectFormatId": "a",
                                "selectorSubjectFormatA": {
                                    "selectLists": ".module-card-item>.module-card-item-info>.module-card-item-title>a",
                                    "preferShorterName": False,
                                },
                                "selectorSubjectFormatIndexed": {
                                    "selectNames": ".module-card-item>.module-card-item-info>.module-card-item-title>a",
                                    "selectLinks": ".module-card-item>.module-card-item-info>.module-card-item-title>a",
                                    "preferShorterName": False,
                                },
                                "selectorSubjectFormatJsonPathIndexed": {
                                    "selectLinks": "$[*]['url', 'link']",
                                    "selectNames": "$[*]['title','name']",
                                    "preferShorterName": False,
                                },
                                "channelFormatId": "no-channel",
                                "selectorChannelFormatFlattened": {
                                    "selectChannelNames": ".module-tab-item>span",
                                    "matchChannelName": "^(.+)$",
                                    "selectEpisodeLists": ".module-card-list",
                                    "selectEpisodesFromList": ".module-card-item>.module-card-item-info>.module-card-item-title>a",
                                    "selectEpisodeLinksFromList": "",
                                    "matchEpisodeSortFromName": "(第\\s*(?<ep>.+)\\s*[话集])|(?<ep>\\d+)",
                                },
                                "selectorChannelFormatNoChannel": {
                                    "selectEpisodes": ".module-card-item>.module-card-item-info>.module-card-item-title>a",
                                    "selectEpisodeLinks": "",
                                    "matchEpisodeSortFromName": "(第\\s*(?<ep>\\d+)\\s*[话集])|(E(?<ep>\\d+))|(\\s(?<ep>\\d+)\\s)|(?<ep>\\d+)",
                                },
                                "defaultResolution": "1080P",
                                "filterByEpisodeSort": True,
                                "filterBySubjectName": True,
                                "selectMedia": {
                                    "distinguishSubjectName": True,
                                    "distinguishChannelName": True,
                                },
                                "matchVideo": {
                                    "enableNestedUrl": False,
                                    "matchNestedUrl": "$^",
                                    "matchVideoUrl": f"({param.base.exposed_url}/tg/api/v1/file/get/.+)",
                                    "cookies": "",
                                    "addHeadersToVideo": {
                                        "referer": "",
                                        "userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
                                    },
                                },
                            },
                            "tier": 0,
                        },
                    }
                ]
            }
        }

        return Response(json.dumps(source_config, ensure_ascii=False), media_type="application/json", status_code=status.HTTP_200_OK)

    except HTTPException:
        raise
    except Exception as err:
        logger.error(f"{err=},{traceback.format_exc()}")
        return Response(json.dumps({"error": str(err)}), status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)


if __name__ == "__main__":
    param = configParse.get_TgToFileSystemParameter()
    isDebug = True if sys.gettrace() else False
    uvicorn.run(app, host="0.0.0.0", port=param.base.port, reload=isDebug)
