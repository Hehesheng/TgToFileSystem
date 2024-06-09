import asyncio
import json
import os
import logging
import traceback
from urllib.parse import quote

import uvicorn
from fastapi import FastAPI, status, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, StreamingResponse
from contextlib import asynccontextmanager
from telethon import types, hints, utils
from pydantic import BaseModel

import configParse
from backend import apiutils
from backend import api_implement as api
from backend.TgFileSystemClientManager import TgFileSystemClientManager

logger = logging.getLogger(__file__.split("/")[-1])


@asynccontextmanager
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
    token: str
    search: str = ""
    chat_ids: list[int] = []
    index: int = 0
    length: int = 10
    refresh: bool = False
    inner: bool = False
    inc: bool = False


@app.post("/tg/api/v1/file/search")
@apiutils.atimeit
async def search_tg_file_list(body: TgToFileListRequestBody):
    try:
        param = configParse.get_TgToFileSystemParameter()
        clients_mgr = TgFileSystemClientManager.get_instance()
        res = hints.TotalList()
        res_type = "msg"
        client = await clients_mgr.get_client_force(body.token)
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
            msg_info["download_url"] = f"{param.base.exposed_url}/tg/api/v1/file/get/{chat_id}/{msg_id}/{file_name}"
            msg_info["src_tg_link"] = f"https://t.me/c/{chat_id}/{msg_id}"
            res_dict.append(msg_info)

        client_dict = json.loads(client.to_json())
        client_dict["sign"] = body.token

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
        res = hints.TotalList()
        res_type = "chat"
        client = await clients_mgr.get_client_force(body.token)
        res_dict = []
        if body.search != "":
            res = await client.get_messages_by_search(
                body.chat_id, search_word=body.search, limit=body.length, offset=body.index, inner_search=body.inner
            )
        else:
            res = await client.get_messages(body.chat_id, limit=body.length, offset=body.index)
        res_type = "msg"
        for item in res:
            file_name = apiutils.get_message_media_name(item)
            if file_name == "":
                file_name = "unknown.tmp"
            msg_info = json.loads(item.to_json())
            msg_info["file_name"] = file_name
            msg_info["download_url"] = (
                f"{param.base.exposed_url}/tg/api/v1/file/get/{body.chat_id}/{item.id}/{file_name}?sign={body.token}"
            )
            res_dict.append(msg_info)

        response_dict = {
            "client": json.loads(client.to_json()),
            "type": res_type,
            "length": len(res_dict),
            "list": res_dict,
        }
        return Response(json.dumps(response_dict), status_code=status.HTTP_200_OK)
    except Exception as err:
        logger.error(f"{err=},{traceback.format_exc()}")
        return Response(json.dumps({"detail": f"{err=}"}), status_code=status.HTTP_404_NOT_FOUND)


@app.get("/tg/api/v1/file/msg")
@apiutils.atimeit
async def get_tg_file_media_stream(token: str, cid: int, mid: int, request: Request):
    msg_id = mid
    chat_id = cid
    headers = {
        # "content-type": "video/mp4",
        "accept-ranges": "bytes",
        "content-encoding": "identity",
        # "content-length": stream_file_size,
        "access-control-expose-headers": ("content-type, accept-ranges, content-length, " "content-range, content-encoding"),
    }
    range_header = request.headers.get("range")
    try:
        clients_mgr = TgFileSystemClientManager.get_instance()
        client = await clients_mgr.get_client_force(token)
        msg = await client.get_message(chat_id, msg_id)
        file_size = msg.media.document.size
        start = 0
        end = file_size - 1
        status_code = status.HTTP_200_OK
        mime_type = msg.media.document.mime_type
        headers["content-type"] = mime_type
        # headers["content-length"] = str(file_size)
        file_name = apiutils.get_message_media_name(msg)
        if file_name == "":
            maybe_file_type = mime_type.split("/")[-1]
            file_name = f"{chat_id}.{msg_id}.{maybe_file_type}"
        headers["Content-Disposition"] = f"inline; filename*=utf-8'{quote(file_name)}'"

        if range_header is not None:
            start, end = apiutils.get_range_header(range_header, file_size)
            size = end - start + 1
            # headers["content-length"] = str(size)
            headers["content-range"] = f"bytes {start}-{end}/{file_size}"
            status_code = status.HTTP_206_PARTIAL_CONTENT
        else:
            headers["content-length"] = str(file_size)
            headers["content-range"] = f"bytes 0-{file_size-1}/{file_size}"
        return StreamingResponse(
            client.streaming_get_iter(msg, start, end, request),
            headers=headers,
            media_type=mime_type,
            status_code=status_code,
        )
    except Exception as err:
        logger.error(f"{err=},{traceback.format_exc()}")
        return Response(json.dumps({"detail": f"{err=}"}), status_code=status.HTTP_404_NOT_FOUND)


@app.get("/tg/api/v1/file/get/{chat_id}/{msg_id}/{file_name}")
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


if __name__ == "__main__":
    param = configParse.get_TgToFileSystemParameter()
    uvicorn.run(app, host="0.0.0.0", port=param.base.port)
