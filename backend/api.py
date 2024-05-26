import asyncio
import json
import os

import uvicorn
from fastapi import FastAPI, status, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, StreamingResponse
from contextlib import asynccontextmanager
from telethon import types, hints
from pydantic import BaseModel

import configParse
from backend import apiutils
from backend.TgFileSystemClientManager import TgFileSystemClientManager

clients_mgr: TgFileSystemClientManager = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global clients_mgr
    param = configParse.get_TgToFileSystemParameter()
    clients_mgr = TgFileSystemClientManager(param)
    yield

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/tg/api/v1/file/login")
@apiutils.atimeit
async def login_new_tg_file_client():
    raise NotImplementedError


class TgToFileListRequestBody(BaseModel):
    token: str
    search: str = ""
    chat_id: int = 0
    index: int = 0
    length: int = 10
    refresh: bool = False
    inner: bool = False
    inc: bool = False

@app.post("/tg/api/v1/file/search")
@apiutils.atimeit
async def search_tg_file_list(body: TgToFileListRequestBody):
    try:
        res = hints.TotalList()
        res_type = "msg"
        client = await clients_mgr.get_client_force(body.token)
        res_dict = {}
        res = await client.get_messages_by_search_db(body.chat_id, body.search, limit=body.length, inc=body.inc, offset=body.index)
        res_dict = [json.loads(item) for item in res]

        response_dict = {
            "client": json.loads(client.to_json()),
            "type": res_type,
            "length": len(res_dict),
            "list": res_dict,
        }
        return Response(json.dumps(response_dict), status_code=status.HTTP_200_OK)
    except Exception as err:
        print(f"{err=}")
        return Response(json.dumps({"detail": f"{err=}"}), status_code=status.HTTP_404_NOT_FOUND)


@app.post("/tg/api/v1/file/list")
@apiutils.atimeit
async def get_tg_file_list(body: TgToFileListRequestBody):
    try:
        res = hints.TotalList()
        res_type = "chat"
        client = await clients_mgr.get_client_force(body.token)
        res_dict = {}
        if body.chat_id == 0:
            res = await client.get_dialogs(limit=body.length, offset=body.index, refresh=body.refresh)
            res_dict = [{"id": item.id, "is_channel": item.is_channel,
                         "is_group": item.is_group, "is_user": item.is_user, "name": item.name, } for item in res]
        elif body.search != "":
            res = await client.get_messages_by_search(body.chat_id, search_word=body.search, limit=body.length, offset=body.index, inner_search=body.inner)
            res_type = "msg"
            res_dict = [json.loads(item.to_json()) for item in res]
        else:
            res = await client.get_messages(body.chat_id, limit=body.length, offset=body.index)
            res_type = "msg"
            res_dict = [json.loads(item.to_json()) for item in res]

        response_dict = {
            "client": json.loads(client.to_json()),
            "type": res_type,
            "length": len(res_dict),
            "list": res_dict,
        }
        return Response(json.dumps(response_dict), status_code=status.HTTP_200_OK)
    except Exception as err:
        print(f"{err=}")
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
        "access-control-expose-headers": (
            "content-type, accept-ranges, content-length, "
            "content-range, content-encoding"
        ),
    }
    range_header = request.headers.get("range")
    try:
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
        headers[
            "Content-Disposition"] = f'Content-Disposition: inline; filename="{file_name.encode("utf-8")}"'

        if range_header is not None:
            start, end = apiutils.get_range_header(range_header, file_size)
            size = end - start + 1
            # headers["content-length"] = str(size)
            headers["content-range"] = f"bytes {start}-{end}/{file_size}"
            status_code = status.HTTP_206_PARTIAL_CONTENT
        return StreamingResponse(
            client.streaming_get_iter(msg, start, end, request),
            headers=headers,
            status_code=status_code,
        )
    except Exception as err:
        print(f"{err=}")
        return Response(json.dumps({"detail": f"{err=}"}), status_code=status.HTTP_404_NOT_FOUND)


@app.get("/tg/api/v1/file/get/{file_name}")
@apiutils.atimeit
async def get_tg_file_media_stream2(file_name: str, sign: str, req: Request):
    raise NotImplementedError


@app.get("/tg/api/v1/file/link_convert")
@apiutils.atimeit
async def convert_tg_msg_link_media_stream(link: str, token: str):
    raise NotImplementedError

if __name__ == "__main__":
    param = configParse.get_TgToFileSystemParameter()
    uvicorn.run(app, host="0.0.0.0", port=param.base.port)
