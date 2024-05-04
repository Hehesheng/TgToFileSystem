import asyncio

import uvicorn
from fastapi import FastAPI
from fastapi import status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from contextlib import asynccontextmanager
from telethon import TelegramClient

import configParse

@asynccontextmanager
async def lifespan(app: FastAPI):
    param = configParse.get_TgToFileSystemParameter()
    loop = asyncio.get_event_loop()
    tg_client_task = loop.create_task(start_tg_client(param))
    yield
    asyncio.gather(*[tg_client_task])

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/tg/{chat_id}/{message_id}")
async def get_test(chat_id: str, message_id: str):
    print(f"test: {chat_id=}, {message_id=}")
    return Response(status_code=status.HTTP_200_OK)
    

async def start_tg_client(param: configParse.TgToFileSystemParameter):
    api_id = param.tgApi.api_id
    api_hash = param.tgApi.api_hash
    session_name = param.base.name
    proxy_param = {
        'proxy_type': param.proxy.proxy_type,
        'addr': param.proxy.addr,
        'port': param.proxy.port,
    } if param.proxy.enable else {}
    client = TelegramClient(session_name, api_id, api_hash, proxy=proxy_param)

    async def tg_client_main():
        # Getting information about yourself
        me = await client.get_me()

        # "me" is a user object. You can pretty-print
        # any Telegram object with the "stringify" method:
        print(me.stringify())

        # When you print something, you see a representation of it.
        # You can access all attributes of Telegram objects with
        # the dot operator. For example, to get the username:
        username = me.username
        print(username)
        print(me.phone)
        # You can print all the dialogs/conversations that you are part of:
        dialogs = await client.get_dialogs()
        for dialog in dialogs:
            print(f"{dialog.name} has ID {dialog.id}")
        # async for dialog in client.iter_dialogs():
        #     print(dialog.name, 'has ID', dialog.id)

    async with client:
        await tg_client_main()



if __name__ == "__main__":
    param = configParse.get_TgToFileSystemParameter()
    uvicorn.run(app, host="0.0.0.0", port=param.base.port)
