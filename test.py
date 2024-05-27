import logging.handlers
import time
import asyncio
import json
import rsa
import pickle
import base64
from datetime import datetime
import logging
import os
import yaml

from telethon import TelegramClient, utils, types
import diskcache

from backend.UserManager import UserManager
from backend import apiutils

import configParse

with open('logging_config.yaml', 'r') as f:
    logging.config.dictConfig(yaml.safe_load(f.read()))
for handler in logging.getLogger().handlers:
    if isinstance(handler, logging.handlers.TimedRotatingFileHandler):
        handler.suffix = "%Y-%m-%d"
logger = logging.getLogger(__file__.split("/")[-1])

logger.debug('This is a debug message')
logger.info('This is an info message')
logger.warning('This is a warning message')
logger.error('This is an error message')
logger.critical('This is a critical message')

exit(0)

# class TestClass(object):
#     int_value: int = 1
#     float_value: float = 2.0
#     bool_value: bool = True
#     bytes_value: bytes = b'Man! What can i say!'

# src_obj = TestClass()
# with open('tmp', 'wb') as f:
#     src_obj.int_value = 10000000000000
#     import random
#     src_obj.bytes_value = random.randbytes(5*1024*1024)
#     pickle.dump(src_obj, f)
# test_bytes = random.randbytes(5*1024*1024)


# with open('tmp', 'rb') as f:
#     test_bytes = f.read()

# @apiutils.timeit_sec
# def pickle_loads_test(loop) -> TestClass:
#     obj_cls: TestClass|None = None
#     for _ in range(loop):
#         obj_cls = pickle.loads(obj_bytes)
#     return obj_cls

# @apiutils.timeit_sec
# def pickle_dumps_test(loop) -> bytes:
#     obj_bytes: bytes|None = None
#     for _ in range(loop):
#         obj_bytes = pickle.dumps(obj_cls)
#     return obj_bytes

# for i in range(10):
#     print(f"loop:{i}")
#     test_obj = pickle_loads_test(test_bytes, 1000)
#     pickle_dumps_test(test_obj, 1000)

# exit(0)

# cache = diskcache.Cache("./cacheTest", size_limit=2**30, eviction_policy='least-recently-used')
# random_key = random.randbytes(1000)
# @apiutils.timeit_sec
# def test_db_write_cache():
#     for i in range(1000):
#         cache.add(int(random_key[i]), test_bytes, expire=300)
# @apiutils.timeit_sec
# def test_db_read_cache():
#     for i in range(1000):
#         exist = cache.touch(int(random_key[i]), expire=300)
#         if exist:
#             cache.get(int(random_key[i]))
# test_db_write_cache()
# test_db_read_cache()

# exit(0)

# db = UserManager()
# search_cur = db.con.cursor()
# update_cur = db.con.cursor()
# res = search_cur.execute("SELECT * FROM message")
# cnt = 0
# for row in res:
#     (unique_id, date_time, msg_js) = (row[0], row[-1], row[-2])
#     msg_dic = json.loads(msg_js)
#     date_time_str = msg_dic['date']
#     if date_time is not None or date_time_str is None:
#         continue
#     date = datetime.fromisoformat(date_time_str)
#     ts = int(date.timestamp() * 1_000) * 1_000_000
#     try:
#         update_cur.execute(f"UPDATE message SET date_time = {ts} WHERE unique_id == '{unique_id}'")
#     except Exception as err:
#         print(f"{err=}")
#     if cnt % 1000 == 0:
#         db.con.commit()
#         print(cnt)
#     cnt += 1
# db.con.commit()
# print(cnt)
# exit(0)

# pubkey, prikey = rsa.newkeys(1024)
# print(pubkey)
# print(prikey)
# print()
# enc_bytes = rsa.encrypt("token=anonnnnnnn1435145nnnnnnn;cid=-1001216816802;mid=95056;t=2000000000000".encode('utf-8'), pubkey)
# print(enc_bytes)
# print(len(enc_bytes))
# b64enc_str = base64.b64encode(enc_bytes)
# print(b64enc_str.decode('utf-8'))
# print(len(b64enc_str))
# dec_bytes = base64.b64decode(b64enc_str)
# # print(dec_bytes)s
# origin_str = rsa.decrypt(dec_bytes, prikey)
# print(origin_str)
# print(len(origin_str.decode('utf-8')))
# exit(0)

param = configParse.get_TgToFileSystemParameter()
# Remember to use your own values from my.telegram.org!
api_id = param.tgApi.api_id
api_hash = param.tgApi.api_hash
client1 = TelegramClient(f'{os.getcwd()}/backend/db/test.session', api_id, api_hash, proxy={
    # 'proxy_type': 'socks5',
    # 'addr': '172.25.32.1',
    # 'port': 7890,
})
# client2 = TelegramClient(f'{os.getcwd()}/backend/db/anon1.session', api_id, api_hash, proxy={
#     'proxy_type': 'socks5',
#     'addr': '172.25.32.1',
#     'port': 7890,
# })
# client.session.set_dc(2, "91.108.56.198", 443)
# client = TelegramClient('anon', api_id, api_hash, proxy=("socks5", '127.0.0.1', 7890))
# proxy=("socks5", '127.0.0.1', 4444)


async def main(client: TelegramClient):
    # Getting information about yourself
    me = await client.get_me()

    # "me" is a user object. You can pretty-print
    # any Telegram object with the "stringify" method:
    # print(me.stringify())

    # When you print something, you see a representation of it.
    # You can access all attributes of Telegram objects with
    # the dot operator. For example, to get the username:
    username = me.username
    print(username)
    print(me.phone)
    
    msg = await client.get_messages(1216816802, ids=[99334])
    # client.download_media(msg, )
    # print(path)

    # client.get_entity
    # i = 0
    # async for msg in client.iter_messages('pitaogo'):
    #     print(f'{msg.id=} ,{msg.message=}, {msg.media=}')
    #     i += 1
    #     if i >= 10:
    #         break
    # You can print all the dialogs/conversations that you are part of:
    # peer_type_list = []
    # async for dialog in client.iter_dialogs():
    #     real_id, peer_type = utils.resolve_id(dialog.id)
    #     if peer_type in peer_type_list:
    #         continue
    #     peer_type_list.append(peer_type)
    #     print(f'{dialog.name} has ID {dialog.id} real_id {real_id} type {peer_type}')
    #     i = 0
    #     async for msg in client.iter_messages(real_id):
    #         print(f'{msg.id=}, {msg.message=}, {msg.media=}')
    #         i += 1
    #         if i >= 10:
    #             break
    #     test_res = await client.get_input_entity(dialog.id)
    #     print(test_res)
    # await client.send_message(-1001150067822, "test message from python")
    # nep_channel = await client.get_dialogs("-1001251458407")

    # You can send messages to yourself...
    # await client.send_message('me', 'Hello, myself!')
    # ...to some chat ID
    # await client.send_message(-100123456, 'Hello, group!')
    # ...to your contacts
    # await client.send_message('+34600123123', 'Hello, friend!')
    # ...or even to any username
    # await client.send_message('username', 'Testing Telethon!')

    # You can, of course, use markdown in your messages:
    # message: types.Message = await client.send_message(
    #     'me',
    #     'This message has **bold**, `code`, __italics__ and '
    #     'a [nice website](https://example.com)!',
    #     link_preview=False
    # )

    # Sending a message returns the sent message object, which you can use
    # print(message.raw_text)

    # You can reply to messages directly if you have a message object
    # await message.reply('Cool!')

    # Or send files, songs, documents, albums...
    # await client.send_file('me', './test.py')

    # You can print the message history of any chat:
    # message = await client.get_messages(nep_channel[0])
    # chat = await client.get_input_entity('me')
    # res = []
    # db = UserManager()
    # async for chat in client.iter_dialogs():
    #     async for message in client.iter_messages(chat):
    #         db.insert_by_message(me, message)
    # async for message in client.iter_messages(chat):
    #     db.insert_by_message(me, message)
        # print(message.id, message.text)
        # print(message.stringify())
        # msg_json_str = message.to_json()
        # print(msg_json_str)
        # json.loads(msg_json_str)
        # res.append(json.loads(msg_json)['media']['_'])
        # print(message.to_dict())
        # async def download_task(s: int):
        #     last_p = 0
        #     last_t = time.time()
        #     def progress_callback(p, file_size):
        #         nonlocal last_p, last_t
        #         t = time.time()
        #         bd = p-last_p
        #         td = t-last_t
        #         print(f"{s}:avg:{bd/td/1024:>10.2f}kbps,{p/1024/1024:>7.2f}/{file_size/1024/1024:>7.2f}/{p/file_size:>5.2%}")
        #         last_p = p
        #         last_t = time.time()
        #     await client.download_media(message, progress_callback=progress_callback )
        # t_list = []
        # for i in range(4):
        #     ti = client.loop.create_task(download_task(i))
        #     t_list.append(ti)
        # await asyncio.gather(*t_list)

        # You can download media from messages, too!
        # The method will return the path where the file was saved.
        # if message.photo:
        #     path = await message.download_media()
        #     print('File saved to', path)  # printed after download is done
    # print(res)
# with client:
#     client.loop.run_until_complete(main())
try:
    client1.start()
    # client2.start()
    client1.loop.run_until_complete(main(client1))
    # client2.loop.run_until_complete(main(client2))
finally:
    client1.disconnect()
    # client2.disconnect()


async def start_tg_client(param: configParse.TgToFileSystemParameter):
    api_id = param.tgApi.api_id
    api_hash = param.tgApi.api_hash
    session_name = "test"
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
        # dialogs = await client.get_dialogs()
        # for dialog in dialogs:
        #     print(f"{dialog.name} has ID {dialog.id}")\
        path_task_list = []
        async for dialog in client.iter_dialogs():
            print(dialog.name, 'has ID', dialog.id)
            # path = await client.download_profile_photo(dialog.id)
        #     t = client.loop.create_task(
        #         client.download_profile_photo(dialog.id))
        #     path_task_list.append(t)
        # res = await asyncio.gather(*path_task_list)
        # for path in res:
        #     print(path)

    # async with client:
    #     await tg_client_main()
    await client.connect()
    # qr_login = await client.qr_login()
    await client.start()
    # print(qr_login.url)
    # await qr_login.wait()
    await tg_client_main()
    await client.disconnect()
