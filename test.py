from telethon import TelegramClient

import configParse

param = configParse.get_TgToFileSystemParameter()
# Remember to use your own values from my.telegram.org!
api_id = param.tgApi.api_id
api_hash = param.tgApi.api_hash
client = TelegramClient('anon', api_id, api_hash, proxy={
    'proxy_type': 'socks5',
    'addr': '172.25.32.1',
    'port': 7890,
})
# client = TelegramClient('anon', api_id, api_hash, proxy=("socks5", '127.0.0.1', 7890))
# proxy=("socks5", '127.0.0.1', 4444)

async def main():
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
    # async for dialog in client.iter_dialogs():
    #     print(dialog.name, 'has ID', dialog.id)
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
    # message = await client.send_message(
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
    chat = await client.get_input_entity(-1001216816802)
    async for message in client.iter_messages(chat, ids=98724):
        print(message.id, message.text)
        # print(message.stringify())
        # print(message.to_json())
        # print(message.to_dict())
        # await client.download_media(message)

        # You can download media from messages, too!
        # The method will return the path where the file was saved.
        # if message.photo:
        #     path = await message.download_media()
        #     print('File saved to', path)  # printed after download is done

with client:
    client.loop.run_until_complete(main())


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
