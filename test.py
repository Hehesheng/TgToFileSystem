from telethon import TelegramClient

import configParse

param = configParse.get_TgToFileSystemParameter()
# Remember to use your own values from my.telegram.org!
api_id = param.ApiParameter.api_id
api_hash = param.ApiParameter.api_hash
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
    async for dialog in client.iter_dialogs():
        print(dialog.name, 'has ID', dialog.id)

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
    message = await client.get_messages('me', ids=206963)
    async for message in client.iter_messages('me'):
        print(message.id, message.text)

        # You can download media from messages, too!
        # The method will return the path where the file was saved.
        # if message.photo:
        #     path = await message.download_media()
        #     print('File saved to', path)  # printed after download is done

with client:
    client.loop.run_until_complete(main())
