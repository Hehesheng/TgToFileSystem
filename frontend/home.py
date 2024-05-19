import sys
import os
import json

import streamlit
import qrcode
import pandas
import requests

sys.path.append(os.getcwd())

import configParse

# qr = qrcode.make("https://www.baidu.com")
# streamlit.image(qrcode.make("https://www.baidu.com").get_image())

param = configParse.get_TgToFileSystemParameter()
background_server_url = f"{param.web.base_url}:{param.base.port}/tg/api/v1/file/list"
download_server_url = f"{param.web.base_url}:{param.base.port}/tg/api/v1/file/msg?token={param.web.token}&cid={param.web.chat_id[0]}&mid="


search_input = streamlit.text_input("输入想搜的:")
col1, col2 = streamlit.columns(2)
search_clicked = False
search_res_limit = streamlit.number_input("限制搜索量", min_value=1, max_value=100, value=10, format="%d")
search_clicked = streamlit.button("Search")
if not search_clicked or search_input == "":
    streamlit.stop()

test_body = {
    "token": param.web.token,
    "search": f"{search_input}",
    "chat_id": param.web.chat_id[0],
    "index": 0,
    "length": search_res_limit,
    "refresh": False,
    "inner": False,
}

req = requests.post(background_server_url, data=json.dumps(test_body))
if req.status_code != 200:
    streamlit.stop()
search_res = json.loads(req.content.decode("utf-8"))


message_list = []
file_name_list = []
file_size_list = []
download_url_list = []
message_id_list = []
for v in search_res['list']:
    message_list.append(v['message'])
    doc = v['media']['document']
    file_size = doc['size'] or 0
    file_size_list.append(f"{file_size/1024/1024:.2f}MB")
    file_name = ""
    for attr in doc['attributes']:
        file_name = attr.get('file_name')
        if file_name is not None:
            file_name_list.append(file_name)
            break
    if file_name == "":
        file_name_list.append("Not A File")
    msg_id = str(v['id'])
    message_id_list.append(msg_id)
    download_url_list.append(download_server_url+msg_id)

df = pandas.DataFrame(
    {
        "message": message_list,
        "file name": file_name_list,
        "file size": file_size_list,
        "url": download_url_list,
        "id": message_id_list,
    }
)

streamlit.dataframe(
    df,
    column_config={
        "url": streamlit.column_config.LinkColumn("URL"),
    },
    hide_index=True,
)
