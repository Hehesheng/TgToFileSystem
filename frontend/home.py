import sys
import os
import json

sys.path.append(os.getcwd())

import streamlit
import qrcode
import pandas
import requests

import configParse

# qr = qrcode.make("https://www.baidu.com")
# streamlit.image(qrcode.make("https://www.baidu.com").get_image())

if streamlit.session_state.get('page_index') is None:
    streamlit.session_state.page_index = 0
if streamlit.session_state.get('search_key') is None:
    streamlit.session_state.search_key = ""

param = configParse.get_TgToFileSystemParameter()
background_server_url = f"{param.web.base_url}:{param.base.port}/tg/api/v1/file/search"
download_server_url = f"{param.web.base_url}:{param.base.port}/tg/api/v1/file/msg?token={param.web.token}&cid={param.web.chat_id[0]}&mid="


search_input = streamlit.text_input("搜索关键字:")
col1, col2 = streamlit.columns(2)
search_res_limit = streamlit.number_input(
    "搜索结果数", min_value=1, max_value=100, value=10, format="%d")
search_clicked = streamlit.button("Search")
if (not search_clicked or search_input == "") and search_input != streamlit.session_state.search_input:
    streamlit.session_state.page_index = 0
    streamlit.stop()
streamlit.session_state.search_input = search_input

@streamlit.experimental_fragment
def show_search_res():
    offset_index = streamlit.session_state.page_index * search_res_limit

    req_body = {
        "token": param.web.token,
        "search": f"{search_input}",
        "chat_id": param.web.chat_id[0],
        "index": offset_index,
        "length": search_res_limit,
        "refresh": False,
        "inner": False,
    }

    req = requests.post(background_server_url, data=json.dumps(req_body))
    if req.status_code != 200:
        streamlit.stop()
    search_res = json.loads(req.content.decode("utf-8"))


    message_list = []
    file_name_list = []
    file_size_list = []
    download_url_list = []
    message_id_list = []
    select_box_list = []
    for v in search_res['list']:
        message_list.append(v['message'])
        doc = None
        file_size = 0
        try:
            doc = v['media']['document']
            file_size = doc['size']
        except:
            pass
        file_size_list.append(f"{file_size/1024/1024:.2f}MB")
        file_name = None
        for attr in doc['attributes']:
            file_name = attr.get('file_name')
            if file_name is not None:
                file_name_list.append(file_name)
                break
        if file_name is None:
            file_name_list.append("Not A File")
        msg_id = str(v['id'])
        message_id_list.append(msg_id)
        download_url_list.append(download_server_url+msg_id)
        select_box_list.append(False)

    df = pandas.DataFrame(
        {
            "select_box": select_box_list,
            "message": message_list,
            "file name": file_name_list,
            "file size": file_size_list,
            "url": download_url_list,
            "id": message_id_list,
        }
    )

    # streamlit.text_area("debug", value=f'{df}')
    if df.empty:
        streamlit.info("No result")
        streamlit.stop()
    data = streamlit.data_editor(
        df,
        column_config={
            "select_box": streamlit.column_config.CheckboxColumn("✅", default=False),
            "url": streamlit.column_config.LinkColumn("URL"),
        },
        disabled=["message",
                "file name",
                "file size",
                "url",
                "id",],
        hide_index=True,
    )
    columns = streamlit.columns(3)
    with columns[0]:
        pre_button = streamlit.button("Prev", use_container_width=True)
        if pre_button:
            streamlit.session_state.page_index = max(streamlit.session_state.page_index - 1, 0)
            streamlit.rerun()
    with columns[1]:
        # streamlit.text(f"{streamlit.session_state.page_index + 1}")
        streamlit.markdown(f"<p style='text-align: center;'>{streamlit.session_state.page_index + 1}</p>", unsafe_allow_html=True)
        # streamlit.markdown(f"<input type='number' style='text-align: center;' value={streamlit.session_state.page_index + 1}>", unsafe_allow_html=True)
    with columns[2]:
        next_button = streamlit.button("Next", use_container_width=True)
        if next_button:
            streamlit.session_state.page_index = streamlit.session_state.page_index + 1
            streamlit.rerun()

    show_text = ""
    select_list = data['select_box']
    url_list = data['url']
    for i in range(len(select_list)):
        if select_list[i]:
            show_text = show_text + url_list[i] + '\n'
    if show_text != "":
        streamlit.text_area("链接", value=show_text)

show_search_res()

