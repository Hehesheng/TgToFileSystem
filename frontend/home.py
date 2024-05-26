import sys
import os
import json

sys.path.append(os.getcwd())
import streamlit as st
import qrcode
import pandas
import requests

import configParse

# qr = qrcode.make("https://www.baidu.com")
# st.image(qrcode.make("https://www.baidu.com").get_image())

st.set_page_config(page_title="TgToolbox", page_icon='🕹️', layout='wide')

if 'page_index' not in st.session_state:
    st.session_state.page_index = 1
if 'search_input' not in st.session_state:
    st.session_state.search_input = ""
if 'last_search_input' not in st.session_state:
    st.session_state.last_search_input = ""
if 'search_clicked' not in st.session_state:
    st.session_state.search_clicked = False
if 'is_order' not in st.session_state:
    st.session_state.is_order = False

param = configParse.get_TgToFileSystemParameter()
background_server_url = f"{param.web.base_url}:{param.base.port}/tg/api/v1/file/search"
download_server_url = f"{param.web.base_url}:{param.base.port}/tg/api/v1/file/msg?token={param.web.token}&cid={param.web.chat_id[0]}&mid="


@st.experimental_fragment
def search_input_container():
    st.session_state.search_input = st.text_input("搜索🔎", value=st.query_params.get(
        'search') if st.query_params.get('search') is not None else "")


search_input_container()

col1, col2 = st.columns(2)
search_res_limit = st.number_input(
    "每页结果", min_value=1, max_value=100, value=10, format="%d")
columns = st.columns([7, 1])
with columns[0]:
    if st.button("Search"):
        st.session_state.page_index = 1
        st.session_state.search_clicked = True
with columns[1]:
    st.session_state.is_order = st.checkbox("顺序")

if st.session_state.search_input == "" or (st.session_state.search_input != st.session_state.last_search_input and not st.session_state.search_clicked):
    st.session_state.search_clicked = False
    st.stop()

st.session_state.last_search_input = st.session_state.search_input
st.query_params.search = st.session_state.search_input


@st.experimental_fragment
def do_search_req():
    offset_index = (st.session_state.page_index - 1) * search_res_limit
    is_order = st.session_state.is_order

    req_body = {
        "token": param.web.token,
        "search": f"{st.session_state.search_input}",
        "chat_id": param.web.chat_id[0],
        "index": offset_index,
        "length": search_res_limit,
        "refresh": False,
        "inner": False,
        "inc": is_order,
    }

    req = requests.post(background_server_url, data=json.dumps(req_body))
    if req.status_code != 200:
        st.stop()
    search_res = json.loads(req.content.decode("utf-8"))

    def page_switch_render():
        columns = st.columns(3)
        with columns[0]:
            pre_button = st.button("Prev", use_container_width=True)
            if pre_button:
                st.session_state.page_index = st.session_state.page_index - 1
                st.session_state.page_index = max(
                    st.session_state.page_index, 1)
                st.rerun()
        with columns[1]:
            # st.text(f"{st.session_state.page_index}")
            st.markdown(
                f"<p style='text-align: center;'>{st.session_state.page_index}</p>", unsafe_allow_html=True)
            # st.markdown(f"<input type='number' style='text-align: center;' value={st.session_state.page_index}>", unsafe_allow_html=True)
        with columns[2]:
            next_button = st.button("Next", use_container_width=True)
            if next_button:
                st.session_state.page_index = st.session_state.page_index + 1
                st.rerun()

    def media_file_res_container(index: int, msg_ctx: str, file_name: str, file_size: str, url: str):
        container = st.container()
        container_columns = container.columns([1, 99])

        st.session_state.search_res_select_list[index] = container_columns[0].checkbox(
            url, label_visibility='collapsed')

        expender_title = f"{(msg_ctx if len(msg_ctx) < 83 else msg_ctx[:80] + '...')} &mdash; *{file_size}*"
        popover = container_columns[1].popover(expender_title)
        popover_columns = popover.columns([1, 1])
        popover_columns[0].video(url)
        popover_columns[1].markdown(f'{msg_ctx}')
        popover_columns[1].markdown(f'**{file_name}**')
        popover_columns[1].markdown(f'文件大小：*{file_size}*')
        popover_columns[1].page_link(url, label='Download Link', icon='⬇️')

    @st.experimental_fragment
    def show_search_res():
        search_res_list = search_res['list']
        if len(search_res_list) == 0:
            st.info("No result")
            page_switch_render()
            st.stop()
        st.session_state.search_res_select_list = [False] * len(search_res_list)
        url_list = []
        for i in range(len(search_res_list)):
            v = search_res_list[i]
            msg_ctx = v['message']
            doc = None
            file_size = 0
            msg_id = str(v['id'])
            download_url = download_server_url + msg_id
            url_list.append(download_url)
            try:
                doc = v['media']['document']
                file_size = doc['size']
            except:
                pass
            file_size_str = f"{file_size/1024/1024:.2f}MB"
            file_name = None
            if doc is not None:
                for attr in doc['attributes']:
                    file_name = attr.get('file_name')
                    if file_name != "" and file_name is not None:
                        break
            if file_name == "" or file_name is None:
                file_name = "Can not get file name"
            media_file_res_container(
                i, msg_ctx, file_name, file_size_str, download_url)
        page_switch_render()

        show_text = ""
        select_list = st.session_state.search_res_select_list
        for i in range(len(select_list)):
            if select_list[i]:
                show_text = show_text + url_list[i] + '\n'
        st.text_area("链接", value=show_text)

    show_search_res()


do_search_req()
