import sys
import os

import streamlit as st

import utils
import remote_api as api

@st.experimental_fragment
def loop():
    if 'page_index' not in st.session_state:
        st.session_state.page_index = 1
    if 'force_skip' not in st.session_state:
        st.session_state.force_skip = False

    if 'search_key' not in st.query_params:
        st.query_params.search_key = ""
    if 'is_order' not in st.query_params:
        st.query_params.is_order = False
    if 'search_res_limit' not in st.query_params:
        st.query_params.search_res_limit = "10"

    @st.experimental_fragment
    def search_container():
        st.query_params.search_key = st.text_input("**搜索🔎**", value=st.query_params.search_key)
        columns = st.columns([7, 1])
        with columns[0]:
            st.query_params.search_res_limit = str(st.number_input(
                "**每页结果**", min_value=1, max_value=100, value=int(st.query_params.search_res_limit), format="%d"))
        with columns[1]:
            st.text("排序")
            st.query_params.is_order = st.toggle("顺序", value=utils.strtobool(st.query_params.is_order))

    search_container()

    search_clicked = st.button('Search', type='primary', use_container_width=True)
    if not st.session_state.force_skip and (not search_clicked or st.query_params.search_key == "" or st.query_params.search_key is None):
        st.stop()

    if not st.session_state.force_skip:
        st.session_state.page_index = 1
    if st.session_state.force_skip:
        st.session_state.force_skip = False

    @st.experimental_fragment
    def do_search_req():
        search_limit = int(st.query_params.search_res_limit)
        offset_index = (st.session_state.page_index - 1) * search_limit
        is_order = utils.strtobool(st.query_params.is_order)

        search_res = api.search_database_by_keyword(st.query_params.search_key, offset_index, search_limit, is_order)
        if search_res is None:
            st.stop()

        def page_switch_render():
            columns = st.columns(3)
            with columns[0]:
                if st.button("Prev", use_container_width=True):
                    st.session_state.page_index = st.session_state.page_index - 1
                    st.session_state.page_index = max(
                        st.session_state.page_index, 1)
                    st.session_state.force_skip = True
                    st.rerun()
            with columns[1]:
                # st.text(f"{st.session_state.page_index}")
                st.markdown(
                    f"<p style='text-align: center;'>{st.session_state.page_index}</p>", unsafe_allow_html=True)
                # st.markdown(f"<input type='number' style='text-align: center;' value={st.session_state.page_index}>", unsafe_allow_html=True)
            with columns[2]:
                if st.button("Next", use_container_width=True):
                    st.session_state.page_index = st.session_state.page_index + 1
                    st.session_state.force_skip = True
                    st.rerun()

        def media_file_res_container(index: int, msg_ctx: str, file_name: str, file_size: int, url: str, src_link: str):
            file_size_str = f"{file_size/1024/1024:.2f}MB"
            container = st.container()
            container_columns = container.columns([1, 99])

            st.session_state.search_res_select_list[index] = container_columns[0].checkbox(
                "search_res_checkbox_" + str(index), label_visibility='collapsed')

            expender_title = f"{(msg_ctx if len(msg_ctx) < 103 else msg_ctx[:100] + '...')} &mdash; *{file_size_str}*"
            popover = container_columns[1].popover(expender_title, use_container_width=True)
            popover_columns = popover.columns([1, 3, 1])
            if url:
                popover_columns[0].video(url)
            else:
                popover_columns[0].video('./static/404.webm', format="video/webm")
            popover_columns[1].markdown(f'{msg_ctx}')
            popover_columns[1].markdown(f'**{file_name}**')
            popover_columns[1].markdown(f'文件大小：*{file_size_str}*')
            popover_columns[2].link_button('⬇️Download Link', url, use_container_width=True)
            popover_columns[2].link_button('🔗Telegram Link', src_link, use_container_width=True)

        @st.experimental_fragment
        def show_search_res(res: dict[str, any]):
            search_res_list = res.get("list")
            if search_res_list is None or len(search_res_list) == 0:
                st.info("No result")
                page_switch_render()
                st.stop()
            sign_token = ""
            try:
                sign_token = res['client']['sign']
            except Exception as err:
                pass
            st.session_state.search_res_select_list = [False] * len(search_res_list)
            url_list = []
            for i in range(len(search_res_list)):
                v = search_res_list[i]
                msg_ctx= ""
                file_name = None
                file_size = 0
                download_url = ""
                src_link = ""
                try:
                    src_link = v['src_tg_link']
                    msg_ctx = v['message']
                    msg_id = str(v['id'])
                    doc = v['media']['document']
                    file_size = doc['size']
                    if doc is not None:
                        for attr in doc['attributes']:
                            file_name = attr.get('file_name')
                            if file_name != "" and file_name is not None:
                                break
                    if file_name == "" or file_name is None:
                        file_name = "Can not get file name"
                    download_url = v['download_url']
                    download_url += f'?sign={sign_token}'
                    url_list.append(download_url)
                except Exception as err:
                    msg_ctx = f"{err=}\r\n\r\n" + msg_ctx
                media_file_res_container(
                    i, msg_ctx, file_name, file_size, download_url, src_link)
            page_switch_render()

            show_text = ""
            select_list = st.session_state.search_res_select_list
            for i in range(len(select_list)):
                if select_list[i]:
                    show_text = show_text + url_list[i] + '\n'
            st.text_area("链接", value=show_text)

        show_search_res(search_res)


    do_search_req()
