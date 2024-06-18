import sys
import os
import traceback

import streamlit as st

import utils
import remote_api as api


@st.experimental_fragment
def loop(sign: str):
    if "page_index" not in st.session_state:
        st.session_state.page_index = 1
    if "force_skip" not in st.session_state:
        st.session_state.force_skip = False
    if "chat_select_list" not in st.session_state:
        st.session_state.chat_select_list = []

    if "search_key" not in st.query_params:
        st.query_params.search_key = ""
    if "is_order" not in st.query_params:
        st.query_params.is_order = False
    if "search_res_limit" not in st.query_params:
        st.query_params.search_res_limit = "10"

    @st.experimental_fragment
    def search_container(keyword, res_limit, isorder):
        if "chat_dict" not in st.session_state:
            wait_client_ready = st.empty()
            wait_client_ready.status("Server Initializing")
            st.session_state.chat_dict = api.get_white_list_chat_dict()
            wait_client_ready.empty()
        st.query_params.search_key = st.text_input("**Search🔎**", value=keyword)
        chat_list = []
        for _, chat_info in st.session_state.chat_dict.items():
            chat_list.append(chat_info["title"])

        columns = st.columns([4, 4, 1])
        with columns[0]:
            st.query_params.search_res_limit = str(
                st.number_input("**Results per page**", min_value=1, max_value=100, value=res_limit, format="%d")
            )
        with columns[1]:
            st.session_state.chat_select_list = st.multiselect("**Search in**", chat_list, default=chat_list)
        with columns[2]:
            st.text("Sort")
            st.query_params.is_order = st.toggle("Time 🔼", value=isorder)

    search_limit_container = st.container()
    with search_limit_container:
        keyword = st.query_params.search_key
        res_limit = int(st.query_params.search_res_limit)
        isorder = utils.strtobool(st.query_params.is_order)
        search_container(keyword, res_limit, isorder)

    search_clicked = st.button("Search", type="primary", use_container_width=True)
    if not st.session_state.force_skip and (
        not search_clicked or st.query_params.search_key == "" or st.query_params.search_key is None
    ):
        return

    if not st.session_state.force_skip:
        st.session_state.page_index = 1
    if st.session_state.force_skip:
        st.session_state.force_skip = False

    @st.experimental_fragment
    def do_search_req():
        search_limit = int(st.query_params.search_res_limit)
        offset_index = (st.session_state.page_index - 1) * search_limit
        is_order = utils.strtobool(st.query_params.is_order)

        status_bar = st.empty()
        status_bar.status("Searching......")
        search_chat_id_list = []
        for chat_id, chat_info in st.session_state.chat_dict.items():
            try:
                if chat_info["title"] in st.session_state.chat_select_list:
                    search_chat_id_list.append(int(chat_id))
            except Exception as err:
                print(f"{err=},{traceback.format_exc()}")
        search_res = api.search_database_by_keyword(
            sign, st.query_params.search_key, search_chat_id_list, offset_index, search_limit, is_order
        )
        status_bar.empty()
        if search_res is None:
            return

        def page_switch_render():
            page_index = st.number_input(
                "Page number:",
                key="page_index_input",
                min_value=1,
                max_value=100,
                value=st.session_state.page_index,
                format="%d",
            )
            if page_index != st.session_state.page_index:
                st.session_state.page_index = page_index
                st.session_state.force_skip = True
                st.rerun()

        def media_file_res_container(
            index: int, msg_ctx: str, file_name: str, file_size: int, url: str, src_link: str, mime_type: str
        ):
            file_size_str = f"{file_size/1024/1024:.2f}MB"
            container = st.container()
            container_columns = container.columns([1, 99])

            st.session_state.search_res_select_list[index] = container_columns[0].checkbox(
                "search_res_checkbox_" + str(index), label_visibility="collapsed"
            )

            expender_title = f"{(msg_ctx if len(msg_ctx) < 103 else msg_ctx[:100] + '...')} &mdash; *{file_size_str}*"
            popover = container_columns[1].popover(expender_title, use_container_width=True)
            # media_file_popover_container(popover, url, msg_ctx, file_name, file_size_str, src_link)
            popover_columns = popover.columns([1, 3, 1])
            video_holder = popover_columns[0].empty()
            if video_holder.button("Preview", key=f"videoBtn{url}{index}", use_container_width=True):
                video_holder.empty()
                p_url = url if url else "./static/404.webm"
                mime_type = mime_type if mime_type else "video/webm"
                video_holder.video(p_url, autoplay=True, format=mime_type)
            popover_columns[1].markdown(f"{msg_ctx}")
            popover_columns[1].markdown(f"**{file_name}**")
            popover_columns[1].markdown(f"File Size: *{file_size_str}*")
            popover_columns[2].link_button("⬇️Download Link", url, use_container_width=True)
            popover_columns[2].link_button("🔗Telegram Link", src_link, use_container_width=True)

        @st.experimental_fragment
        def show_search_res(res: dict[str, any]):
            search_res_list = res.get("list")
            if search_res_list is None or len(search_res_list) == 0:
                st.info("No result")
                page_switch_render()
                return
            sign_token = ""
            try:
                sign_token = res["client"]["sign"]
            except Exception as err:
                pass
            st.session_state.search_res_select_list = [False] * len(search_res_list)
            url_list = []
            for i in range(len(search_res_list)):
                v = search_res_list[i]
                msg_ctx = ""
                file_name = None
                file_size = 0
                download_url = ""
                src_link = ""
                mime_type = ""
                try:
                    src_link = v["src_tg_link"]
                    msg_ctx = v["message"]
                    msg_id = str(v["id"])
                    doc = v["media"]["document"]
                    mime_type = doc["mime_type"]
                    file_size = doc["size"]
                    if doc is not None:
                        for attr in doc["attributes"]:
                            file_name = attr.get("file_name")
                            if file_name != "" and file_name is not None:
                                break
                    if file_name == "" or file_name is None:
                        file_name = "Can not get file name"
                    download_url = v["download_url"]
                    download_url += f"?sign={sign_token}"
                except Exception as err:
                    msg_ctx = f"Not a filelike~\r\n\r\n" + msg_ctx
                    print(f"Not a filelike {err=},{traceback.format_exc()}")
                url_list.append(download_url)
                media_file_res_container(i, msg_ctx, file_name, file_size, download_url, src_link, mime_type)
            page_switch_render()

            show_text = ""
            select_list = st.session_state.search_res_select_list
            for i in range(len(select_list)):
                if select_list[i]:
                    show_text = show_text + url_list[i] + "\n"
            st.text_area("Links", value=show_text)

        show_search_res(search_res)

    do_search_req()
