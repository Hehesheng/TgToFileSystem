import streamlit as st

import remote_api as api

st.set_page_config(page_title="TgToolbox", page_icon="🕹️", layout="wide", initial_sidebar_state="collapsed")

backend_status = api.get_backend_client_status()
need_login = False

for v in backend_status["clients"]:
    if not v["status"]:
        need_login = True

if need_login:
    import login
    login.loop()
    st.stop()

search_tab, link_convert_tab = st.tabs(["Search", "Link Convert"])
with search_tab:
    import search
    search.loop()
with link_convert_tab:
    st.header("施工中<(￣︶￣)↗[GO!]")
    st.markdown("# 404 Not Found")
