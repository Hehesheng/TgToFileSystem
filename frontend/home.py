import time

import streamlit as st

import remote_api as api

st.set_page_config(page_title="TgToolbox", page_icon="🕹️", layout="wide", initial_sidebar_state="collapsed")

backend_status = api.get_backend_client_status()
need_login = False
sign = ""

if backend_status is None or not backend_status["init"]:
    st.status("Server not ready")
    time.sleep(0.5)
    st.rerun()

for v in backend_status["clients"]:
    if v["name"] != api.get_config_default_name():
        continue
    need_login = not v["status"]
    sign = v["sign"]

if need_login:
    import login

    login.loop()
    st.stop()

search_tab, link_convert_tab = st.tabs(["Search", "Link Convert"])
with search_tab:
    import search

    search.loop(sign)
with link_convert_tab:
    import link_convert

    link_convert.loop()
