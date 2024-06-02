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
else:
    import search
