import streamlit as st

import remote_api as api


def loop():
    input_link = st.text_input("Telegram share link:", placeholder="https://t.me/c/xxx/xxx/ or https://t.me/xxx/xxx")
    button_clicked = st.button("Convert", type="primary", use_container_width=True)
    res = ""
    if button_clicked and input_link != "":
        res = api.convert_tg_link_to_proxy_link(input_link)
    st.text_area("Convert res text area", value=res, label_visibility="hidden")
