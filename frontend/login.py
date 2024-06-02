import sys
import os

import streamlit as st
import qrcode

sys.path.append(os.getcwd() + "/../")
import configParse
import utils
import remote_api as api

url = api.login_client_by_qr_code_url()

if url is None or url == "":
    st.text("Something wrong, no login url got.")
    st.stop()

st.markdown("### Please scan the qr code by telegram client.")
qr = qrcode.make(url)
st.image(qr.get_image())

st.markdown("**Click the Refrash button if you have been scaned**")
if st.button("Refresh"):
    st.rerun()
