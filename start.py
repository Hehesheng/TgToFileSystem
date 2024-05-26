import asyncio
import os
import sys

import uvicorn

import configParse
from backend import backendapp

if __name__ == "__main__":
    param = configParse.get_TgToFileSystemParameter()
    async def run_web_server():
        cmd = f"streamlit run {os.getcwd()}/frontend/home.py --server.port {param.web.port}"
        proc = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE,
                                              stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await proc.communicate()
        print(f'[{cmd!r} exited with {proc.returncode}]')
        if stdout:
            print(f'[stdout]\n{stdout.decode()}')
        if stderr:
            print(f'[stderr]\n{stderr.decode()}')
    if param.web.enable:
        ret = os.fork()
        if ret == 0:
            asyncio.get_event_loop().run_until_complete(run_web_server())
            sys.exit(0)
    uvicorn.run(backendapp, host="0.0.0.0", port=param.base.port)
