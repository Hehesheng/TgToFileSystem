import asyncio
import os
import sys
import yaml
import logging

import uvicorn

import configParse
from backend import backendapp

if not os.path.exists(os.path.dirname(__file__) + '/logs'):
    os.mkdir(os.path.dirname(__file__) + '/logs')
with open('logging_config.yaml', 'r') as f:
    logging.config.dictConfig(yaml.safe_load(f.read()))
for handler in logging.getLogger().handlers:
    if isinstance(handler, logging.handlers.TimedRotatingFileHandler):
        handler.suffix = "%Y-%m-%d"

logger = logging.getLogger(__file__.split("/")[-1])

if __name__ == "__main__":
    param = configParse.get_TgToFileSystemParameter()
    async def run_web_server():
        cmd = f"streamlit run {os.getcwd()}/frontend/home.py --server.port {param.web.port}"
        proc = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE,
                                              stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await proc.communicate()
        logger.info(f'[{cmd!r} exited with {proc.returncode}]')
        if stdout:
            logger.info(f'[stdout]\n{stdout.decode()}')
        if stderr:
            logger.info(f'[stderr]\n{stderr.decode()}')
    if param.web.enable:
        ret = os.fork()
        if ret == 0:
            asyncio.get_event_loop().run_until_complete(run_web_server())
            sys.exit(0)
    uvicorn.run(backendapp, host="0.0.0.0", port=param.base.port)
