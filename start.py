import asyncio
import os
import sys
import yaml
import logging

import uvicorn

import configParse
from backend import backendapp

log_config = None
if not os.path.exists(os.path.dirname(__file__) + "/logs"):
    os.mkdir(os.path.dirname(__file__) + "/logs")
with open("logging_config.yaml", "r") as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger(__file__.split("/")[-1])

for handle in logger.handlers:
    if isinstance(handle, logging.handlers.TimedRotatingFileHandler):
        handle.suffix = "%Y-%m-%d.log"

if __name__ == "__main__":
    param = configParse.get_TgToFileSystemParameter()

    async def run_web_server():
        cmd = f"streamlit run {os.getcwd()}/frontend/home.py --server.port {param.web.port}"
        proc = await asyncio.create_subprocess_shell(
            cmd, cwd=f"{os.path.dirname(__file__)}/frontend", stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )

        async def loop_get_cli_pipe(p, suffix=""):
            while True:
                stdp = await p.readline()
                if stdp:
                    logger.info(f"[web:{suffix}]{stdp.decode()[:-1]}")
                else:
                    break

        stdout_task = asyncio.create_task(loop_get_cli_pipe(proc.stdout, "out"))
        stderr_task = asyncio.create_task(loop_get_cli_pipe(proc.stderr, "err"))
        await asyncio.gather(*[stdout_task, stderr_task])
        logger.info(f"[{cmd!r} exited with {proc.returncode}]")

    if param.web.enable:
        ret = os.fork()
        if ret == 0:
            asyncio.get_event_loop().run_until_complete(run_web_server())
            sys.exit(0)
    uvicorn.run(backendapp, host="0.0.0.0", port=param.base.port, app_dir="backend", log_config=log_config)
