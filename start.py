import asyncio
import os
import sys
import yaml
import logging

import uvicorn
from uvicorn.config import LOGGING_CONFIG

import configParse
from backend import backendapp

if not os.path.exists(os.path.dirname(__file__) + '/logs'):
    os.mkdir(os.path.dirname(__file__) + '/logs')
with open('logging_config.yaml', 'r') as f:
    logging.config.dictConfig(yaml.safe_load(f.read()))

LOGGING_CONFIG["formatters"]["default"]["fmt"] = "[%(levelname)s] %(asctime)s [uvicorn.default]:%(message)s"
LOGGING_CONFIG["formatters"]["access"]["fmt"] = '[%(levelname)s]%(asctime)s [uvicorn.access]:%(client_addr)s - "%(request_line)s" %(status_code)s'
LOGGING_CONFIG["handlers"]["timed_rotating_api_file"] = {
    "class": "logging.handlers.TimedRotatingFileHandler",
    "filename": "logs/app.log",
    "when": "midnight",
    "interval": 1,
    "backupCount": 7,
    "level": "INFO",
    "formatter": "default",
    "encoding": "utf-8",
}
LOGGING_CONFIG["loggers"]["uvicorn"]["handlers"].append("timed_rotating_api_file")
LOGGING_CONFIG["loggers"]["uvicorn.access"]["handlers"].append("timed_rotating_api_file")

for handler in logging.getLogger().handlers:
    if isinstance(handler, logging.handlers.TimedRotatingFileHandler):
        handler.suffix = "%Y-%m-%d"

logger = logging.getLogger(__file__.split("/")[-1])

if __name__ == "__main__":
    param = configParse.get_TgToFileSystemParameter()
    async def run_web_server():
        cmd = f"streamlit run {os.getcwd()}/frontend/home.py --server.port {param.web.port}"
        proc = await asyncio.create_subprocess_shell(cmd, cwd=f"{os.path.dirname(__file__)}/frontend", stdout=asyncio.subprocess.PIPE,
                                              stderr=asyncio.subprocess.PIPE)
        async def loop_get_cli_pipe(p, suffix = ""):
            while True:
                stdp = await p.readline()
                if stdp:
                    logger.info(f"[web:{suffix}]{stdp.decode()[:-1]}")
                else:
                    break
        stdout_task = asyncio.create_task(loop_get_cli_pipe(proc.stdout, "out"))
        stderr_task = asyncio.create_task(loop_get_cli_pipe(proc.stderr, "err"))
        await asyncio.gather(*[stdout_task, stderr_task])
        logger.info(f'[{cmd!r} exited with {proc.returncode}]')
    if param.web.enable:
        ret = os.fork()
        if ret == 0:
            asyncio.get_event_loop().run_until_complete(run_web_server())
            sys.exit(0)
    uvicorn.run(backendapp, host="0.0.0.0", port=param.base.port)
