import time
import logging

from fastapi import status, HTTPException
from telethon import types
from functools import wraps

import configParse

logger = logging.getLogger(__file__.split("/")[-1])

def get_range_header(range_header: str, file_size: int) -> tuple[int, int]:
    def _invalid_range():
        return HTTPException(
            status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE,
            detail=f"Invalid request range (Range:{range_header!r})",
        )

    try:
        h = range_header.replace("bytes=", "").split("-")
        start = int(h[0]) if h[0] != "" else 0
        end = int(h[1]) if h[1] != "" else file_size - 1
    except ValueError:
        raise _invalid_range()

    if start > end or start < 0 or end > file_size - 1:
        raise _invalid_range()
    return start, end


def get_message_media_name(msg: types.Message) -> str:
    if msg.media is None or msg.media.document is None:
        return ""
    for attr in msg.media.document.attributes:
        if isinstance(attr, types.DocumentAttributeFilename):
            return attr.file_name
    return ""

def get_message_media_name_from_dict(msg: dict[str, any]) -> str:
    doc = None
    try:
        doc = msg['media']['document']
    except:
        pass
    file_name = None
    if doc is not None:
        for attr in doc['attributes']:
            file_name = attr.get('file_name')
            if file_name != "" and file_name is not None:
                break
    if file_name == "" or file_name is None:
        file_name = "unknown.tmp"
    return file_name

def timeit_sec(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        logger.debug(
            f'Function called {func.__name__}{args} {kwargs}')
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        logger.debug(
            f'Function quited {func.__name__}{args} {kwargs} Took {total_time:.4f} seconds')
        return result
    return timeit_wrapper

def timeit(func):
    if configParse.get_TgToFileSystemParameter().base.timeit_enable:
        @wraps(func)
        def timeit_wrapper(*args, **kwargs):
            logger.debug(
                f'Function called {func.__name__}{args} {kwargs}')
            start_time = time.perf_counter()
            result = func(*args, **kwargs)
            end_time = time.perf_counter()
            total_time = end_time - start_time
            logger.debug(
                f'Function quited {func.__name__}{args} {kwargs} Took {total_time:.4f} seconds')
            return result
        return timeit_wrapper
    return func


def atimeit(func):
    if configParse.get_TgToFileSystemParameter().base.timeit_enable:
        @wraps(func)
        async def timeit_wrapper(*args, **kwargs):
            logger.debug(
                f'AFunction called {func.__name__}{args} {kwargs}')
            start_time = time.perf_counter()
            result = await func(*args, **kwargs)
            end_time = time.perf_counter()
            total_time = end_time - start_time
            logger.debug(
                f'AFunction quited {func.__name__}{args} {kwargs} Took {total_time:.4f} seconds')
            return result
        return timeit_wrapper
    return func