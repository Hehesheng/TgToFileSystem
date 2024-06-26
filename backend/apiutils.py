import time
import logging

from fastapi import status, HTTPException
from telethon import types, utils
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


def _get_message_media_document_kind_and_names(document: types.MessageMediaDocument) -> tuple[str, str]:
    """Gets kind and possible names for :tl:`DocumentAttribute`."""
    kind = "document"
    possible_names = []
    for attr in document.attributes:
        if isinstance(attr, types.DocumentAttributeFilename):
            possible_names.insert(0, attr.file_name)

        elif isinstance(attr, types.DocumentAttributeAudio):
            kind = "audio"
            if attr.performer and attr.title:
                possible_names.append("{} - {}".format(attr.performer, attr.title))
            elif attr.performer:
                possible_names.append(attr.performer)
            elif attr.title:
                possible_names.append(attr.title)
            elif attr.voice:
                kind = "voice"

    return kind, possible_names


def get_message_media_name(msg: types.Message) -> str:
    if msg.media is None:
        return ""
    match type(msg.media):
        case types.MessageMediaPhoto:
            return f"{msg.media.photo.id}.jpg"
        case types.MessageMediaDocument:
            kind, possible_names = _get_message_media_document_kind_and_names(msg.media.document)
            try:
                name = None if possible_names is None else next(x for x in possible_names if x)
            except StopIteration:
                name = None
            if name:
                return name
            extension = utils.get_extension(msg.media)
            peer_id = utils.get_peer_id(msg.peer_id)
            return f"{kind}_{peer_id}-{msg.id}{extension}"
        case _:
            return ""


def _get_message_media_valid_photo(msg: types.Message) -> types.Photo | None:
    if msg.media is None:
        return None
    photo = msg.media
    if isinstance(photo, types.MessageMediaPhoto):
        photo = photo.photo
    if not isinstance(photo, types.Photo):
        return None
    return photo


def _sort_message_media_photo_thumbs(thumbs: list[any]) -> list[any]:
    def sort_thumbs(thumb):
        if isinstance(thumb, types.PhotoStrippedSize):
            return 1, len(thumb.bytes)
        if isinstance(thumb, types.PhotoCachedSize):
            return 1, len(thumb.bytes)
        if isinstance(thumb, types.PhotoSize):
            return 1, thumb.size
        if isinstance(thumb, types.PhotoSizeProgressive):
            return 1, max(thumb.sizes)
        if isinstance(thumb, types.VideoSize):
            return 2, thumb.size

        # Empty size or invalid should go last
        return 0, 0

    thumbs = list(sorted(thumbs), key=sort_thumbs)
    for i in reversed(range(len(thumbs))):
        if isinstance(thumbs[i], types.PhotoPathSize):
            thumbs.pop(i)

    return thumbs


def _get_message_media_photo_file_last_photo_size(thumbs: list[any]):
    thumbs = _sort_message_media_photo_thumbs(thumbs)

    size = thumbs[-1] if thumbs else None
    if not size or isinstance(size, types.PhotoSizeEmpty):
        return None
    return size


def get_message_media_photo_file_name(msg: types.Message) -> str:
    photo = _get_message_media_valid_photo(msg)
    if not photo:
        return ""

    size = _get_message_media_photo_file_last_photo_size(photo.sizes + (photo.video_sizes or []))
    if not size:
        return ""
    if isinstance(size, types.VideoSize):
        return f"{photo.id}.mp4"
    return f"{photo.id}.jpg"


def get_message_media_photo_file_size(msg: types.Message) -> int:
    photo = _get_message_media_valid_photo(msg)
    if not photo:
        return 0

    size = _get_message_media_photo_file_last_photo_size(photo.sizes + (photo.video_sizes or []))
    if not size:
        return 0

    if isinstance(size, types.PhotoStrippedSize):
        return len(utils.stripped_photo_to_jpg(size.bytes))
    elif isinstance(size, types.PhotoCachedSize):
        return len(size.bytes)

    if isinstance(size, types.PhotoSizeProgressive):
        return max(size.sizes)
    return size.size


def get_message_media_name_from_dict(msg: dict[str, any]) -> str:
    doc = None
    try:
        doc = msg["media"]["document"]
    except:
        pass
    file_name = None
    if doc is not None:
        for attr in doc["attributes"]:
            file_name = attr.get("file_name")
            if file_name != "" and file_name is not None:
                break
    if file_name == "" or file_name is None:
        file_name = "unknown.tmp"
    return file_name


def get_message_chat_id_from_dict(msg: dict[str, any]) -> int:
    try:
        return msg["peer_id"]["channel_id"]
    except:
        pass
    return 0


def get_message_msg_id_from_dict(msg: dict[str, any]) -> int:
    try:
        return msg["id"]
    except:
        pass
    return 0


def timeit_sec(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        logger.debug(f"Function called {func.__name__}{args} {kwargs}")
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        logger.debug(f"Function quited {func.__name__}{args} {kwargs} Took {total_time:.4f} seconds")
        return result

    return timeit_wrapper


def timeit(func):
    if configParse.get_TgToFileSystemParameter().base.timeit_enable:

        @wraps(func)
        def timeit_wrapper(*args, **kwargs):
            logger.debug(f"Function called {func.__name__}{args} {kwargs}")
            start_time = time.perf_counter()
            result = func(*args, **kwargs)
            end_time = time.perf_counter()
            total_time = end_time - start_time
            logger.debug(f"Function quited {func.__name__}{args} {kwargs} Took {total_time:.4f} seconds")
            return result

        return timeit_wrapper
    return func


def atimeit(func):
    if configParse.get_TgToFileSystemParameter().base.timeit_enable:

        @wraps(func)
        async def timeit_wrapper(*args, **kwargs):
            logger.debug(f"AFunction called {func.__name__}{args} {kwargs}")
            start_time = time.perf_counter()
            result = await func(*args, **kwargs)
            end_time = time.perf_counter()
            total_time = end_time - start_time
            logger.debug(f"AFunction quited {func.__name__}{args} {kwargs} Took {total_time:.4f} seconds")
            return result

        return timeit_wrapper
    return func
