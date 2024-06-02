import os
from enum import Enum, IntEnum, unique, auto
import sqlite3
import logging
import datetime

from pydantic import BaseModel
from telethon import types

logger = logging.getLogger(__file__.split("/")[-1])


class UserUpdateParam(BaseModel):
    client_id: str
    username: str
    phone: str
    tg_user_id: int
    last_login_time: int


class MessageUpdateParam(BaseModel):
    unique_id: str
    user_id: int
    chat_id: int
    msg_id: int
    msg_type: str
    msg_ctx: str
    file_type: str
    file_name: str
    msg_js: str


class UserManager(object):
    def __init__(self) -> None:
        if not os.path.exists(os.path.dirname(__file__) + "/db"):
            os.mkdir(os.path.dirname(__file__) + "/db")
        self.con = sqlite3.connect(f"{os.path.dirname(__file__)}/db/user.db")
        self.cur = self.con.cursor()
        if not self._table_has_been_inited():
            self._first_runtime_run_once()

    def __del__(self) -> None:
        self.con.commit()
        self.con.close()

    def update_user(self) -> None:
        raise NotImplementedError

    def update_message(self) -> None:
        raise NotImplementedError

    def generate_unique_id_by_msg(self, me: types.User, msg: types.Message) -> str:
        user_id = me.id
        chat_id = msg.chat_id
        msg_id = msg.id
        unique_id = str(user_id) + str(chat_id) + str(msg_id)
        return unique_id

    def get_all_msg_by_chat_id(self, chat_id: int) -> list[any]:
        res = self.cur.execute(
            "SELECT * FROM message WHERE chat_id = ? ORDER BY date_time DESC",
            (chat_id,),
        )
        return res.fetchall()

    def get_msg_by_chat_id_and_keyword(
        self,
        chat_id: int,
        keyword: str,
        limit: int = 10,
        offset: int = 0,
        inc: bool = False,
        ignore_case: bool = False,
    ) -> list[any]:
        keyword_condition = "msg_ctx LIKE '%{key}%' OR file_name LIKE '%{key}%'"
        if ignore_case:
            keyword_condition = "LOWER(msg_ctx) LIKE LOWER('%{key}%') OR LOWER(file_name) LIKE LOWER('%{key}%')"
        keyword_condition = keyword_condition.format(key=keyword)
        execute_script = f"SELECT * FROM message WHERE chat_id = {chat_id} AND ({keyword_condition}) ORDER BY date_time {'' if inc else 'DESC '}LIMIT {limit} OFFSET {offset}"
        logger.info(f"{execute_script=}")
        res = self.cur.execute(execute_script)
        return res

    def get_oldest_msg_by_chat_id(self, chat_id: int) -> list[any]:
        res = self.cur.execute(
            "SELECT * FROM message WHERE chat_id = ? ORDER BY date_time LIMIT 1",
            (chat_id,),
        )
        return res.fetchall()

    def get_newest_msg_by_chat_id(self, chat_id: int) -> list[any]:
        res = self.cur.execute(
            "SELECT * FROM message WHERE chat_id = ? ORDER BY date_time DESC LIMIT 1",
            (chat_id,),
        )
        return res.fetchall()

    def get_msg_by_unique_id(self, unique_id: str) -> list[any]:
        res = self.cur.execute(
            "SELECT * FROM message WHERE unique_id = ? ORDER BY date_time DESC LIMIT 1",
            (unique_id,),
        )
        return res.fetchall()

    @unique
    class MessageTypeEnum(Enum):
        OTHERS = "others"
        TEXT = "text"
        PHOTO = "photo"
        FILE = "file"

    @unique
    class ColumnStrEnum(Enum):
        UNIQUE_ID = "unique_id"
        USER_ID = "user_id"
        CHAT_ID = "chat_id"
        MSG_ID = "msg_id"
        MSG_TYPE = "msg_type"
        MSG_CTX = "msg_ctx"
        MIME_TYPE = "mime_type"
        FILE_NAME = "file_name"
        MSG_JS = "msg_js"
        DATE_TIME = "date_time"

    def insert_by_message(self, me: types.User, msg: types.Message):
        user_id = me.id
        chat_id = msg.chat_id
        msg_id = msg.id
        unique_id = str(user_id) + str(chat_id) + str(msg_id)
        msg_type = UserManager.MessageTypeEnum.OTHERS.value
        mime_type = ""
        file_name = ""
        msg_ctx = msg.message
        msg_js = msg.to_json()
        date_time = int(msg.date.timestamp() * 1_000) * 1_000_000
        try:
            if msg.media is None:
                msg_type = UserManager.MessageTypeEnum.TEXT.value
            elif isinstance(msg.media, types.MessageMediaPhoto):
                msg_type = UserManager.MessageTypeEnum.PHOTO.value
            elif isinstance(msg.media, types.MessageMediaDocument):
                document = msg.media.document
                mime_type = document.mime_type
                for attr in document.attributes:
                    if isinstance(attr, types.DocumentAttributeFilename):
                        file_name = attr.file_name
                msg_type = UserManager.MessageTypeEnum.FILE.value
        except Exception as err:
            logger.error(f"{err=}")
        insert_data = (
            unique_id,
            user_id,
            chat_id,
            msg_id,
            msg_type,
            msg_ctx,
            mime_type,
            file_name,
            msg_js,
            date_time,
        )
        execute_script = "INSERT INTO message (unique_id, user_id, chat_id, msg_id, msg_type, msg_ctx, mime_type, file_name, msg_js, date_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        try:
            self.cur.execute(execute_script, insert_data)
            self.con.commit()
        except Exception as err:
            logger.error(f"{err=}")

    @unique
    class ColumnEnum(IntEnum):
        UNIQUE_ID = 0
        USER_ID = auto()
        CHAT_ID = auto()
        MSG_ID = auto()
        MSG_TYPE = auto()
        MSG_CTX = auto()
        MIME_TYPE = auto()
        FILE_NAME = auto()
        MSG_JS = auto()
        DATE_TIME = auto()
        COLUMN_LEN = auto()

    def get_column_by_enum(self, column: tuple[any], index: ColumnEnum) -> any:
        if len(column) == UserManager.ColumnEnum.COLUMN_LEN:
            return column[index]
        return None

    def get_column_msg_id(self, column: tuple[any]) -> int | None:
        if len(column) == UserManager.ColumnEnum.COLUMN_LEN:
            return column[UserManager.ColumnEnum.MSG_ID]
        return None

    def get_column_msg_js(self, column: tuple[any]) -> str | None:
        if len(column) == UserManager.ColumnEnum.COLUMN_LEN:
            return column[UserManager.ColumnEnum.MSG_JS]
        return None

    def get_user_info() -> None:
        raise NotImplementedError

    def _table_has_been_inited(self) -> bool:
        res = self.cur.execute("SELECT name FROM sqlite_master")
        return len(res.fetchall()) != 0

    def _first_runtime_run_once(self) -> None:
        if len(self.cur.execute("SELECT name FROM sqlite_master WHERE name='user'").fetchall()) == 0:
            self.cur.execute("CREATE TABLE user(client_id primary key, username, phone, tg_user_id, last_login_time)")
        if len(self.cur.execute("SELECT name FROM sqlite_master WHERE name='message'").fetchall()) == 0:
            self.cur.execute(
                "CREATE TABLE message(unique_id varchar(64) primary key, user_id int NOT NULL, chat_id int NOT NULL, msg_id int NOT NULL, msg_type varchar(64), msg_ctx text, mime_type text, file_name text, msg_js text, date_time int NOT NULL)"
            )


if __name__ == "__main__":
    db = UserManager()
    # db.cur.execute(
    #     "UPDATE user SET (client_id, username, phone) = (123, 'hehe', 66666) WHERE client_id == 123")
    # res = db.cur.execute("SELECT name FROM sqlite_master")
    # print(res.fetchall())
    # res = db.cur.execute(
    #     "SELECT msg_id, msg_ctx, file_name FROM message WHERE chat_id == -1001216816802")
    # # res.execute("SELECT * FROM message WHERE chat_id == ? ORDER BY msg_id DESC LIMIT 1", (-1001216816802,))
    # # print(res.fetchall())
    # # print("\n\n\n\n\n\n")
    # res.execute("SELECT COUNT(msg_id) FROM message")
    # # res = db.cur.execute("SELECT DISTINCT chat_id FROM message")
    # print(res.fetchall())
    # db.cur.execute("SELECT * FROM")
