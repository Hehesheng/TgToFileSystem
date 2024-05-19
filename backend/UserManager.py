import os
import sqlite3

from pydantic import BaseModel
from telethon import types


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

    def insert_by_message(self, me: types.User, msg: types.Message):
        user_id = me.id
        chat_id = msg.chat_id
        msg_id = msg.id
        unique_id = str(user_id) + str(chat_id) + str(msg_id)
        msg_type = "others"
        mime_type = ""
        file_name = ""
        msg_ctx = msg.message
        msg_js = msg.to_json()
        if msg.media is None:
            msg_type = "text"
        elif isinstance(msg.media, types.MessageMediaPhoto):
            msg_type = "photo"
        elif isinstance(msg.media, types.MessageMediaDocument):
            msg_type = "file"
            document = msg.media.document
            mime_type = document.mime_type
            for attr in document.attributes:
                if isinstance(attr, types.DocumentAttributeFilename):
                    file_name = attr.file_name
        insert_data = (unique_id, user_id, chat_id, msg_id,
                       msg_type, msg_ctx, mime_type, file_name, msg_js)
        execute_script = "INSERT INTO message (unique_id, user_id, chat_id, msg_id, msg_type, msg_ctx, mime_type, file_name, msg_js) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        try:
            self.cur.execute(execute_script, insert_data)
            self.con.commit()
        except Exception as err:
            print(f"{err=}")

    def get_user_info() -> None:
        raise NotImplementedError

    def _table_has_been_inited(self) -> bool:
        res = self.cur.execute("SELECT name FROM sqlite_master")
        return len(res.fetchall()) != 0

    def _first_runtime_run_once(self) -> None:
        if len(self.cur.execute("SELECT name FROM sqlite_master WHERE name='user'").fetchall()) == 0:
            self.cur.execute(
                "CREATE TABLE user(client_id primary key, username, phone, tg_user_id, last_login_time)")
        if len(self.cur.execute("SELECT name FROM sqlite_master WHERE name='message'").fetchall()) == 0:
            self.cur.execute(
                "CREATE TABLE message(unique_id varchar(64) primary key, user_id int NOT NULL, chat_id int NOT NULL, msg_id int NOT NULL, msg_type varchar(64), msg_ctx, mime_type, file_name, msg_js)")


if __name__ == "__main__":
    db = UserManager()
    db.cur.execute(
        "UPDATE user SET (client_id, username, phone) = (123, 'hehe', 66666) WHERE client_id == 123")
    res = db.cur.execute("SELECT name FROM sqlite_master")
    print(res.fetchall())
    res = db.cur.execute("SELECT msg_ctx FROM message WHERE true AND msg_ctx like '%Cyan%'")
    print(res.fetchall())
