import sqlite3

from pydantic import BaseModel


class UserUpdateParam(BaseModel):
    client_id: str
    username: str
    phone: str
    tg_user_id: int
    last_login_time: int


class MessageUpdateParam(BaseModel):
    tg_chat_id: int
    tg_message_id: int
    client_id: str
    username: str
    phone: str
    tg_user_id: int


class UserManager(object):
    def __init__(self) -> None:
        self.con = sqlite3.connect("user.db")
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

    def get_user_info() -> None:
        raise NotImplementedError

    def _table_has_been_inited(self) -> bool:
        res = self.cur.execute("SELECT name FROM sqlite_master")
        return len(res.fetchall()) != 0

    def _first_runtime_run_once(self) -> None:
        if len(self.cur.execute("SELECT name FROM sqlite_master WHERE name='user'").fetchall()) == 0:
            self.cur.execute(
                "CREATE TABLE user(client_id, username, phone, tg_user_id, last_login_time)")
        if len(self.cur.execute("SELECT name FROM sqlite_master WHERE name='message'").fetchall()) == 0:
            self.cur.execute(
                "CREATE TABLE message(tg_chat_id, tg_message_id, client_id, username, phone, tg_user_id, msg_ctx, msg_type)")


if __name__ == "__main__":
    db = UserManager()
    res = db.cur.execute("SELECT name FROM sqlite_master")
    print(res.fetchall())
