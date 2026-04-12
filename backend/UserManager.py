import os
from enum import Enum, IntEnum, unique, auto
import sqlite3
import logging
import datetime
import traceback

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
    """UserManager with FTS5 full-text search support using trigram tokenizer."""

    def __init__(self) -> None:
        db_dir = os.path.dirname(__file__) + "/db"
        if not os.path.exists(db_dir):
            os.mkdir(db_dir)
        self.con = sqlite3.connect(f"{db_dir}/user.db")
        self.cur = self.con.cursor()
        if not self._table_has_been_inited():
            self._first_runtime_run_once()
        # Ensure FTS table exists and is synced
        self._ensure_fts_table()

    def __del__(self) -> None:
        self.con.commit()
        self.con.close()

    def update_user(self) -> None:
        raise NotImplementedError

    def update_message(self) -> None:
        raise NotImplementedError

    @staticmethod
    def generate_unique_id_by_msg(me: types.User, msg: types.Message) -> str:
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
        chat_ids: list[int],
        keyword: str,
        limit: int = 10,
        offset: int = 0,
        inc: bool = False,
        ignore_case: bool = False,  # FTS5 ignores case by default
    ) -> list[any]:
        """
        Search messages using FTS5 MATCH query.

        Args:
            chat_ids: List of chat IDs to filter
            keyword: Search keyword (supports AND, OR, NOT operators)
            limit: Maximum results
            offset: Offset for pagination
            inc: True for ascending order, False for descending (by relevance)
            ignore_case: Ignored (FTS5 is case-insensitive by default)

        Returns:
            List of matching messages
        """
        if not chat_ids:
            logger.warning("chat_ids is empty.")
            return []

        if not keyword or keyword.strip() == "":
            # No keyword, return by date
            chat_placeholders = ",".join(["?"] * len(chat_ids))
            order_direction = "" if inc else "DESC"
            execute_script = f"""
                SELECT * FROM message
                WHERE chat_id IN ({chat_placeholders})
                ORDER BY date_time {order_direction}
                LIMIT ? OFFSET ?
            """
            params = tuple(chat_ids) + (limit, offset)
            logger.info(f"SQL: {execute_script}")
            return self.cur.execute(execute_script, params)

        chat_placeholders = ",".join(["?"] * len(chat_ids))

        import re
        keyword_no_space = re.sub(r'\s+', '', keyword)

        # Get trigrams for scoring (or single keyword for short terms)
        if len(keyword_no_space) >= 3:
            search_trigrams = self._split_into_trigrams(keyword_no_space)
            fts_query = self._sanitize_fts_query(keyword)
            # Use FTS5 for initial filtering
            execute_script = f"""
                SELECT m.* FROM message m
                JOIN message_fts f ON m.unique_id = f.unique_id
                WHERE m.chat_id IN ({chat_placeholders})
                AND message_fts MATCH ?
            """
            params = tuple(chat_ids) + (fts_query,)
            logger.info(f"FTS query: {fts_query}")
        else:
            # Short keyword: scan all messages (no FTS, use LIKE logic)
            search_trigrams = [keyword_no_space]
            execute_script = f"""
                SELECT * FROM message
                WHERE chat_id IN ({chat_placeholders})
                AND (msg_ctx LIKE ? OR file_name LIKE ?)
            """
            params = tuple(chat_ids) + (f"%{keyword_no_space}%", f"%{keyword_no_space}%")
            logger.info(f"LIKE scan for short keyword: {keyword_no_space}")

        # Fetch results for scoring
        raw_results = self.cur.execute(execute_script, params).fetchall()

        # Score and sort results by trigram match count
        scored_results = []
        for row in raw_results:
            msg_ctx = row[5] or ""
            file_name = row[7] or ""
            combined_text = re.sub(r'\s+', '', msg_ctx + file_name)

            # Calculate match score: count how many trigrams match
            match_count = sum(1 for tg in search_trigrams if tg in combined_text)
            if match_count == 0:
                continue  # Skip non-matching results (from OR query noise)

            date_time = row[9]
            scored_results.append((match_count, date_time, row))

        # Sort: higher match_count first, then by date (inc/desc)
        if inc:
            scored_results.sort(key=lambda x: (-x[0], x[1]))
        else:
            scored_results.sort(key=lambda x: (-x[0], -x[1]))

        # Apply offset and limit
        final_results = [r[2] for r in scored_results[offset:offset + limit]]
        return final_results

    def _sanitize_fts_query(self, keyword: str) -> str:
        """
        Sanitize and format keyword for FTS5 MATCH query.

        For Chinese text, removes spaces and splits into trigram segments
        to handle cases like "金牌得主第二季" matching "金牌得主 第二季".
        """
        import re

        keyword = keyword.strip()
        keyword = keyword.replace('"', '""')

        # Check if contains Chinese characters
        has_chinese = bool(re.search(r'[\u4e00-\u9fff]', keyword))

        if not has_chinese:
            # Non-Chinese: simple phrase match
            return f'"{keyword}"'

        # Chinese text: remove spaces and split into trigram segments
        # This allows "金牌得主第二季" to match "金牌得主 第二季"
        # because spaces are removed before matching
        keyword_no_space = re.sub(r'\s+', '', keyword)

        if len(keyword_no_space) >= 3:
            # Split into overlapping trigrams (3-char segments)
            # Use OR for flexible matching - any trigram match is sufficient
            segments = self._split_into_trigrams(keyword_no_space)
            return ' OR '.join(f'"{seg}"' for seg in segments)
        else:
            # Short Chinese: use LIKE fallback instead (handled upstream)
            return f'"{keyword_no_space}"'

    def _split_into_trigrams(self, text: str) -> list[str]:
        """
        Split text into overlapping 3-character segments (trigrams).

        Example: "金牌得主第二季" -> ["金牌得", "牌得主", "得主第", "得第二季"]
        """
        result = []
        chars = list(text)
        for i in range(len(chars) - 2):
            trigram = chars[i] + chars[i + 1] + chars[i + 2]
            result.append(trigram)

        # Remove duplicates while preserving order
        seen = set()
        unique_result = []
        for seg in result:
            if seg not in seen:
                seen.add(seg)
                unique_result.append(seg)

        return unique_result if unique_result else [text]

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
        msg_ctx = msg.message or ""
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
            logger.error(f"{err=},{traceback.format_exc()}")

        # Insert into message table
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
        try:
            self.cur.execute(
                "INSERT OR IGNORE INTO message (unique_id, user_id, chat_id, msg_id, msg_type, msg_ctx, mime_type, file_name, msg_js, date_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                insert_data,
            )
            # Insert into FTS table with spaces removed for better Chinese matching
            # This allows "金牌得主第二季" to match "金牌得主 第二季"
            import re
            msg_ctx_fts = re.sub(r'\s+', '', msg_ctx) if msg_ctx else ""
            file_name_fts = re.sub(r'\s+', '', file_name) if file_name else ""
            self.cur.execute(
                "INSERT OR REPLACE INTO message_fts (unique_id, msg_ctx, file_name) VALUES (?, ?, ?)",
                (unique_id, msg_ctx_fts, file_name_fts),
            )
            self.con.commit()
        except Exception as err:
            logger.error(f"{err=},{traceback.format_exc()}")

    def delete_by_unique_id(self, unique_id: str) -> bool:
        """Delete message by unique_id from both message and FTS tables."""
        try:
            self.cur.execute("DELETE FROM message WHERE unique_id = ?", (unique_id,))
            self.cur.execute("DELETE FROM message_fts WHERE unique_id = ?", (unique_id,))
            self.con.commit()
            return True
        except Exception as err:
            logger.error(f"{err=},{traceback.format_exc()}")
            return False

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
        """Create base tables if not exist."""
        if len(self.cur.execute("SELECT name FROM sqlite_master WHERE name='user'").fetchall()) == 0:
            self.cur.execute(
                "CREATE TABLE user(client_id primary key, username, phone, tg_user_id, last_login_time)"
            )
        if len(self.cur.execute("SELECT name FROM sqlite_master WHERE name='message'").fetchall()) == 0:
            self.cur.execute(
                """CREATE TABLE message(
                    unique_id varchar(64) primary key,
                    user_id int NOT NULL,
                    chat_id int NOT NULL,
                    msg_id int NOT NULL,
                    msg_type varchar(64),
                    msg_ctx text,
                    mime_type text,
                    file_name text,
                    msg_js text,
                    date_time int NOT NULL
                )"""
            )

    def _ensure_fts_table(self) -> None:
        """Ensure FTS5 virtual table exists and sync existing data."""
        fts_exists = len(
            self.cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='message_fts'").fetchall()
        ) > 0

        if not fts_exists:
            # Create FTS5 virtual table with trigram tokenizer
            # trigram works well for Chinese, English, and Japanese
            self.cur.execute(
                """CREATE VIRTUAL TABLE message_fts USING fts5(
                    unique_id UNINDEXED,
                    msg_ctx,
                    file_name,
                    tokenize='trigram'
                )"""
            )
            logger.info("Created FTS5 table with trigram tokenizer")

            # Sync existing data from message table
            self._sync_fts_data()

    def _sync_fts_data(self) -> None:
        """Sync existing message data to FTS table."""
        import re
        try:
            # Check if there's data to sync
            count = self.cur.execute("SELECT COUNT(*) FROM message").fetchone()[0]
            if count == 0:
                return

            # Sync data with spaces removed for better Chinese matching
            # SQLite doesn't support replace() in INSERT SELECT, so do it row by row
            rows = self.cur.execute("SELECT unique_id, msg_ctx, file_name FROM message")
            for row in rows:
                unique_id, msg_ctx, file_name = row
                msg_ctx_fts = re.sub(r'\s+', '', msg_ctx or "")
                file_name_fts = re.sub(r'\s+', '', file_name or "")
                self.cur.execute(
                    "INSERT OR IGNORE INTO message_fts (unique_id, msg_ctx, file_name) VALUES (?, ?, ?)",
                    (unique_id, msg_ctx_fts, file_name_fts),
                )
            self.con.commit()
            logger.info(f"Synced {count} messages to FTS table")
        except Exception as err:
            logger.error(f"FTS sync error: {err=},{traceback.format_exc()}")

    def rebuild_fts(self) -> bool:
        """Rebuild FTS index from scratch (for maintenance)."""
        try:
            self.cur.execute("DELETE FROM message_fts")
            self._sync_fts_data()
            logger.info("FTS index rebuilt successfully")
            return True
        except Exception as err:
            logger.error(f"FTS rebuild error: {err=},{traceback.format_exc()}")
            return False

    def get_fts_stats(self) -> dict:
        """Get FTS table statistics."""
        try:
            msg_count = self.cur.execute("SELECT COUNT(*) FROM message").fetchone()[0]
            fts_count = self.cur.execute("SELECT COUNT(*) FROM message_fts").fetchone()[0]
            return {
                "message_count": msg_count,
                "fts_count": fts_count,
                "sync_status": msg_count == fts_count,
            }
        except Exception as err:
            logger.error(f"{err=}")
            return {}


if __name__ == "__main__":
    db = UserManager()
    print(db.get_fts_stats())