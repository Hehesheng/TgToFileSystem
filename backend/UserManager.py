import os
import json
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

    def _compact_msg_js(self, msg: types.Message) -> str:
        """
        Compact msg_js to only essential fields for storage optimization.

        Reduces storage from ~5KB to ~2KB per message.
        Removes: entities, replies, reactions, all boolean flags, etc.
        Keeps: id, peer_id, date, media (compact version)
        """
        compact = {
            "id": msg.id,
            "peer_id": msg.peer_id.to_dict() if msg.peer_id else None,
            "date": str(msg.date),
        }

        # Compact media
        if msg.media:
            compact["media"] = self._compact_media(msg.media)

        return json.dumps(compact, ensure_ascii=False)

    def _compact_media(self, media) -> dict:
        """
        Compact media to essential fields for download/preview.

        For documents: keeps id, access_hash, file_reference, mime_type, size, dc_id, attributes, thumbs
        For photos: keeps id, access_hash, file_reference, dc_id, sizes (without stripped bytes)
        """
        result = {"_": media.__class__.__name__}

        if isinstance(media, types.MessageMediaPhoto):
            photo = media.photo
            if photo and isinstance(photo, types.Photo):
                result["photo"] = {
                    "id": photo.id,
                    "access_hash": photo.access_hash,
                    "file_reference": photo.file_reference.hex() if photo.file_reference else None,
                    "dc_id": photo.dc_id,
                    "sizes": self._compact_photo_sizes(photo.sizes),
                }
                # Keep video_cover if exists (for motion photo)
                if media.video_cover:
                    result["video_cover"] = {
                        "id": media.video_cover.id,
                        "access_hash": media.video_cover.access_hash,
                        "dc_id": media.video_cover.dc_id,
                    }

        elif isinstance(media, types.MessageMediaDocument):
            doc = media.document
            if doc and isinstance(doc, types.Document):
                result["document"] = {
                    "id": doc.id,
                    "access_hash": doc.access_hash,
                    "file_reference": doc.file_reference.hex() if doc.file_reference else None,
                    "dc_id": doc.dc_id,
                    "mime_type": doc.mime_type,
                    "size": doc.size,
                    "attributes": self._compact_doc_attributes(doc.attributes),
                    "thumbs": self._compact_thumbs(doc.thumbs) if doc.thumbs else None,
                    "video_thumbs": self._compact_thumbs(doc.video_thumbs) if doc.video_thumbs else None,
                }
                # Keep video_cover if exists
                if media.video_cover:
                    result["video_cover"] = {
                        "id": media.video_cover.id,
                        "access_hash": media.video_cover.access_hash,
                        "dc_id": media.video_cover.dc_id,
                    }

        return result

    def _compact_photo_sizes(self, sizes: list) -> list:
        """Compact photo sizes, removing PhotoStrippedSize.bytes."""
        result = []
        for size in sizes:
            if isinstance(size, types.PhotoStrippedSize):
                # Skip stripped size (too small for preview)
                continue
            elif isinstance(size, types.PhotoCachedSize):
                # Skip cached size bytes
                result.append({
                    "_": "PhotoCachedSize",
                    "type": size.type,
                    "w": size.w,
                    "h": size.h,
                })
            elif isinstance(size, (types.PhotoSize, types.PhotoSizeProgressive)):
                result.append({
                    "_": size.__class__.__name__,
                    "type": size.type,
                    "w": size.w,
                    "h": size.h,
                    "size": size.size if hasattr(size, 'size') else max(size.sizes) if hasattr(size, 'sizes') else 0,
                })
        return result

    def _compact_doc_attributes(self, attrs: list) -> list:
        """Compact document attributes to essential fields."""
        result = []
        for attr in attrs:
            if isinstance(attr, types.DocumentAttributeFilename):
                result.append({
                    "_": "DocumentAttributeFilename",
                    "file_name": attr.file_name,
                })
            elif isinstance(attr, types.DocumentAttributeVideo):
                result.append({
                    "_": "DocumentAttributeVideo",
                    "duration": attr.duration,
                    "w": attr.w,
                    "h": attr.h,
                })
            elif isinstance(attr, types.DocumentAttributeAudio):
                result.append({
                    "_": "DocumentAttributeAudio",
                    "duration": attr.duration,
                    "performer": attr.performer,
                    "title": attr.title,
                })
            elif isinstance(attr, types.DocumentAttributeImageSize):
                result.append({
                    "_": "DocumentAttributeImageSize",
                    "w": attr.w,
                    "h": attr.h,
                })
        return result

    def _compact_thumbs(self, thumbs: list) -> list:
        """Compact thumbs to essential fields."""
        result = []
        for thumb in thumbs or []:
            if isinstance(thumb, (types.PhotoSize, types.PhotoSizeProgressive)):
                result.append({
                    "_": thumb.__class__.__name__,
                    "type": thumb.type,
                    "w": thumb.w,
                    "h": thumb.h,
                    "size": thumb.size if hasattr(thumb, 'size') else max(thumb.sizes) if hasattr(thumb, 'sizes') else 0,
                })
        return result

    def _expand_msg_js(self, msg_js: str) -> dict:
        """
        Expand compact msg_js for frontend compatibility.

        Adds default values for missing fields so frontend API remains unchanged.
        """
        data = json.loads(msg_js)

        # Check if this is old format (has 'entities' with content)
        if "entities" in data and data.get("entities"):
            # Old format with entities - return as-is
            return data

        # Compact format: expand to match frontend expectations
        expanded = {
            "_": "Message",
            "id": data.get("id"),
            "peer_id": data.get("peer_id"),
            "date": data.get("date"),
            "message": "",  # Empty, actual content is in msg_ctx column
            "media": data.get("media"),
            # Add default empty values for other fields frontend might check
            "entities": [],
            "replies": None,
            "reactions": None,
            "views": None,
            "forwards": None,
            "edit_date": None,
            "post_author": None,
            "grouped_id": None,
            "from_id": None,
            "fwd_from": None,
            "via_bot_id": None,
            "reply_to": None,
        }

        # Restore file_reference from hex if needed
        if expanded.get("media"):
            expanded["media"] = self._restore_media_file_ref(expanded["media"])

        return expanded

    def _restore_media_file_ref(self, media: dict) -> dict:
        """Restore file_reference from hex string to bytes."""
        if "photo" in media and media["photo"]:
            if media["photo"].get("file_reference"):
                media["photo"]["file_reference"] = media["photo"]["file_reference"]
        if "document" in media and media["document"]:
            if media["document"].get("file_reference"):
                media["document"]["file_reference"] = media["document"]["file_reference"]
        return media

    def insert_by_message(self, me: types.User, msg: types.Message):
        user_id = me.id
        chat_id = msg.chat_id
        msg_id = msg.id
        unique_id = str(user_id) + str(chat_id) + str(msg_id)
        msg_type = UserManager.MessageTypeEnum.OTHERS.value
        mime_type = ""
        file_name = ""
        msg_ctx = msg.message or ""
        # Use compact msg_js for storage optimization
        msg_js = self._compact_msg_js(msg)
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

    def get_column_msg_js_expanded(self, column: tuple[any]) -> dict | None:
        """Get msg_js expanded for frontend compatibility."""
        msg_js = self.get_column_msg_js(column)
        if msg_js:
            return self._expand_msg_js(msg_js)
        return None

    def is_compact_format(self, msg_js: str) -> bool:
        """Check if msg_js is compact format (has 'message' or 'entities' field = old format)."""
        try:
            data = json.loads(msg_js)
            # Debug: print what we're checking
            logger.debug(f"is_compact_format: entities={data.get('entities')}, message={data.get('message')}")

            # Old format has 'entities' or 'message' field with actual content
            # Compact format has 'message' = '' (empty placeholder)
            has_entities = "entities" in data and isinstance(data.get("entities"), list) and len(data.get("entities", [])) > 0
            has_message = "message" in data and data.get("message") and len(str(data.get("message", ""))) > 0

            if has_entities or has_message:
                return False  # Old format

            return True  # Compact format
        except Exception as err:
            logger.warning(f"is_compact_format error: {err}")
            return True

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
            # Use fetchall() to avoid cursor reset during insert iteration
            rows = self.cur.execute("SELECT unique_id, msg_ctx, file_name FROM message").fetchall()
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

    def get_storage_stats(self) -> dict:
        """Get msg_js storage statistics."""
        import os
        try:
            # Count compact vs old format
            sample_size = 1000
            rows = self.cur.execute("SELECT msg_js FROM message LIMIT ?", (sample_size,)).fetchall()
            compact_count = sum(1 for r in rows if self.is_compact_format(r[0]))
            old_count = len(rows) - compact_count

            # Estimate sizes
            avg_compact_size = 0
            avg_old_size = 0
            compact_samples = [r[0] for r in rows if self.is_compact_format(r[0])][:100]
            old_samples = [r[0] for r in rows if not self.is_compact_format(r[0])][:100]

            if compact_samples:
                avg_compact_size = sum(len(s) for s in compact_samples) / len(compact_samples)
            if old_samples:
                avg_old_size = sum(len(s) for s in old_samples) / len(old_samples)

            total_count = self.cur.execute("SELECT COUNT(*) FROM message").fetchone()[0]
            db_size = os.path.getsize(f"{os.path.dirname(__file__)}/db/user.db")

            return {
                "total_count": total_count,
                "db_size_mb": db_size / 1024 / 1024,
                "compact_count_estimate": int(compact_count / sample_size * total_count),
                "old_count_estimate": int(old_count / sample_size * total_count),
                "avg_compact_size": avg_compact_size,
                "avg_old_size": avg_old_size,
                "potential_saving_mb": (avg_old_size - avg_compact_size) * old_count * total_count / sample_size / 1024 / 1024 if avg_old_size > avg_compact_size else 0,
            }
        except Exception as err:
            logger.error(f"{err=},{traceback.format_exc()}")
            return {}

    def migrate_to_compact(self, batch_size: int = 1000, limit: int = None) -> dict:
        """
        Migrate old msg_js format to compact format.

        Args:
            batch_size: Number of records per batch
            limit: Maximum total records to migrate (None = all)

        Returns:
            Stats about migration progress
        """
        import re
        try:
            # Get old format records
            count_query = "SELECT COUNT(*) FROM message"
            total_count = self.cur.execute(count_query).fetchone()[0]

            # Find old format records (those with entities or large msg_js)
            # Simple heuristic: msg_js length > 3000 = likely old format
            if limit:
                query = f"SELECT unique_id, msg_js FROM message WHERE LENGTH(msg_js) > 3000 LIMIT {limit}"
            else:
                query = "SELECT unique_id, msg_js FROM message WHERE LENGTH(msg_js) > 3000"

            rows = self.cur.execute(query).fetchall()
            to_migrate = len(rows)

            logger.info(f"Found {to_migrate} old format records to migrate")

            migrated = 0
            errors = 0

            for i, (unique_id, msg_js) in enumerate(rows):
                try:
                    old_data = json.loads(msg_js)

                    # Create compact version from old data
                    compact = {
                        "id": old_data.get("id"),
                        "peer_id": old_data.get("peer_id"),
                        "date": old_data.get("date"),
                    }

                    # Compact media if exists
                    if old_data.get("media"):
                        compact["media"] = self._compact_media_from_dict(old_data["media"])

                    compact_js = json.dumps(compact, ensure_ascii=False)

                    # Update record
                    self.cur.execute(
                        "UPDATE message SET msg_js = ? WHERE unique_id = ?",
                        (compact_js, unique_id),
                    )

                    # Also update FTS
                    msg_ctx = old_data.get("message", "")
                    file_name = self._extract_file_name_from_dict(old_data.get("media"))
                    msg_ctx_fts = re.sub(r'\s+', '', msg_ctx or "")
                    file_name_fts = re.sub(r'\s+', '', file_name or "")
                    self.cur.execute(
                        "UPDATE OR REPLACE message_fts SET msg_ctx = ?, file_name = ? WHERE unique_id = ?",
                        (msg_ctx_fts, file_name_fts, unique_id),
                    )

                    migrated += 1

                    # Commit in batches
                    if (i + 1) % batch_size == 0:
                        self.con.commit()
                        logger.info(f"Migrated {migrated}/{to_migrate} records")

                except Exception as err:
                    errors += 1
                    logger.warning(f"Migration error for {unique_id}: {err}")

            self.con.commit()
            logger.info(f"Migration complete: {migrated} records, {errors} errors")

            return {
                "total_found": to_migrate,
                "migrated": migrated,
                "errors": errors,
                "remaining": to_migrate - migrated,
            }

        except Exception as err:
            logger.error(f"Migration error: {err=},{traceback.format_exc()}")
            return {"error": str(err)}

    def _compact_media_from_dict(self, media: dict) -> dict:
        """Compact media dict (from old msg_js format)."""
        result = {"_": media.get("_", "")}

        if "photo" in media:
            photo = media["photo"]
            if photo:
                result["photo"] = {
                    "id": photo.get("id"),
                    "access_hash": photo.get("access_hash"),
                    "file_reference": photo.get("file_reference"),
                    "dc_id": photo.get("dc_id"),
                    "sizes": self._compact_photo_sizes_from_dict(photo.get("sizes", [])),
                }

        if "document" in media:
            doc = media["document"]
            if doc:
                result["document"] = {
                    "id": doc.get("id"),
                    "access_hash": doc.get("access_hash"),
                    "file_reference": doc.get("file_reference"),
                    "dc_id": doc.get("dc_id"),
                    "mime_type": doc.get("mime_type"),
                    "size": doc.get("size"),
                    "attributes": self._compact_attrs_from_dict(doc.get("attributes", [])),
                    "thumbs": self._compact_thumbs_from_dict(doc.get("thumbs")),
                    "video_thumbs": self._compact_thumbs_from_dict(doc.get("video_thumbs")),
                }

        if "video_cover" in media:
            vc = media["video_cover"]
            if vc:
                result["video_cover"] = {
                    "id": vc.get("id"),
                    "access_hash": vc.get("access_hash"),
                    "dc_id": vc.get("dc_id"),
                }

        return result

    def _compact_photo_sizes_from_dict(self, sizes: list) -> list:
        """Compact photo sizes from dict."""
        result = []
        for size in sizes:
            type_str = size.get("_", "")
            if "PhotoStrippedSize" in type_str:
                continue  # Skip stripped
            compact_size = {
                "_": type_str,
                "type": size.get("type"),
                "w": size.get("w"),
                "h": size.get("h"),
            }
            if "size" in size:
                compact_size["size"] = size["size"]
            elif "sizes" in size:
                compact_size["size"] = max(size["sizes"])
            result.append(compact_size)
        return result

    def _compact_attrs_from_dict(self, attrs: list) -> list:
        """Compact document attributes from dict."""
        result = []
        for attr in attrs:
            type_str = attr.get("_", "")
            if "Filename" in type_str:
                result.append({"_": type_str, "file_name": attr.get("file_name")})
            elif "Video" in type_str:
                result.append({"_": type_str, "duration": attr.get("duration"), "w": attr.get("w"), "h": attr.get("h")})
            elif "Audio" in type_str:
                result.append({"_": type_str, "duration": attr.get("duration"), "performer": attr.get("performer"), "title": attr.get("title")})
            elif "ImageSize" in type_str:
                result.append({"_": type_str, "w": attr.get("w"), "h": attr.get("h")})
        return result

    def _compact_thumbs_from_dict(self, thumbs: list | None) -> list | None:
        """Compact thumbs from dict."""
        if not thumbs:
            return None
        result = []
        for thumb in thumbs:
            type_str = thumb.get("_", "")
            if "Stripped" in type_str:
                continue
            compact_thumb = {
                "_": type_str,
                "type": thumb.get("type"),
                "w": thumb.get("w"),
                "h": thumb.get("h"),
            }
            if "size" in thumb:
                compact_thumb["size"] = thumb["size"]
            elif "sizes" in thumb:
                compact_thumb["size"] = max(thumb["sizes"])
            result.append(compact_thumb)
        return result if result else None

    def _extract_file_name_from_dict(self, media: dict | None) -> str:
        """Extract file name from media dict."""
        if not media:
            return ""
        doc = media.get("document")
        if doc:
            for attr in doc.get("attributes", []):
                if "Filename" in attr.get("_", ""):
                    return attr.get("file_name", "")
        return ""


if __name__ == "__main__":
    db = UserManager()
    print(db.get_fts_stats())