# app.py — tg mirror VPS backend (REWRITE: poll last_id, realtime, poll/post role split)
#
# Features (as requested):
# - Poll channels continuously using only message IDs (last_id), near-realtime
# - Separate roles:
#     * Poll session: ONLY polls (cannot post)
#     * Post session: ONLY posts (cannot poll)
#     * Or post by bot token
# - Poll session DIE -> auto failover to another poll session to continue polling
# - Post session DIE -> log + alert, DO NOT failover; keep pending and retry later
# - Multiple jobs can share the same poll channel:
#     * only ONE poll session joins/polls that source channel
#     * each job posts to its own destination via its configured post session or bot
# - No message loss:
#     * each job has its own cursor last_ok_id
#     * cursor advances ONLY when repost succeeds
#
# APIs:
#   GET  /status
#   POST /add
#   POST /sessions/upload
#   POST /sessions/delete
#   POST /session/start
#   POST /session/code
#   POST /session/password
#   POST /session/resend
#   GET  /session/status
#   GET  /session/download
#
# Install:
#   pip install -U fastapi uvicorn[standard] telethon aiohttp pydantic-settings python-multipart
# Run:
#   python app.py

import asyncio
import json
import os
import io
import random
import time
import re
import secrets
import shutil
import zipfile
import hashlib
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime
from contextlib import asynccontextmanager

import aiohttp
from fastapi import FastAPI, Depends, HTTPException, Request, UploadFile, File
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings

from telethon import TelegramClient
from telethon.errors import (
    ChannelPrivateError,
    FloodWaitError,
    ChatAdminRequiredError,
    SessionPasswordNeededError,
    PhoneCodeInvalidError,
    PhoneCodeExpiredError,
    PhoneNumberInvalidError,
    AuthKeyUnregisteredError,
    UserDeactivatedError,
)
from telethon.tl.types import (
    Message,
    MessageMediaPhoto,
    MessageMediaDocument,
)
from telethon.tl.functions.channels import JoinChannelRequest

try:
    from telethon.extensions import html as tl_html
except Exception:
    tl_html = None


# ===================== Config =====================
class Cfg(BaseSettings):
    API_ID: int
    API_HASH: str

    # Protect API
    API_BEARER: str = ""

    # Realtime polling
    POLL_TICK_SEC: float = 1.5            # near realtime
    BATCH_MAX: int = 50                   # batch per poll tick per channel
    IDLE_JITTER_MS: int = 150             # jitter to avoid sync bursts

    # Join throttle
    JOIN_INTERVAL_SEC: int = 180
    JOIN_JITTER_MS: int = 2500

    # Session scan / health
    SESS_RESCAN_SEC: int = 20
    HEALTHCHECK_INTERVAL_SEC: int = 45

    # Media
    INCLUDE_MEDIA: bool = Field(default=True)
    MEDIA_MAX_MB: float = Field(default=50.0)

    # Alerts via Bot (optional)
    TELEGRAM_ALERT_BOT_TOKEN: str = ""
    TELEGRAM_ALERT_CHAT_ID: str = ""
    TELEGRAM_ALERT_TOPIC_ID: Optional[int] = None

    # Bind
    BIND_HOST: str = Field(default="0.0.0.0")
    BIND_PORT: int = Field(default=8080)

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


cfg = Cfg()

# ===================== Paths =====================
BASE_DIR = Path(__file__).resolve().parent
SESS_DIR = BASE_DIR / "sessions"
PENDING_SESS_DIR = BASE_DIR / "sessions_pending"
STATE_FILE = BASE_DIR / "state.json"

SESS_DIR.mkdir(parents=True, exist_ok=True)
PENDING_SESS_DIR.mkdir(parents=True, exist_ok=True)

MEDIA_MAX_BYTES = int(cfg.MEDIA_MAX_MB * 1024 * 1024)
SEPARATOR = "\n\n--------------------------------\n"
_SAFE_NAME_RE = re.compile(r"^[a-zA-Z0-9._+-]+$")

# ===================== State =====================
_state_lock = asyncio.Lock()

# New state layout:
#   pollers: { poll_channel: {poll_session_name, session_index, chat_id, created_ts} }
#   jobs:    { job_id: {poll_chanel, post_chanel, post_by, post_seasion, bot_token, textdelete, caption, last_ok_id, last_error, paused_reason} }
#   recent_by_session: { "xxx.session": [ {link,poll,post,ts} ... ] }
#   recent_by_bot:     { bot_key: [ ... ] }
#   dead_sessions:     { session_file: {ts,reason,last_error} }
_state: Dict[str, Any] = {
    "pollers": {},
    "jobs": {},
    "recent_by_session": {},
    "recent_by_bot": {},
    "dead_sessions": {},
}

if STATE_FILE.exists():
    try:
        _state.update(json.loads(STATE_FILE.read_text("utf-8")))
        _state.setdefault("pollers", {})
        _state.setdefault("jobs", {})
        _state.setdefault("recent_by_session", {})
        _state.setdefault("recent_by_bot", {})
        _state.setdefault("dead_sessions", {})
    except Exception:
        pass


async def save_state():
    async with _state_lock:
        STATE_FILE.write_text(json.dumps(_state, ensure_ascii=False, indent=2), "utf-8")


# ===================== Auth =====================
def require_bearer(req: Request):
    if not cfg.API_BEARER:
        return
    auth = req.headers.get("authorization", "")
    if not auth.startswith("Bearer ") or auth[7:] != cfg.API_BEARER:
        raise HTTPException(status_code=401, detail="Invalid token")


# ===================== Sessions wrap =====================
class SessionWrap:
    def __init__(self, index: int, path: Path):
        self.index = index
        self.path = path
        self.client: Optional[TelegramClient] = None
        self.online: bool = False
        self.next_join_ts: float = 0.0
        self.last_check_ts: float = 0.0
        self.last_error: str = ""


_sessions: List[SessionWrap] = []
_session_by_path: Dict[str, SessionWrap] = {}
_sessions_lock = asyncio.Lock()


def list_session_files() -> List[Path]:
    return sorted(SESS_DIR.glob("*.session"))


def _now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _norm_ch(s: str) -> str:
    return (s or "").strip().lstrip("@").strip()


def _norm_session_name(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    if s.endswith(".session"):
        s = s[:-8]
    return s


def _safe_filename(name: str) -> str:
    name = os.path.basename(name or "")
    if not name:
        return ""
    if not _SAFE_NAME_RE.match(name):
        name = re.sub(r"[^a-zA-Z0-9._+-]+", "_", name)
    return name


def _session_display_name(sw: SessionWrap) -> str:
    return sw.path.stem or f"session_{sw.index+1}"


def _sess_key(sw: SessionWrap) -> str:
    return sw.path.name


def _bot_key(tok: str) -> str:
    t = (tok or "").strip().encode("utf-8")
    return hashlib.sha1(t).hexdigest()[:16]


# ===================== Alerts =====================
async def alert(text: str):
    if not (cfg.TELEGRAM_ALERT_BOT_TOKEN and cfg.TELEGRAM_ALERT_CHAT_ID):
        return

    url = f"https://api.telegram.org/bot{cfg.TELEGRAM_ALERT_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": str(cfg.TELEGRAM_ALERT_CHAT_ID).strip(),
        "text": text,
        "disable_web_page_preview": False,
    }
    topic = cfg.TELEGRAM_ALERT_TOPIC_ID
    if topic is not None and str(topic).strip() != "":
        payload["message_thread_id"] = int(str(topic).strip())

    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as s:
            async with s.post(url, json=payload) as r:
                _ = await r.text()
    except Exception:
        pass


async def notify_new_post(who: str, post_display: str, link: str):
    text = (
        f"Tên seasion: {who}\n"
        f"Tên chanel : {post_display}\n"
        f"Link bài mới: {link}\n"
        f"Ngày giờ    : {_now_str()}"
    )
    await alert(text)


# ===================== Role rules =====================
def _compute_roles_from_state() -> Tuple[set, set]:
    """Return (poll_sessions, post_sessions) as sets of lowercase session base names."""
    poll_sessions = set()
    post_sessions = set()

    for _poll, meta in (_state.get("pollers") or {}).items():
        sn = (meta.get("poll_session_name") or "").strip().lower()
        if sn:
            poll_sessions.add(sn)

    for _jid, job in (_state.get("jobs") or {}).items():
        if (job.get("post_by") or "user").lower() == "user":
            ps = (job.get("post_seasion") or "").strip().lower()
            if ps:
                post_sessions.add(ps)

    return poll_sessions, post_sessions


def _online_sessions_snapshot() -> List[SessionWrap]:
    return [sw for sw in _sessions if sw.client and sw.online]


def _poll_load_counts() -> Dict[int, int]:
    """How many pollers assigned to each session index."""
    counts: Dict[int, int] = {}
    for _poll, meta in (_state.get("pollers") or {}).items():
        idx = meta.get("session_index")
        if isinstance(idx, int) and idx >= 0:
            counts[idx] = counts.get(idx, 0) + 1
    return counts


def _pick_poll_session_least_loaded(exclude_session_names_lower: set) -> Optional[SessionWrap]:
    online = _online_sessions_snapshot()
    if not online:
        return None
    counts = _poll_load_counts()
    best = None
    best_count = 10**9
    for sw in online:
        name = _session_display_name(sw).lower()
        if name in exclude_session_names_lower:
            continue
        c = counts.get(sw.index, 0)
        if c < best_count:
            best = sw
            best_count = c
    return best


async def start_session(sw: SessionWrap):
    if sw.client:
        return
    sw.client = TelegramClient(str(sw.path.with_suffix("")), cfg.API_ID, cfg.API_HASH)
    try:
        await sw.client.connect()
        ok = await sw.client.is_user_authorized()
        sw.online = bool(ok)
        sw.last_error = "" if ok else "not authorized"
    except (AuthKeyUnregisteredError, UserDeactivatedError) as e:
        sw.online = False
        sw.last_error = f"{type(e).__name__}"
    except Exception as e:
        sw.online = False
        sw.last_error = f"start failed: {e}"
    finally:
        sw.last_check_ts = time.time()


async def stop_session(sw: SessionWrap):
    try:
        if sw.client:
            await sw.client.disconnect()
    except Exception:
        pass
    sw.client = None
    sw.online = False


async def _check_session_health(sw: SessionWrap):
    sw.last_check_ts = time.time()
    if not sw.client:
        await start_session(sw)
        return
    try:
        if not sw.client.is_connected():
            await sw.client.connect()
        ok = await sw.client.is_user_authorized()
        sw.online = bool(ok)
        sw.last_error = "" if ok else "not authorized"
    except (AuthKeyUnregisteredError, UserDeactivatedError) as e:
        sw.online = False
        sw.last_error = f"{type(e).__name__}"
    except Exception as e:
        sw.online = False
        sw.last_error = f"health failed: {e}"


async def find_session_wrap_by_name(name: str) -> Optional[SessionWrap]:
    key = _norm_session_name(name).lower()
    if not key:
        return None
    async with _sessions_lock:
        snapshot = list(_sessions)
    for sw in snapshot:
        stem = (sw.path.stem or "").lower()
        full = (sw.path.name or "").lower()
        disp = (_session_display_name(sw) or "").lower()
        if key in (stem, disp, full):
            if not sw.client:
                await start_session(sw)
            return sw
    return None


# ===================== Join helpers =====================
async def ensure_join(sw: SessionWrap, channel: str):
    if not sw.client:
        return
    now = time.time()
    if now < sw.next_join_ts:
        await asyncio.sleep(sw.next_join_ts - now)
    await asyncio.sleep(random.uniform(0, cfg.JOIN_JITTER_MS / 1000.0))
    try:
        ent = await sw.client.get_entity(channel)
        await sw.client(JoinChannelRequest(ent))
    except FloodWaitError as e:
        await asyncio.sleep(int(e.seconds))
    except (ChannelPrivateError, ChatAdminRequiredError):
        pass
    except Exception:
        pass
    sw.next_join_ts = time.time() + cfg.JOIN_INTERVAL_SEC


# ===================== Post link / recent =====================
def _build_post_link(post_entity, msg_id: int) -> str:
    try:
        username = getattr(post_entity, "username", None)
        if username:
            return f"https://t.me/{username}/{msg_id}"
        cid = getattr(post_entity, "id", None)
        if cid:
            return f"https://t.me/c/{cid}/{msg_id}"
    except Exception:
        pass
    return "N/A"


async def record_reup_session(sw: SessionWrap, poll_name: str, post_ent, post_name_norm: str, msg_id: int):
    link = _build_post_link(post_ent, msg_id)
    item = {"link": link, "poll": _norm_ch(poll_name), "post": post_name_norm, "ts": _now_str()}

    _state.setdefault("recent_by_session", {})
    key = _sess_key(sw)
    arr = _state["recent_by_session"].setdefault(key, [])
    arr.insert(0, item)
    del arr[10:]
    await save_state()


async def record_reup_bot(bot_token: str, poll_name: str, post_name_norm: str, link: str):
    item = {"link": link, "poll": _norm_ch(poll_name), "post": post_name_norm, "ts": _now_str()}

    _state.setdefault("recent_by_bot", {})
    key = _bot_key(bot_token)
    arr = _state["recent_by_bot"].setdefault(key, [])
    arr.insert(0, item)
    del arr[10:]
    await save_state()


# ===================== Text helpers =====================
def _apply_textdelete(text: str, textdelete: str) -> str:
    if not textdelete:
        return text or ""
    return (text or "").replace(textdelete, "").strip()


def _append_caption(clean_text: str, caption: str) -> str:
    cap = (caption or "").strip()
    if not cap:
        return (clean_text or "").strip()
    base = (clean_text or "").strip()
    if not base:
        return cap
    return (base + SEPARATOR + cap).strip()


# ===================== Media download =====================
async def _download_media_bytes(client: TelegramClient, msg: Message) -> Optional[Tuple[str, bytes, str]]:
    if not msg.media:
        return None

    if isinstance(msg.media, MessageMediaPhoto):
        buf = io.BytesIO()
        try:
            await client.download_media(msg, file=buf)
            data = buf.getvalue()
            if not data or len(data) > MEDIA_MAX_BYTES:
                return None
            return (f"photo_{msg.id}.jpg", data, "image/jpeg")
        except Exception:
            return None

    if isinstance(msg.media, MessageMediaDocument) and msg.media.document:
        doc = msg.media.document
        mime = (doc.mime_type or "application/octet-stream").lower()

        name = None
        for a in (doc.attributes or []):
            if hasattr(a, "file_name") and a.file_name:
                name = a.file_name
                break
        if not name:
            name = f"file_{msg.id}"

        buf = io.BytesIO()
        try:
            await client.download_media(msg, file=buf)
            data = buf.getvalue()
            if not data or len(data) > MEDIA_MAX_BYTES:
                return None
            return (name, data, mime)
        except Exception:
            return None

    return None


# ===================== Bot API send =====================
async def _tg_bot_call(token: str, method: str, data: Dict[str, Any]) -> Dict[str, Any]:
    url = f"https://api.telegram.org/bot{token}/{method}"
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as s:
        async with s.post(url, json=data) as r:
            j = await r.json(content_type=None)
            if not j.get("ok"):
                raise RuntimeError(str(j))
            return j


async def _tg_bot_upload(token: str, method: str, fields: Dict[str, Any], file_field: str, fname: str, data: bytes) -> Dict[str, Any]:
    url = f"https://api.telegram.org/bot{token}/{method}"
    form = aiohttp.FormData()
    for k, v in fields.items():
        form.add_field(k, str(v))
    form.add_field(file_field, data, filename=fname)
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as s:
        async with s.post(url, data=form) as r:
            j = await r.json(content_type=None)
            if not j.get("ok"):
                raise RuntimeError(str(j))
            return j


def _bot_chat_id_from_channel(username: str) -> str:
    u = _norm_ch(username)
    return f"@{u}"


async def _bot_send_text_html(token: str, dest: str, html_text: str) -> str:
    j = await _tg_bot_call(token, "sendMessage", {
        "chat_id": _bot_chat_id_from_channel(dest),
        "text": html_text or "",
        "parse_mode": "HTML",
        "disable_web_page_preview": False,
    })
    msg = j.get("result") or {}
    mid = msg.get("message_id")
    ch = msg.get("chat") or {}
    uname = ch.get("username") or _norm_ch(dest)
    return f"https://t.me/{uname}/{mid}" if mid else "N/A"


async def _bot_send_text_plain(token: str, dest: str, text: str) -> str:
    j = await _tg_bot_call(token, "sendMessage", {
        "chat_id": _bot_chat_id_from_channel(dest),
        "text": text or "",
        "disable_web_page_preview": False,
    })
    msg = j.get("result") or {}
    mid = msg.get("message_id")
    ch = msg.get("chat") or {}
    uname = ch.get("username") or _norm_ch(dest)
    return f"https://t.me/{uname}/{mid}" if mid else "N/A"


async def _bot_send_photo(token: str, dest: str, caption_html: str, fname: str, data: bytes) -> str:
    j = await _tg_bot_upload(token, "sendPhoto", {
        "chat_id": _bot_chat_id_from_channel(dest),
        "caption": caption_html or "",
        "parse_mode": "HTML" if caption_html else None,
    }, "photo", fname, data)
    msg = j.get("result") or {}
    mid = msg.get("message_id")
    ch = msg.get("chat") or {}
    uname = ch.get("username") or _norm_ch(dest)
    return f"https://t.me/{uname}/{mid}" if mid else "N/A"


async def _bot_send_document(token: str, dest: str, caption_html: str, fname: str, data: bytes) -> str:
    j = await _tg_bot_upload(token, "sendDocument", {
        "chat_id": _bot_chat_id_from_channel(dest),
        "caption": caption_html or "",
        "parse_mode": "HTML" if caption_html else None,
    }, "document", fname, data)
    msg = j.get("result") or {}
    mid = msg.get("message_id")
    ch = msg.get("chat") or {}
    uname = ch.get("username") or _norm_ch(dest)
    return f"https://t.me/{uname}/{mid}" if mid else "N/A"


# ===================== Repost core (returns bool) =====================
async def _repost_single(poll_sw: SessionWrap, poll_name: str, job: Dict[str, Any], msg: Message) -> bool:
    """
    Return True if posted successfully, else False.
    Important: Do NOT advance job cursor unless True.
    """
    post_name = job.get("post_chanel") or ""
    if not post_name:
        return False

    post_by = (job.get("post_by") or "user").lower()
    textdelete = job.get("textdelete") or ""
    caption = job.get("caption") or ""
    post_seasion = job.get("post_seasion") or ""
    bot_token = (job.get("bot_token") or "").strip()

    original_text = msg.message or ""
    clean_text = _apply_textdelete(original_text, textdelete)
    final_text = _append_caption(clean_text, caption)

    # Try to preserve formatting:
    # - User mode: prefer entities if no textdelete and no extra caption
    # - Bot mode: prefer HTML if possible (Telethon html.unparse)
    use_entities = (not textdelete) and bool(msg.entities)
    html_text_for_bot = None
    if use_entities and tl_html:
        try:
            html_text_for_bot = tl_html.unparse(original_text, msg.entities)
        except Exception:
            html_text_for_bot = None

    # media download via poll session (poll_sw)
    media_blob = None
    if cfg.INCLUDE_MEDIA and poll_sw.client:
        media_blob = await _download_media_bytes(poll_sw.client, msg)

    # ===== BOT MODE =====
    if post_by == "bot":
        if not bot_token:
            job["last_error"] = "BOT: missing bot_token"
            return False
        try:
            # for bot: use HTML only if we are not appending caption separator logic that breaks entities.
            # If caption/textdelete applied, we just send plain final_text.
            if textdelete or caption:
                bot_text = final_text or ""
                if media_blob:
                    fname, data, mime = media_blob
                    if mime.startswith("image/"):
                        link = await _bot_send_photo(bot_token, post_name, bot_text, fname, data)
                    else:
                        link = await _bot_send_document(bot_token, post_name, bot_text, fname, data)
                else:
                    link = await _bot_send_text_plain(bot_token, post_name, bot_text)
            else:
                # no caption/textdelete -> keep formatting
                bot_html = html_text_for_bot or (final_text or "")
                if media_blob:
                    fname, data, mime = media_blob
                    if mime.startswith("image/"):
                        link = await _bot_send_photo(bot_token, post_name, bot_html, fname, data)
                    else:
                        link = await _bot_send_document(bot_token, post_name, bot_html, fname, data)
                else:
                    # if we have html_text_for_bot use HTML else plain
                    if html_text_for_bot:
                        link = await _bot_send_text_html(bot_token, post_name, bot_html)
                    else:
                        link = await _bot_send_text_plain(bot_token, post_name, bot_html)

            await record_reup_bot(bot_token, poll_name, _norm_ch(post_name), link)
            await notify_new_post("BOT", f"@{_norm_ch(post_name)} (BOT)", link)
            job["last_error"] = ""
            job["paused_reason"] = ""
            return True
        except Exception as e:
            job["last_error"] = f"BOT send failed: {e}"
            return False

    # ===== USER MODE (post session) =====
    if not post_seasion:
        job["last_error"] = "USER post requires post_seasion"
        return False

    post_sw = await find_session_wrap_by_name(post_seasion)
    if not post_sw or not post_sw.client:
        job["last_error"] = f"POST session not found: {post_seasion}"
        job["paused_reason"] = "post_session_missing"
        return False

    await _check_session_health(post_sw)
    if not post_sw.online:
        # as requested: DO NOT failover, just log/alert and keep pending
        job["last_error"] = f"POST session DIE: {_session_display_name(post_sw)} ({post_sw.last_error})"
        job["paused_reason"] = "post_session_die"
        # alert once in a while (cheap throttle)
        if int(time.time()) % 30 == 0:
            await alert(
                "⚠️ Post session DIE (không thay thế)\n"
                f"Job: {job.get('job_id')}\n"
                f"Poll: @{_norm_ch(poll_name)}\n"
                f"Post: @{_norm_ch(post_name)}\n"
                f"Post session: {_session_display_name(post_sw)}\n"
                f"Lỗi: {post_sw.last_error}"
            )
        return False

    # Ensure join destination (post session only)
    await ensure_join(post_sw, post_name)

    client = post_sw.client
    try:
        post_ent = await client.get_entity(post_name)
    except Exception as e:
        job["last_error"] = f"get_entity(post) failed: {e}"
        return False

    # Decide sending method
    try:
        if media_blob:
            fname, data, _mime = media_blob
            file_obj = io.BytesIO(data)
            file_obj.name = fname

            sent = await client.send_file(
                post_ent,
                file=file_obj,
                caption=final_text if final_text else None,
                force_document=False
            )
            if sent and getattr(sent, "id", None):
                link = _build_post_link(post_ent, sent.id)
                await record_reup_session(post_sw, poll_name, post_ent, _norm_ch(post_name), sent.id)
                await notify_new_post(_session_display_name(post_sw), getattr(post_ent, "title", None) or f"@{_norm_ch(post_name)}", link)
                job["last_error"] = ""
                job["paused_reason"] = ""
                return True
            job["last_error"] = "send_file returned no id"
            return False

        # no media:
        if use_entities and (not caption) and (not textdelete):
            # keep entities exactly
            sent = await client.send_message(post_ent, original_text, entities=msg.entities, link_preview=True)
            if sent and getattr(sent, "id", None):
                link = _build_post_link(post_ent, sent.id)
                await record_reup_session(post_sw, poll_name, post_ent, _norm_ch(post_name), sent.id)
                await notify_new_post(_session_display_name(post_sw), getattr(post_ent, "title", None) or f"@{_norm_ch(post_name)}", link)
                job["last_error"] = ""
                job["paused_reason"] = ""
                return True
            job["last_error"] = "send_message(entities) returned no id"
            return False

        # else: send final_text
        sent = await client.send_message(post_ent, final_text or "", link_preview=True)
        if sent and getattr(sent, "id", None):
            link = _build_post_link(post_ent, sent.id)
            await record_reup_session(post_sw, poll_name, post_ent, _norm_ch(post_name), sent.id)
            await notify_new_post(_session_display_name(post_sw), getattr(post_ent, "title", None) or f"@{_norm_ch(post_name)}", link)
            job["last_error"] = ""
            job["paused_reason"] = ""
            return True

        job["last_error"] = "send_message returned no id"
        return False

    except ChatAdminRequiredError as e:
        job["last_error"] = f"ChatAdminRequiredError: {e}"
        return False
    except Exception as e:
        job["last_error"] = f"send failed: {e}"
        return False


async def _repost_album(poll_sw: SessionWrap, poll_name: str, job: Dict[str, Any], msgs: List[Message]) -> bool:
    """
    Album repost:
    - For reliability & speed, we repost based on the "primary" caption msg.
    - If you need perfect album replication, tell me, I'll expand this.
    """
    if not msgs:
        return False
    # Prefer the msg with longest text as primary
    primary = max(msgs, key=lambda x: len(x.message or ""), default=msgs[0])
    return await _repost_single(poll_sw, poll_name, job, primary)


# ===================== Poller + job helpers =====================
def _job_id(poll: str, post: str, post_by: str, post_sess: str, bot_token: str) -> str:
    raw = f"{_norm_ch(poll)}|{_norm_ch(post)}|{(post_by or 'user').lower()}|{_norm_session_name(post_sess).lower()}|{_bot_key(bot_token) if bot_token else ''}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _jobs_for_poll(poll_name: str) -> List[Dict[str, Any]]:
    out = []
    for _jid, job in (_state.get("jobs") or {}).items():
        if _norm_ch(job.get("poll_chanel") or "") == _norm_ch(poll_name):
            out.append(job)
    return out


async def _get_latest_id(client: TelegramClient, poll_name: str) -> int:
    try:
        ent = await client.get_entity(poll_name)
        # iter_messages(limit=1) returns most recent
        async for m in client.iter_messages(ent, limit=1):
            return int(m.id or 0)
    except Exception:
        return 0


async def _failover_poller_if_needed(poll_name: str):
    pollers = _state.get("pollers") or {}
    meta = pollers.get(poll_name)
    if not meta:
        return

    poll_session_name = (meta.get("poll_session_name") or "").strip()
    sw = await find_session_wrap_by_name(poll_session_name) if poll_session_name else None
    if sw:
        await _check_session_health(sw)
    if sw and sw.online and sw.client:
        return  # ok

    # poll session is die/missing -> pick another poll session (NOT in post role)
    poll_sessions, post_sessions = _compute_roles_from_state()
    dst = _pick_poll_session_least_loaded(exclude_session_names_lower=post_sessions)
    if not dst:
        meta["last_error"] = f"poller has no available poll session (old={poll_session_name})"
        return

    meta["poll_session_name"] = _session_display_name(dst)
    meta["session_index"] = dst.index
    meta["last_error"] = ""
    meta["last_failover_ts"] = _now_str()
    pollers[poll_name] = meta
    _state["pollers"] = pollers
    await save_state()

    await alert(
        "🔁 Failover poll session\n"
        f"Poll channel: @{_norm_ch(poll_name)}\n"
        f"Session cũ : {poll_session_name or '(none)'}\n"
        f"Session mới: {_session_display_name(dst)}"
    )


# ===================== Poll loop =====================
async def poll_loop():
    try:
        while True:
            try:
                await asyncio.sleep(random.uniform(0, cfg.IDLE_JITTER_MS / 1000.0))

                pollers = list((_state.get("pollers") or {}).keys())
                if not pollers:
                    await asyncio.sleep(cfg.POLL_TICK_SEC)
                    continue

                # Poll all channels concurrently (lightweight)
                tasks = []
                for poll_name in pollers:
                    tasks.append(_poll_one_channel(poll_name))
                await asyncio.gather(*tasks, return_exceptions=True)

            except Exception:
                pass

            await asyncio.sleep(cfg.POLL_TICK_SEC)
    except asyncio.CancelledError:
        return


async def _poll_one_channel(poll_name: str):
    poll_name = _norm_ch(poll_name)
    if not poll_name:
        return

    await _failover_poller_if_needed(poll_name)
    meta = (_state.get("pollers") or {}).get(poll_name) or {}
    poll_session_name = (meta.get("poll_session_name") or "").strip()
    if not poll_session_name:
        return

    poll_sw = await find_session_wrap_by_name(poll_session_name)
    if not poll_sw or not poll_sw.client:
        return
    await _check_session_health(poll_sw)
    if not poll_sw.online:
        return

    jobs = _jobs_for_poll(poll_name)
    if not jobs:
        return

    # Poll session should join source channel (only this session joins/polls source)
    await ensure_join(poll_sw, poll_name)

    # Determine min cursor among jobs (only by message ID)
    min_cursor = None
    for job in jobs:
        try:
            cid = int(job.get("last_ok_id") or 0)
        except Exception:
            cid = 0
        if min_cursor is None or cid < min_cursor:
            min_cursor = cid
    min_cursor = int(min_cursor or 0)

    client = poll_sw.client
    try:
        ent = await client.get_entity(poll_name)
    except Exception:
        return

    # Fetch messages with id > min_cursor, oldest->newest (reverse=True)
    msgs: List[Message] = []
    try:
        async for m in client.iter_messages(ent, min_id=min_cursor, reverse=True, limit=cfg.BATCH_MAX):
            if not m or not getattr(m, "id", None):
                continue
            if int(m.id) <= min_cursor:
                continue
            msgs.append(m)
    except FloodWaitError as e:
        await asyncio.sleep(int(e.seconds))
        return
    except (ChannelPrivateError, ChatAdminRequiredError):
        return
    except AuthKeyUnregisteredError:
        poll_sw.online = False
        poll_sw.last_error = "AuthKeyUnregisteredError"
        return
    except Exception:
        return

    if not msgs:
        return

    # Group albums
    groups: Dict[int, List[Message]] = {}
    singles: List[Message] = []
    for m in msgs:
        gid = getattr(m, "grouped_id", None)
        if gid:
            groups.setdefault(gid, []).append(m)
        else:
            singles.append(m)

    # Process albums first (stable ordering by smallest msg id)
    for gid in sorted(groups.keys()):
        album = groups[gid]
        album.sort(key=lambda x: int(x.id or 0))
        await _process_message_for_jobs(poll_sw, poll_name, album[-1], album_messages=album)

    # Process singles in ascending order
    singles.sort(key=lambda x: int(x.id or 0))
    for m in singles:
        await _process_message_for_jobs(poll_sw, poll_name, m, album_messages=None)

    await save_state()


async def _process_message_for_jobs(poll_sw: SessionWrap, poll_name: str, msg: Message, album_messages: Optional[List[Message]]):
    jobs = _jobs_for_poll(poll_name)
    if not jobs:
        return

    mid = int(msg.id or 0)
    if mid <= 0:
        return

    # For each job, if this message id is new for that job, try repost.
    # Important: job cursor advances ONLY on success.
    for job in jobs:
        try:
            last_ok = int(job.get("last_ok_id") or 0)
        except Exception:
            last_ok = 0

        if mid <= last_ok:
            continue

        ok = False
        if album_messages:
            ok = await _repost_album(poll_sw, poll_name, job, album_messages)
        else:
            ok = await _repost_single(poll_sw, poll_name, job, msg)

        if ok:
            job["last_ok_id"] = mid
            job["last_error"] = ""
            job["paused_reason"] = ""
        else:
            # keep last_ok_id unchanged -> will retry next ticks
            # If posting session is die -> this job will remain pending, as requested.
            # Continue to next job; do not block other jobs posting to other destinations.
            pass


# ===================== Session monitor / health =====================
async def rescan_sessions():
    async with _sessions_lock:
        files = list_session_files()
        paths = {str(p): p for p in files}

        # remove missing
        for p in list(_session_by_path.keys()):
            if p not in paths:
                sw = _session_by_path.pop(p)
                try:
                    await stop_session(sw)
                finally:
                    try:
                        _sessions.remove(sw)
                    except ValueError:
                        pass

        # add new
        existing = set(_session_by_path.keys())
        for p in files:
            sp = str(p)
            if sp in existing:
                continue
            sw = SessionWrap(len(_sessions), p)
            _sessions.append(sw)
            _session_by_path[sp] = sw
            await start_session(sw)

        # re-index
        for i, sw in enumerate(_sessions):
            sw.index = i

    await save_state()


async def _monitor_sessions():
    try:
        while True:
            try:
                await rescan_sessions()
            except Exception:
                pass
            await asyncio.sleep(cfg.SESS_RESCAN_SEC)
    except asyncio.CancelledError:
        return


async def _health_loop():
    try:
        while True:
            try:
                async with _sessions_lock:
                    snapshot = list(_sessions)

                for sw in snapshot:
                    await _check_session_health(sw)

                dead_map = {}
                for sw in snapshot:
                    if not (sw.online and sw.client):
                        dead_map[_sess_key(sw)] = {
                            "ts": _now_str(),
                            "reason": "DIE",
                            "last_error": sw.last_error or ""
                        }
                _state["dead_sessions"] = dead_map
                await save_state()
            except Exception:
                pass
            await asyncio.sleep(cfg.HEALTHCHECK_INTERVAL_SEC)
    except asyncio.CancelledError:
        return


# ===================== Session login flow (login_id) =====================
LOGIN_TTL_SEC = 10 * 60
_pending_login_lock = asyncio.Lock()
_pending_logins: Dict[str, Dict[str, Any]] = {}


def _session_paths_in(dir_: Path, session_base: str) -> Tuple[Path, Path]:
    sess = dir_ / f"{session_base}.session"
    journal = dir_ / f"{session_base}.session-journal"
    return sess, journal


def _cleanup_login_files(dir_: Path, session_base: str):
    sess, journal = _session_paths_in(dir_, session_base)
    try:
        if sess.exists():
            sess.unlink()
    except Exception:
        pass
    try:
        if journal.exists():
            journal.unlink()
    except Exception:
        pass


def _move_login_files(session_base: str, force: bool = False):
    src_sess, src_journal = _session_paths_in(PENDING_SESS_DIR, session_base)
    dst_sess, dst_journal = _session_paths_in(SESS_DIR, session_base)

    if force:
        try:
            if dst_sess.exists():
                dst_sess.unlink()
        except Exception:
            pass
        try:
            if dst_journal.exists():
                dst_journal.unlink()
        except Exception:
            pass

    if src_sess.exists():
        shutil.move(str(src_sess), str(dst_sess))
    if src_journal.exists():
        shutil.move(str(src_journal), str(dst_journal))


async def _gc_pending_logins():
    now = time.time()
    async with _pending_login_lock:
        dead = []
        for lid, meta in _pending_logins.items():
            if now > float(meta.get("expires_at", 0)):
                dead.append(lid)
        for lid in dead:
            meta = _pending_logins.pop(lid, None) or {}
            client = meta.get("client")
            session_base = meta.get("session_base")
            try:
                if client:
                    await client.disconnect()
            except Exception:
                pass
            if session_base:
                _cleanup_login_files(PENDING_SESS_DIR, session_base)


def _mask_phone(phone: str) -> str:
    p = (phone or "").strip()
    if len(p) <= 4:
        return "***"
    return p[:2] + "*" * (len(p) - 4) + p[-2:]


def _status_payload(meta: Dict[str, Any]) -> Dict[str, Any]:
    now = time.time()
    expires_at = float(meta.get("expires_at", now))
    return {
        "ok": True,
        "step": meta.get("step", "code"),
        "login_id": meta.get("login_id"),
        "session": f"{meta.get('session_base')}.session" if meta.get("session_base") else None,
        "phone": _mask_phone(meta.get("phone", "")),
        "expires_in_sec": max(0, int(expires_at - now)),
        "need_password": meta.get("step") == "password",
        "last_error": meta.get("last_error"),
        "message": meta.get("message") or "",
    }


class SessionStartPayload(BaseModel):
    session: str
    phone: str
    force: bool = False


class SessionCodePayload(BaseModel):
    login_id: str
    code: str


class SessionPasswordPayload(BaseModel):
    login_id: str
    password: str


class SessionResendPayload(BaseModel):
    login_id: str


class SessionCancelPayload(BaseModel):
    login_id: str


# ===================== API models =====================
class AddPayload(BaseModel):
    poll_chanel: str
    post_chanel: Optional[str] = ""
    textdelete: Optional[str] = ""
    caption: Optional[str] = ""
    delete: Optional[str] = None

    # compat
    seasion: Optional[str] = None
    poll_seasion: Optional[str] = None

    # mode post
    post_by: Optional[str] = "user"      # user | bot
    post_seasion: Optional[str] = None
    bot_token: Optional[str] = None


class SessionDeletePayload(BaseModel):
    session: str


# ===================== Lifespan =====================
@asynccontextmanager
async def lifespan(app: FastAPI):
    await rescan_sessions()

    tasks = [
        asyncio.create_task(poll_loop(), name="poll_loop"),
        asyncio.create_task(_monitor_sessions(), name="monitor_sessions"),
        asyncio.create_task(_health_loop(), name="health_loop"),
    ]
    try:
        # notify start
        try:
            async with _sessions_lock:
                snap = list(_sessions)
            lines = ["🚀 Server start - trạng thái sessions"]
            if not snap:
                lines.append("• (Không có session nào trong sessions/)")
            else:
                for sw in snap:
                    status_txt = "Live" if (sw.online and sw.client) else "Die"
                    lines.append(f"• {_session_display_name(sw)} ({sw.path.name}) -> {status_txt}")
            await alert("\n".join(lines))
        except Exception:
            pass

        yield
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        async with _sessions_lock:
            for sw in _sessions:
                try:
                    await stop_session(sw)
                except Exception:
                    pass


# ===================== FastAPI =====================
app = FastAPI(
    title="tg-mirror (poll last_id -> repost via post sessions or bot; role split)",
    lifespan=lifespan
)


@app.get("/")
async def root():
    return JSONResponse({
        "ok": True,
        "hint": "POST /add, GET /status, POST /sessions/upload, POST /sessions/delete, "
                "POST /session/start, POST /session/code, POST /session/password, POST /session/resend, "
                "GET /session/status, GET /session/download"
    })


# ===================== /status =====================
@app.get("/status")
async def status(_: Any = Depends(require_bearer)):
    async with _sessions_lock:
        snapshot = list(_sessions)

    poll_sessions, post_sessions = _compute_roles_from_state()
    jobs_map = _state.get("jobs") or {}
    pollers_map = _state.get("pollers") or {}

    # sessions info
    sessions_out = []
    for sw in snapshot:
        sess_file = sw.path.name
        status_txt = "Live" if sw.online and sw.client else "Die"
        name_lower = _session_display_name(sw).lower()

        role = "free"
        if name_lower in poll_sessions:
            role = "poll"
        elif name_lower in post_sessions:
            role = "post"

        recent_items = ((_state.get("recent_by_session") or {}).get(sess_file, []) or [])[:10]

        sessions_out.append({
            "seasion": sess_file,
            "display": _session_display_name(sw),
            "role": role,
            "trang_thai": status_txt,
            "last_check_ts": int(sw.last_check_ts) if sw.last_check_ts else 0,
            "last_error": sw.last_error or "",
            "recent_10": recent_items
        })

    # pollers info
    pollers_out = []
    for poll_ch, meta in pollers_map.items():
        pollers_out.append({
            "poll_chanel": f"@{_norm_ch(poll_ch)}",
            "poll_session_name": meta.get("poll_session_name"),
            "session_index": meta.get("session_index", -1),
            "last_error": meta.get("last_error", ""),
            "created_ts": meta.get("created_ts", ""),
        })

    # jobs info
    jobs_out = []
    for jid, job in jobs_map.items():
        jobs_out.append({
            "job_id": jid,
            "poll_chanel": f"@{_norm_ch(job.get('poll_chanel') or '')}",
            "post_chanel": f"@{_norm_ch(job.get('post_chanel') or '')}",
            "post_by": job.get("post_by"),
            "post_seasion": job.get("post_seasion") or "",
            "bot_key": _bot_key(job.get("bot_token") or "") if (job.get("bot_token") or "") else "",
            "last_ok_id": int(job.get("last_ok_id") or 0),
            "paused_reason": job.get("paused_reason") or "",
            "last_error": job.get("last_error") or "",
            "textdelete": job.get("textdelete") or "",
            "caption": job.get("caption") or "",
        })

    # bots list
    bots_out = []
    for k, items in (_state.get("recent_by_bot") or {}).items():
        bots_out.append({
            "bot_key": k,
            "recent": (items or [])[:10]
        })

    return {
        "ok": True,
        "all_sessions": sessions_out,
        "pollers": pollers_out,
        "jobs": jobs_out,
        "dead_sessions": _state.get("dead_sessions") or {},
        "bots": bots_out
    }


# ===================== /add =====================
@app.post("/add")
async def add(payload: AddPayload, _: Any = Depends(require_bearer)):
    poll_name = _norm_ch(payload.poll_chanel)
    if not poll_name:
        raise HTTPException(400, "poll_chanel is required")

    # delete all jobs for this poll channel
    if payload.delete and str(payload.delete).lower() == "all":
        # remove jobs
        jobs_map = _state.get("jobs") or {}
        to_del = [jid for jid, job in jobs_map.items() if _norm_ch(job.get("poll_chanel") or "") == poll_name]
        for jid in to_del:
            jobs_map.pop(jid, None)
        _state["jobs"] = jobs_map

        # remove poller
        pollers_map = _state.get("pollers") or {}
        existed_poller = poll_name in pollers_map
        pollers_map.pop(poll_name, None)
        _state["pollers"] = pollers_map

        await save_state()
        return {"ok": True, "deleted_jobs": len(to_del), "deleted_poller": existed_poller, "poll_chanel": poll_name}

    post_name = _norm_ch(payload.post_chanel or "")
    if not post_name:
        raise HTTPException(400, "post_chanel is required when not deleting")

    textdelete = (payload.textdelete or "").strip()
    caption = (payload.caption or "").strip()

    post_by = (payload.post_by or "user").lower()
    if post_by not in ("user", "bot"):
        post_by = "user"

    bot_token = (payload.bot_token or "").strip() if post_by == "bot" else ""
    post_seasion = _norm_session_name(payload.post_seasion or "") if post_by == "user" else ""

    # role enforcement:
    poll_sessions, post_sessions = _compute_roles_from_state()

    # choose / set poll session for this poll channel
    poll_req = _norm_session_name(payload.poll_seasion or payload.seasion or "")
    pollers_map = _state.get("pollers") or {}
    poller = pollers_map.get(poll_name)

    if poll_req:
        if poll_req.lower() in post_sessions:
            raise HTTPException(409, f"Session '{poll_req}' is already used as POST session. Not allowed to poll.")
        sw = await find_session_wrap_by_name(poll_req)
        if not sw:
            raise HTTPException(404, f"Poll session not found: {poll_req}")
        await _check_session_health(sw)
        if not sw.online:
            raise HTTPException(503, f"Poll session DIE: {poll_req} ({sw.last_error})")
        poll_session_name = _session_display_name(sw)
        poll_session_index = sw.index
    else:
        # if poller exists, keep its poll session; else pick least loaded
        if poller and poller.get("poll_session_name"):
            poll_session_name = poller.get("poll_session_name")
            sw = await find_session_wrap_by_name(poll_session_name)
            if sw:
                await _check_session_health(sw)
            if not sw or not sw.online:
                await _failover_poller_if_needed(poll_name)
                poller = (_state.get("pollers") or {}).get(poll_name) or {}
                poll_session_name = poller.get("poll_session_name")
            sw = await find_session_wrap_by_name(poll_session_name)
            if not sw or not sw.online:
                raise HTTPException(503, "No online poll session available")
            poll_session_index = sw.index
        else:
            dst = _pick_poll_session_least_loaded(exclude_session_names_lower=post_sessions)
            if not dst:
                raise HTTPException(503, "No online poll session available")
            poll_session_name = _session_display_name(dst)
            poll_session_index = dst.index

    # post role enforcement
    if post_by == "user":
        if not post_seasion:
            raise HTTPException(400, "post_by=user requires post_seasion")
        if post_seasion.lower() in poll_sessions:
            raise HTTPException(409, f"Session '{post_seasion}' is already used as POLL session. Not allowed to post.")
        psw = await find_session_wrap_by_name(post_seasion)
        if not psw:
            raise HTTPException(404, f"Post session not found: {post_seasion}")
        # if die -> allow create job, but it will be pending; as requested.
        await _check_session_health(psw)
    else:
        if not bot_token:
            raise HTTPException(400, "post_by=bot requires bot_token")

    # Ensure poller exists (one per poll channel)
    pollers_map = _state.get("pollers") or {}
    if poll_name not in pollers_map:
        pollers_map[poll_name] = {
            "poll_session_name": poll_session_name,
            "session_index": poll_session_index,
            "chat_id": None,
            "created_ts": _now_str(),
            "last_error": "",
        }
    else:
        # update poller poll session if explicitly chosen
        pollers_map[poll_name]["poll_session_name"] = poll_session_name
        pollers_map[poll_name]["session_index"] = poll_session_index

    _state["pollers"] = pollers_map

    # baseline last_id for the new job cursor (start from latest, not repost old)
    sw = await find_session_wrap_by_name(poll_session_name)
    if not sw or not sw.client:
        raise HTTPException(503, "Poll session not ready")
    await _check_session_health(sw)
    if not sw.online:
        raise HTTPException(503, f"Poll session DIE: {poll_session_name}")

    # poll session joins only source; post session/bot handles dest join separately
    await ensure_join(sw, poll_name)

    baseline_last = await _get_latest_id(sw.client, poll_name)

    # create/update job
    jid = _job_id(poll_name, post_name, post_by, post_seasion, bot_token)
    jobs_map = _state.get("jobs") or {}
    old = jobs_map.get(jid) or {}

    jobs_map[jid] = {
        "job_id": jid,
        "poll_chanel": poll_name,
        "post_chanel": post_name,
        "textdelete": textdelete,
        "caption": caption,
        "post_by": post_by,
        "post_seasion": post_seasion or "",
        "bot_token": bot_token or "",
        # cursor: only advanced when repost success
        "last_ok_id": int(old.get("last_ok_id", baseline_last) if old else baseline_last),
        "last_error": old.get("last_error", ""),
        "paused_reason": old.get("paused_reason", ""),
        "created_ts": old.get("created_ts", _now_str()),
        "updated_ts": _now_str(),
    }
    _state["jobs"] = jobs_map

    await save_state()

    return {
        "ok": True,
        "job_id": jid,
        "poll_chanel": poll_name,
        "post_chanel": post_name,
        "poll_session_name": poll_session_name,
        "poll_session_index": poll_session_index + 1,
        "post_by": post_by,
        "post_seasion": post_seasion or "",
        "bot_token": ("****" + bot_token[-6:]) if bot_token else "",
        "baseline_last_id": int(baseline_last or 0),
        "note": "Cursor (last_ok_id) advances ONLY on successful repost. Post session DIE will pause this job without losing messages."
    }


# ===================== Upload/Delete session =====================
@app.post("/sessions/upload")
async def upload_session(file: UploadFile = File(...), _: Any = Depends(require_bearer)):
    fname = _safe_filename(file.filename)
    if not fname:
        raise HTTPException(400, "Invalid filename")
    if not (fname.endswith(".session") or fname.endswith(".session-journal")):
        raise HTTPException(400, "Only .session or .session-journal files are allowed")

    data = await file.read()
    if not data or len(data) > 10 * 1024 * 1024:
        raise HTTPException(400, "File too large or empty (max ~10MB)")

    dest = SESS_DIR / fname
    dest.write_bytes(data)

    await rescan_sessions()
    return {"ok": True, "saved": f"sessions/{fname}", "total_sessions": len(list_session_files())}


@app.post("/sessions/delete")
async def delete_session(payload: SessionDeletePayload, _: Any = Depends(require_bearer)):
    target = _norm_session_name(payload.session)
    if not target:
        raise HTTPException(400, "session is required")

    sess_file = SESS_DIR / f"{target}.session"
    journal_file = SESS_DIR / f"{target}.session-journal"
    existed = sess_file.exists() or journal_file.exists()

    sw = await find_session_wrap_by_name(target)
    if sw:
        try:
            await stop_session(sw)
        except Exception:
            pass

    try:
        if sess_file.exists():
            sess_file.unlink()
        if journal_file.exists():
            journal_file.unlink()
    except Exception as e:
        raise HTTPException(500, f"Delete session file failed: {e}")

    # Remove recent for that session file
    _state.setdefault("recent_by_session", {})
    _state["recent_by_session"].pop(f"{target}.session", None)

    await save_state()
    await rescan_sessions()

    if existed:
        await alert(f"🗑️ Đã xóa session\nSession: {target}\nTime: {_now_str()}")

    return {"ok": True, "deleted": existed, "session": target, "total_sessions": len(list_session_files())}


# ===================== Session auth endpoints =====================
@app.get("/session/status")
async def session_status(login_id: str, _: Any = Depends(require_bearer)):
    await _gc_pending_logins()
    login_id = (login_id or "").strip()
    if not login_id:
        raise HTTPException(400, "login_id is required")

    async with _pending_login_lock:
        meta = _pending_logins.get(login_id)
    if not meta:
        raise HTTPException(404, "login_id not found or expired")
    return _status_payload(meta)


@app.post("/session/start")
async def session_start(payload: SessionStartPayload, _: Any = Depends(require_bearer)):
    await _gc_pending_logins()

    session_base = _safe_filename(_norm_session_name(payload.session))
    if not session_base or not _SAFE_NAME_RE.match(session_base):
        raise HTTPException(400, "Invalid session name")

    phone = (payload.phone or "").strip()
    if not phone:
        raise HTTPException(400, "phone is required")

    dst_sess, dst_journal = _session_paths_in(SESS_DIR, session_base)
    if (dst_sess.exists() or dst_journal.exists()) and not payload.force:
        raise HTTPException(409, f"Session already exists: {session_base}.session (set force=true to overwrite)")

    async with _pending_login_lock:
        for meta in _pending_logins.values():
            if (meta.get("session_base") or "").lower() == session_base.lower():
                raise HTTPException(409, f"Login already in progress for session: {session_base}")

    _cleanup_login_files(PENDING_SESS_DIR, session_base)

    client = TelegramClient(str((PENDING_SESS_DIR / session_base)), cfg.API_ID, cfg.API_HASH)
    try:
        await client.connect()
        sent = await client.send_code_request(phone)
        code_hash = getattr(sent, "phone_code_hash", None)
    except PhoneNumberInvalidError:
        try:
            await client.disconnect()
        except Exception:
            pass
        _cleanup_login_files(PENDING_SESS_DIR, session_base)
        raise HTTPException(400, "Phone number invalid")
    except FloodWaitError as e:
        try:
            await client.disconnect()
        except Exception:
            pass
        _cleanup_login_files(PENDING_SESS_DIR, session_base)
        raise HTTPException(429, f"FloodWait {int(e.seconds)}s")
    except Exception as e:
        try:
            await client.disconnect()
        except Exception:
            pass
        _cleanup_login_files(PENDING_SESS_DIR, session_base)
        raise HTTPException(500, f"send_code_request failed: {e}")

    login_id = secrets.token_urlsafe(18)
    now = time.time()

    async with _pending_login_lock:
        _pending_logins[login_id] = {
            "login_id": login_id,
            "created_ts": now,
            "expires_at": now + LOGIN_TTL_SEC,
            "session_base": session_base,
            "phone": phone,
            "client": client,
            "code_hash": code_hash,
            "force": bool(payload.force),
            "step": "code",
            "last_error": None,
            "message": "Nhập code Telegram gửi về.",
        }

    return _status_payload(_pending_logins[login_id])


@app.post("/session/resend")
async def session_resend(payload: SessionResendPayload, _: Any = Depends(require_bearer)):
    await _gc_pending_logins()
    login_id = (payload.login_id or "").strip()
    if not login_id:
        raise HTTPException(400, "login_id is required")

    async with _pending_login_lock:
        meta = _pending_logins.get(login_id)
    if not meta:
        raise HTTPException(404, "login_id not found or expired")

    if meta.get("step") not in ("code", "error"):
        return _status_payload(meta)

    client: TelegramClient = meta["client"]
    phone = meta["phone"]
    try:
        sent = await client.send_code_request(phone)
        meta["code_hash"] = getattr(sent, "phone_code_hash", None)
        meta["step"] = "code"
        meta["last_error"] = None
        meta["message"] = "Đã gửi lại code. Nhập code mới."
        meta["expires_at"] = time.time() + LOGIN_TTL_SEC
        async with _pending_login_lock:
            _pending_logins[login_id] = meta
    except FloodWaitError as e:
        meta["step"] = "error"
        meta["last_error"] = f"FloodWait {int(e.seconds)}s"
        meta["message"] = meta["last_error"]
        async with _pending_login_lock:
            _pending_logins[login_id] = meta
        raise HTTPException(429, meta["last_error"])
    except Exception as e:
        meta["step"] = "error"
        meta["last_error"] = f"resend failed: {e}"
        meta["message"] = meta["last_error"]
        async with _pending_login_lock:
            _pending_logins[login_id] = meta
        raise HTTPException(500, meta["last_error"])

    return _status_payload(meta)


@app.post("/session/code")
async def session_code(payload: SessionCodePayload, _: Any = Depends(require_bearer)):
    await _gc_pending_logins()
    login_id = (payload.login_id or "").strip()
    code = (payload.code or "").strip().replace(" ", "")
    if not login_id or not code:
        raise HTTPException(400, "login_id and code are required")

    async with _pending_login_lock:
        meta = _pending_logins.get(login_id)
    if not meta:
        raise HTTPException(404, "login_id not found or expired")

    client: TelegramClient = meta["client"]
    phone = meta["phone"]
    session_base = meta["session_base"]
    code_hash = meta.get("code_hash")

    try:
        if code_hash:
            await client.sign_in(phone=phone, code=code, phone_code_hash=code_hash)
        else:
            await client.sign_in(phone=phone, code=code)

        try:
            await client.disconnect()
        except Exception:
            pass

        _move_login_files(session_base, force=bool(meta.get("force")))

        async with _pending_login_lock:
            _pending_logins.pop(login_id, None)

        await rescan_sessions()
        await alert(f"✅ Login session thành công\nSession: {session_base}.session\nPhone: {_mask_phone(phone)}")
        return {"ok": True, "step": "done", "session": f"{session_base}.session", "need_password": False}

    except SessionPasswordNeededError:
        meta["step"] = "password"
        meta["last_error"] = None
        meta["message"] = "Telegram yêu cầu mật khẩu 2FA."
        meta["expires_at"] = time.time() + LOGIN_TTL_SEC
        async with _pending_login_lock:
            _pending_logins[login_id] = meta
        return _status_payload(meta)

    except PhoneCodeInvalidError:
        meta["step"] = "error"
        meta["last_error"] = "Code invalid"
        meta["message"] = meta["last_error"]
        async with _pending_login_lock:
            _pending_logins[login_id] = meta
        raise HTTPException(400, "Code invalid")

    except PhoneCodeExpiredError:
        meta["step"] = "error"
        meta["last_error"] = "Code expired"
        meta["message"] = meta["last_error"]
        async with _pending_login_lock:
            _pending_logins[login_id] = meta
        raise HTTPException(400, "Code expired")

    except Exception as e:
        meta["step"] = "error"
        meta["last_error"] = f"sign_in failed: {e}"
        meta["message"] = meta["last_error"]
        async with _pending_login_lock:
            _pending_logins[login_id] = meta
        raise HTTPException(500, meta["last_error"])


@app.post("/session/password")
async def session_password(payload: SessionPasswordPayload, _: Any = Depends(require_bearer)):
    await _gc_pending_logins()
    login_id = (payload.login_id or "").strip()
    password = (payload.password or "").strip()
    if not login_id or not password:
        raise HTTPException(400, "login_id and password are required")

    async with _pending_login_lock:
        meta = _pending_logins.get(login_id)
    if not meta:
        raise HTTPException(404, "login_id not found or expired")

    if meta.get("step") != "password":
        return _status_payload(meta)

    client: TelegramClient = meta["client"]
    phone = meta["phone"]
    session_base = meta["session_base"]

    try:
        await client.sign_in(password=password)

        try:
            await client.disconnect()
        except Exception:
            pass

        _move_login_files(session_base, force=bool(meta.get("force")))

        async with _pending_login_lock:
            _pending_logins.pop(login_id, None)

        await rescan_sessions()
        await alert(f"✅ Login session (2FA) thành công\nSession: {session_base}.session\nPhone: {_mask_phone(phone)}")
        return {"ok": True, "step": "done", "session": f"{session_base}.session", "need_password": False}

    except Exception as e:
        meta["step"] = "error"
        meta["last_error"] = f"password sign_in failed: {e}"
        meta["message"] = meta["last_error"]
        meta["expires_at"] = time.time() + LOGIN_TTL_SEC
        async with _pending_login_lock:
            _pending_logins[login_id] = meta
        raise HTTPException(400, meta["last_error"])


@app.post("/session/cancel")
async def session_cancel(payload: SessionCancelPayload, _: Any = Depends(require_bearer)):
    login_id = (payload.login_id or "").strip()
    if not login_id:
        raise HTTPException(400, "login_id is required")

    async with _pending_login_lock:
        meta = _pending_logins.pop(login_id, None)
    if not meta:
        return {"ok": True, "cancelled": False}

    client = meta.get("client")
    session_base = meta.get("session_base")

    try:
        if client:
            await client.disconnect()
    except Exception:
        pass
    if session_base:
        _cleanup_login_files(PENDING_SESS_DIR, session_base)
    return {"ok": True, "cancelled": True}


@app.get("/session/download")
async def session_download(session: str, _: Any = Depends(require_bearer)):
    base = _safe_filename(_norm_session_name(session))
    if not base:
        raise HTTPException(400, "session is required")

    sess = SESS_DIR / f"{base}.session"
    jour = SESS_DIR / f"{base}.session-journal"
    if not sess.exists() and not jour.exists():
        raise HTTPException(404, "session not found")

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as z:
        if sess.exists():
            z.write(sess, arcname=sess.name)
        if jour.exists():
            z.write(jour, arcname=jour.name)
    data = buf.getvalue()

    headers = {
        "content-type": "application/zip",
        "content-disposition": f'attachment; filename="{base}.zip"',
        "cache-control": "no-store",
    }
    return Response(content=data, headers=headers)


# ===================== Run =====================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host=cfg.BIND_HOST, port=cfg.BIND_PORT, reload=False, log_level="info")
