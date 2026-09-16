#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════╗
║            📂   P D F   M A N A G E R   🤖        ║
║         مدیر حرفه‌ای فایل‌های PDF در تلگرام          ║
╠══════════════════════════════════════════════════╣
║  ⚡ لینک PDF تا ۲ گیگابایت + اسم دلخواه            ║
║  📩 فوروارد PDF ← تغییر اسم ← ارسال به ادمین        ║
║  🔗 لینک پیام تلگرام (t.me) ← کپی فوری بدون آپلود   ║
║  🗂 آرشیو کامل ← ارسال مجدد همه فایل‌ها با یک دکمه    ║
║  📊 آمار دقیق + دکمه استارت همیشه کنار فیلد چت      ║
╚══════════════════════════════════════════════════╝

نصب:   pip install -U telethon cryptg aiohttp
اجرا:  python pdf_manager.py
تنظیم: متغیرهای محیطی API_ID, API_HASH, BOT_TOKEN, ADMIN_CHAT_ID
"""

import os
import re
import html
import time
import shutil
import sqlite3
import logging
import tempfile
import asyncio
from datetime import datetime
from urllib.parse import urlparse, unquote

import aiohttp
from telethon import TelegramClient, events, Button
from telethon.network.connection.tcpabridged import ConnectionTcpAbridged
from telethon.tl.types import (DocumentAttributeFilename, BotCommand,
                               BotCommandScopeDefault)
from telethon.tl.functions.help import SetBotCommandsRequest
from telethon.errors import FloodWaitError

# ═══════════════════ تنظیمات اصلی ═══════════════════
API_ID        = int(os.getenv("API_ID", "0"))
API_HASH      = os.getenv("API_HASH", "")
BOT_TOKEN     = os.getenv("BOT_TOKEN", "")
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0"))

MAX_SIZE       = 2000 * 1024 * 1024   # 🚀 سقف تلگرام برای ربات‌ها (≈۲ گیگابایت)
CHUNK_SIZE     = 512 * 1024
PROGRESS_EVERY = 3.0
MAX_CONCURRENT = 3                    # حداکثر آپلود همزمان (سرعت کم نشه)
MAX_RETRIES    = 3                    # تلاش مجدد دانلود
STATE_TTL      = 900                  # مهلت جواب دادن به سوال اسم (ثانیه)
DB_FILE        = "pdf_manager.db"
ALLOWED_FILE   = "allowed_users.json"
START_TS       = time.time()
APP_NAME       = "📂 PDF Manager"
LINE           = "━━━━━━━━━━━━━━━━━━━━"
# ════════════════════════════════════════════════════

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("pdf-manager")

URL_RE     = re.compile(r"https?://\S+", re.IGNORECASE)
TG_LINK_RE = re.compile(r"^https?://(www\.)?(t\.me|telegram\.me)/", re.IGNORECASE)

SOURCE_LABELS = {
    "url":     "🌐 لینک وب",
    "forward": "📩 فوروارد تلگرام",
    "tme":     "🔗 پیام تلگرام",
}

bot = TelegramClient(
    "pdf_manager_session",
    API_ID,
    API_HASH,
    connection=ConnectionTcpAbridged,
    connection_retries=5,
    retry_delay=1,
    timeout=30,
    auto_reconnect=True,
)
bot.parse_mode = "html"
bot.flood_sleep_threshold = 60

upload_semaphore = asyncio.Semaphore(MAX_CONCURRENT)
user_states = {}          # {user_id: state}
allowed_users = set()     # کاربران مجاز


# ═══════════════════ پایگاه داده (آرشیو + آمار) ═══════════════════
_db = sqlite3.connect(DB_FILE, check_same_thread=False)
_db.row_factory = sqlite3.Row


def db_init():
    _db.execute("""
        CREATE TABLE IF NOT EXISTS files (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_msg_id  INTEGER NOT NULL,
            file_name     TEXT    NOT NULL,
            file_size     INTEGER NOT NULL,
            source_type   TEXT,
            source_detail TEXT,
            sender_id     INTEGER,
            sender_name   TEXT,
            sent_at       TEXT
        )""")
    _db.commit()


def add_file(admin_msg_id, name, size, source_type, detail, sender) -> int:
    sname = "—"
    if sender:
        sname = getattr(sender, "first_name", None) or "—"
        if getattr(sender, "username", None):
            sname += f" (@{sender.username})"
    cur = _db.execute(
        "INSERT INTO files (admin_msg_id,file_name,file_size,source_type,"
        "source_detail,sender_id,sender_name,sent_at) VALUES (?,?,?,?,?,?,?,?)",
        (admin_msg_id, name, size, source_type, detail,
         getattr(sender, "id", 0) if sender else 0, sname,
         datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
    )
    _db.commit()
    return cur.lastrowid


def get_file(fid):
    return _db.execute("SELECT * FROM files WHERE id=?", (fid,)).fetchone()


def get_all_files():
    return _db.execute("SELECT * FROM files ORDER BY id DESC").fetchall()


def update_file(fid, new_name, new_msg_id):
    _db.execute("UPDATE files SET file_name=?, admin_msg_id=? WHERE id=?",
                (new_name, new_msg_id, fid))
    _db.commit()


def wipe_files():
    _db.execute("DELETE FROM files")
    _db.commit()


def get_stats():
    total = _db.execute(
        "SELECT COUNT(*) n, COALESCE(SUM(file_size),0) s FROM files").fetchone()
    today = _db.execute(
        "SELECT COUNT(*) n, COALESCE(SUM(file_size),0) s FROM files "
        "WHERE substr(sent_at,1,10)=?",
        (datetime.now().strftime("%Y-%m-%d"),)).fetchone()
    big = _db.execute(
        "SELECT file_name,file_size FROM files ORDER BY file_size DESC LIMIT 1").fetchone()
    users = _db.execute("SELECT COUNT(DISTINCT sender_id) FROM files").fetchone()[0]
    top = _db.execute(
        "SELECT sender_name,COUNT(*) n,SUM(file_size) s FROM files "
        "GROUP BY sender_id ORDER BY n DESC LIMIT 5").fetchall()
    src = dict(_db.execute(
        "SELECT source_type,COUNT(*) FROM files GROUP BY source_type").fetchall())
    last = _db.execute(
        "SELECT file_name,sent_at FROM files ORDER BY id DESC LIMIT 1").fetchone()
    n = total["n"]
    return {
        "total_n": n, "total_s": total["s"],
        "avg": total["s"] / n if n else 0,
        "today_n": today["n"], "today_s": today["s"],
        "big": big, "users": users, "top": top, "src": src, "last": last,
    }


# ═══════════════════ کاربران مجاز ═══════════════════
def load_allowed():
    global allowed_users
    try:
        if os.path.exists(ALLOWED_FILE):
            with open(ALLOWED_FILE, "r", encoding="utf-8") as f:
                allowed_users = set(json_safe(f.read()))
            log.info("کاربران مجاز بارگذاری شد: %s", allowed_users)
    except Exception as e:
        log.warning("خطا در بارگذاری کاربران مجاز: %s", e)
        allowed_users = set()


def json_safe(raw):
    import json
    return json.loads(raw)


def save_allowed():
    import json
    try:
        with open(ALLOWED_FILE, "w", encoding="utf-8") as f:
            json.dump(list(allowed_users), f)
    except Exception as e:
        log.error("خطا در ذخیره کاربران مجاز: %s", e)


def is_allowed(user_id: int) -> bool:
    if user_id == ADMIN_CHAT_ID:
        return True
    if not allowed_users:
        return True
    return user_id in allowed_users


# ═══════════════════ ابزارها ═══════════════════
def human_size(n: float) -> str:
    for unit in ("بایت", "KB", "MB", "GB"):
        if n < 1024:
            return f"{int(n)} بایت" if unit == "بایت" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def fmt_eta(sec: float) -> str:
    if sec <= 0 or sec > 86400:
        return "—"
    m, s = divmod(int(sec), 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}س {m}د"
    if m:
        return f"{m}د {s}ث"
    return f"{s} ثانیه"


def fmt_dur(sec: float) -> str:
    m, s = divmod(int(sec), 60)
    h, m = divmod(m, 60)
    d, h = divmod(h, 24)
    parts = []
    if d: parts.append(f"{d} روز")
    if h: parts.append(f"{h} ساعت")
    if m: parts.append(f"{m} دقیقه")
    if not parts: parts.append(f"{s} ثانیه")
    return " و ".join(parts)


def make_bar(pct: int, length: int = 16) -> str:
    filled = round(length * pct / 100)
    return "█" * filled + "░" * (length - filled)


def sanitize(name: str) -> str:
    name = re.sub(r'[\\/:*?"<>|\r\n]+', "_", str(name)).strip()
    if not name.lower().endswith(".pdf"):
        name += ".pdf"
    return name or "document.pdf"


def short(s: str, n: int = 60) -> str:
    return s if len(s) <= n else s[:n] + "…"


async def safe_edit(msg, text: str, **kw):
    try:
        await msg.edit(text, **kw)
    except Exception:
        pass


async def respond(event, text: str, **kw):
    """پاسخ‌دهی که هم برای Message کار می‌کند هم CallbackQuery"""
    try:
        return await event.respond(text, **kw)
    except Exception:
        return await bot.send_message(event.chat_id, text, **kw)


def get_filename_from_url(url: str, content_disposition: str | None = None) -> str:
    if content_disposition:
        m = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^";]+)"?', content_disposition, re.I)
        if m:
            name = unquote(m.group(1)).strip()
            if name:
                return name if name.lower().endswith(".pdf") else name + ".pdf"
    name = unquote(os.path.basename(urlparse(url).path)) or "document"
    return name if name.lower().endswith(".pdf") else name + ".pdf"


def doc_filename(doc) -> str:
    for a in doc.attributes:
        if isinstance(a, DocumentAttributeFilename) and a.file_name:
            return a.file_name
    mime = (doc.mime_type or "").lower()
    ext = ".pdf" if "pdf" in mime else ".bin"
    return f"file_{doc.id}{ext}"


def is_pdf_doc(doc, name: str) -> bool:
    mime = (doc.mime_type or "").lower()
    return mime == "application/pdf" or name.lower().endswith(".pdf")


def build_caption(name: str, size: int, sender, source_line: str, detail: str = "") -> str:
    uname = f"@{sender.username}" if sender and getattr(sender, "username", None) else "—"
    first = html.escape(getattr(sender, "first_name", None) or "ناشناس") if sender else "ناشناس"
    lines = [
        "📥 <b>فایل جدید دریافت شد</b>",
        LINE,
        f"🗂 <b>نام فایل:</b> <code>{html.escape(name)}</code>",
        f"📦 <b>حجم:</b> {human_size(size)}",
        source_line,
    ]
    if detail:
        lines.append(f"🔗 <b>مشخصات منبع:</b> <code>{html.escape(short(detail, 60))}</code>")
    lines += [
        LINE,
        f"👤 <b>ارسال‌کننده:</b> {first} ({uname})",
        f"🆔 <b>آیدی:</b> <code>{sender.id if sender else '—'}</code>",
        f"🕒 <b>زمان:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
    ]
    return "\n".join(lines)


def done_text(name: str, size: int, fid: int, took: float | None = None) -> str:
    t = f"   |   ⏱ {fmt_dur(took)}" if took else ""
    return (
        f"🎉 {LINE}\n"
        f"✅ <b>با موفقیت برای ادمین ارسال شد!</b>\n{LINE}\n"
        f"🗂 {html.escape(name)}\n"
        f"📦 {human_size(size)}{t}\n"
        f"🗃 شماره در آرشیو: <b>#{fid}</b>"
    )


# ═══════════════════ نوار پیشرفت ═══════════════════
async def edit_progress(status, state, title, done: int, total: int):
    now = time.monotonic()
    start = state.get("start") or now
    speed = done / max(now - start, 0.001)
    if total > 0:
        pct = min(int(done * 100 / total), 100)
        if now - state["t"] < PROGRESS_EVERY and pct != 100:
            return
        if pct == state.get("p") and pct != 100:
            return
        state["t"], state["p"] = now, pct
        eta = (total - done) / speed if speed > 0 else 0
        text = (
            f"{title}\n\n"
            f"{make_bar(pct)}  <b>{pct}%</b>\n"
            f"📦 {human_size(done)} از {human_size(total)}\n"
            f"🚀 {human_size(speed)}/ثانیه   |   ⏳ {fmt_eta(eta)}"
        )
    else:
        if now - state["t"] < PROGRESS_EVERY and done != 0:
            return
        state["t"] = now
        text = f"{title}\n\n📦 {human_size(done)}\n🚀 {human_size(speed)}/ثانیه"
    await safe_edit(status, text)


# ═══════════════════ دانلود از وب ═══════════════════
async def _download_once(url, dest, status, headers, timeout):
    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        async with session.get(url) as resp:
            if resp.status != 200:
                raise RuntimeError(f"سرور کد {resp.status} برگرداند (لینک منقضی یا نامعتبر؟)")
            cl = resp.headers.get("Content-Length", "")
            total = int(cl) if cl.isdigit() else 0
            if total > MAX_SIZE:
                raise RuntimeError(f"حجم فایل ({human_size(total)}) از سقف مجاز ({human_size(MAX_SIZE)}) بیشتر است ❗")
            need = (total or MAX_SIZE) + 64 * 1024 * 1024
            if shutil.disk_usage(os.path.dirname(dest) or ".").free < need:
                raise RuntimeError("فضای دیسک سرور کافی نیست ❗")
            state = {"t": 0.0, "p": -1, "start": time.monotonic()}
            done = 0
            first = True
            with open(dest, "wb") as f:
                async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                    if first:
                        if b"%PDF" not in chunk[:2048]:
                            raise RuntimeError("محتوای این لینک یک فایل PDF معتبر نیست ❗")
                        first = False
                    f.write(chunk)
                    done += len(chunk)
                    if done > MAX_SIZE:
                        raise RuntimeError(f"حجم فایل از سقف مجاز ({human_size(MAX_SIZE)}) بیشتر شد ❗")
                    await edit_progress(status, state, "⬇️ <b>در حال دانلود</b>", done, total)
            if first:
                raise RuntimeError("فایل خالی است ❗")
            return get_filename_from_url(url, resp.headers.get("Content-Disposition")), done


async def download_pdf(url, dest, status):
    timeout = aiohttp.ClientTimeout(total=None, connect=30, sock_read=600)
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                             "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return await _download_once(url, dest, status, headers, timeout)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            last_err = e
            if attempt < MAX_RETRIES:
                await safe_edit(status, f"⚠️ خطای شبکه! تلاش مجدد <b>{attempt + 1}</b> از {MAX_RETRIES}...")
                await asyncio.sleep(2 * attempt)
    raise RuntimeError(f"دانلود پس از {MAX_RETRIES} تلاش ناموفق بود: {last_err}")


# ═══════════════════ ارسال به ادمین ═══════════════════
async def upload_path_to_admin(status, path, name, size, source_type, detail, sender):
    """آپلود واقعی (با نوار پیشرفت) + ثبت در آرشیو"""
    label = SOURCE_LABELS.get(source_type, "—")
    up = {"t": 0.0, "p": -1, "start": time.monotonic()}
    t0 = time.monotonic()

    async def cb(cur, tot):
        await edit_progress(status, up, "⬆️ <b>در حال ارسال به ادمین</b>", cur, tot)

    async with upload_semaphore:
        sent = await bot.send_file(
            ADMIN_CHAT_ID, path,
            caption=build_caption(name, size, sender, f"📡 <b>منبع:</b> {label}", detail),
            force_document=True,
            attributes=[DocumentAttributeFilename(name)],
            progress_callback=cb,
            part_size_kb=512,
        )
    fid = add_file(sent.id, name, size, source_type, detail, sender)
    return sent, time.monotonic() - t0, fid


async def copy_media_to_admin(status, media, name, size, source_type, detail, sender):
    """⚡ کپی فوری — فایل دوباره آپلود نمی‌شود، مستقیم از سرور تلگرام کپی می‌شود"""
    label = SOURCE_LABELS.get(source_type, "—")
    async with upload_semaphore:
        sent = await bot.send_file(
            ADMIN_CHAT_ID, media,
            caption=build_caption(name, size, sender,
                                  f"📡 <b>منبع:</b> {label}  ⚡ <i>کپی فوری</i>", detail),
            force_document=True,
        )
    fid = add_file(sent.id, name, size, source_type, detail, sender)
    return sent, fid


# ═══════════════════ پردازش لینک وب ═══════════════════
async def process_url(event, url: str, custom_name: str | None = None):
    status = await respond(event, "🔎 <b>در حال بررسی لینک...</b>")
    tmpdir = tempfile.mkdtemp(prefix="pdfman_")
    try:
        fd, raw_path = tempfile.mkstemp(dir=tmpdir, suffix=".pdf")
        os.close(fd)
        orig_name, size = await download_pdf(url, raw_path, status)
        final_name = sanitize(custom_name) if custom_name else sanitize(orig_name)
        final_path = os.path.join(tmpdir, final_name)
        os.rename(raw_path, final_path)

        await safe_edit(status, "✅ <b>دانلود کامل شد!</b>\n⬆️ در صف ارسال به ادمین...")
        sender = await event.get_sender()
        sent, took, fid = await upload_path_to_admin(
            status, final_path, final_name, size, "url", url, sender)
        await safe_edit(status, done_text(final_name, size, fid, took))
        log.info("✅ #%s %s (%s)", fid, final_name, human_size(size))
    except Exception as e:
        await safe_edit(status, f"❌ <b>خطا:</b>\n{html.escape(str(e))}")
        log.error("Error: %s", e)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ═══════════════════ پردازش فوروارد ═══════════════════
async def process_forward_copy(event, st):
    status = await respond(event, "⚡ <b>کپی فوری (بدون دانلود و آپلود مجدد)...</b>")
    try:
        msg = await bot.get_messages(st["chat_id"], ids=st["msg_id"])
        if not msg or not msg.document:
            raise RuntimeError("پیام فورواردشده دیگر در دسترس نیست.")
        sender = await event.get_sender()
        sent, fid = await copy_media_to_admin(
            status, msg.media, st["name"], st["size"],
            "forward", "فوروارد از چت کاربر", sender)
        await safe_edit(status, done_text(st["name"], st["size"], fid))
    except Exception as e:
        await safe_edit(status, f"❌ <b>خطا:</b>\n{html.escape(str(e))}")
        log.error("Error: %s", e)


async def process_forward_rename(event, st, new_name: str):
    status = await respond(event, "📩 <b>در حال دریافت فایل از پیام فورواردشده...</b>")
    tmpdir = tempfile.mkdtemp(prefix="pdfman_")
    try:
        msg = await bot.get_messages(st["chat_id"], ids=st["msg_id"])
        if not msg or not msg.document:
            raise RuntimeError("پیام فورواردشده دیگر در دسترس نیست.")
        raw = os.path.join(tmpdir, "raw.bin")
        dl = {"t": 0.0, "p": -1, "start": time.monotonic()}

        async def dcb(cur, tot):
            await edit_progress(status, dl, "⬇️ <b>در حال دریافت از تلگرام</b>", cur, tot)

        await bot.download_media(msg, file=raw, progress_callback=dcb)

        final_name = sanitize(new_name)
        final_path = os.path.join(tmpdir, final_name)
        os.rename(raw, final_path)
        size = os.path.getsize(final_path)

        sender = await event.get_sender()
        sent, took, fid = await upload_path_to_admin(
            status, final_path, final_name, size, "forward", "فوروارد + تغییر نام", sender)
        await safe_edit(status, done_text(final_name, size, fid, took))
    except Exception as e:
        await safe_edit(status, f"❌ <b>خطا:</b>\n{html.escape(str(e))}")
        log.error("Error: %s", e)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ═══════════════════ پردازش لینک تلگرامی ═══════════════════
def parse_tg_link(url: str):
    p = urlparse(url)
    parts = [x for x in p.path.split("/") if x]
    if not parts:
        return None
    if parts[0] == "c":                                # لینک خصوصی
        if len(parts) >= 3 and parts[1].isdigit():
            ids = [int(x) for x in parts[2:] if x.isdigit()][:10]
            if ids:
                return ("private", int("-100" + parts[1]), ids)
        return None
    if parts[0] in ("joinchat", "+"):                  # لینک دعوت — پشتیبانی نمی‌شود
        return None
    ids = [int(x) for x in parts[1:] if x.isdigit()][:10]   # لینک عمومی
    if ids:
        return ("public", parts[0], ids)
    return None


async def process_tg(event, st, custom_name: str | None = None):
    status = await respond(event, "🔗 <b>در حال دریافت پیام از تلگرام...</b>")
    try:
        msgs = await bot.get_messages(st["peer"], ids=st["ids"])
        docs = [m for m in msgs if m and m.document]
        if not docs:
            raise RuntimeError("در این پیام فایلی پیدا نشد.")
        sender = await event.get_sender()
        total = sum(m.document.size for m in docs)
        t0 = time.monotonic()
        done_names = []

        for i, m in enumerate(docs):
            doc = m.document
            if doc.size > MAX_SIZE:
                await safe_edit(status, f"⚠️ فایل {i + 1} بزرگ‌تر از سقف مجاز است؛ رد شد.")
                continue
            orig = doc_filename(doc)
            name = sanitize(custom_name) if (custom_name and i == 0) else sanitize(orig)

            if custom_name and i == 0:
                # تغییر نام → نیاز به دانلود + آپلود مجدد
                tmpdir = tempfile.mkdtemp(prefix="pdfman_")
                try:
                    raw = os.path.join(tmpdir, "raw.bin")
                    dl = {"t": 0.0, "p": -1, "start": time.monotonic()}

                    async def dcb(cur, tot):
                        await edit_progress(status, dl,
                                            f"⬇️ <b>دریافت فایل {i + 1} از {len(docs)}</b>", cur, tot)

                    await bot.download_media(m, file=raw, progress_callback=dcb)
                    fpath = os.path.join(tmpdir, name)
                    os.rename(raw, fpath)
                    sent, took, fid = await upload_path_to_admin(
                        status, fpath, name, doc.size, "tme", st.get("detail", ""), sender)
                finally:
                    shutil.rmtree(tmpdir, ignore_errors=True)
            else:
                await safe_edit(status, f"⚡ <b>کپی فوری فایل {i + 1} از {len(docs)}...</b>")
                sent, fid = await copy_media_to_admin(
                    status, m.media, name, doc.size, "tme", st.get("detail", ""), sender)
            done_names.append(f"#{fid} — {name}")

        took = fmt_dur(time.monotonic() - t0)
        listing = "\n".join(f"🗂 {html.escape(n)}" for n in done_names[:5])
        more = f"\n… و {len(done_names) - 5} فایل دیگر" if len(done_names) > 5 else ""
        await safe_edit(status,
                        f"🎉 {LINE}\n✅ <b>{len(done_names)} فایل ارسال شد!</b>\n{LINE}\n"
                        f"{listing}{more}\n{LINE}\n"
                        f"📦 مجموع: {human_size(total)}   |   ⏱ {took}")
    except Exception as e:
        await safe_edit(status, f"❌ <b>خطا:</b>\n{html.escape(str(e))}")
        log.error("Error: %s", e)


# ═══════════════════ تغییر نام فایل آرشیو ═══════════════════
async def process_stored_rename(event, st, new_name: str):
    rec = get_file(st["fid"])
    status = await respond(event, "🔎 <b>در حال آماده‌سازی تغییر نام...</b>")
    tmpdir = tempfile.mkdtemp(prefix="pdfman_")
    try:
        if not rec:
            raise RuntimeError("این فایل در آرشیو پیدا نشد.")
        msg = await bot.get_messages(ADMIN_CHAT_ID, ids=rec["admin_msg_id"])
        if not msg or not msg.document:
            raise RuntimeError("پیام اصلی فایل در چت ادمین پیدا نشد.")
        raw = os.path.join(tmpdir, "raw.bin")
        dl = {"t": 0.0, "p": -1, "start": time.monotonic()}

        async def dcb(cur, tot):
            await edit_progress(status, dl, "⬇️ <b>در حال دریافت فایل اصلی</b>", cur, tot)

        await bot.download_media(msg, file=raw, progress_callback=dcb)

        final_name = sanitize(new_name)
        fpath = os.path.join(tmpdir, final_name)
        os.rename(raw, fpath)
        size = os.path.getsize(fpath)

        sender = await event.get_sender()
        sent, took, _fid = await upload_path_to_admin(
            status, fpath, final_name, size, rec["source_type"], "تغییر نام از آرشیو", sender)
        update_file(rec["id"], final_name, sent.id)
        try:
            await msg.delete()   # پیام قدیمی حذف شود که فایل تکراری نماند
        except Exception:
            pass
        await safe_edit(status, done_text(final_name, size, rec["id"], took))
    except Exception as e:
        await safe_edit(status, f"❌ <b>خطا:</b>\n{html.escape(str(e))}")
        log.error("Error: %s", e)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ═══════════════════ ارسال مجدد از آرشیو ═══════════════════
async def resend_one(event, rec):
    msg = await bot.get_messages(ADMIN_CHAT_ID, ids=rec["admin_msg_id"])
    if not msg or not msg.document:
        raise RuntimeError("پیام فایل در چت ادمین پیدا نشد.")
    await bot.send_file(
        ADMIN_CHAT_ID, msg.media, force_document=True,
        caption=(f"📤 <b>ارسال مجدد از آرشیو {APP_NAME}</b>\n"
                 f"🗂 <code>{html.escape(rec['file_name'])}</code>\n"
                 f"📦 {human_size(rec['file_size'])}   |   🗃 #{rec['id']}"),
    )


async def send_all_files(event):
    rows = list(reversed(get_all_files()))   # از قدیمی به جدید
    if not rows:
        await respond(event, "📭 آرشیو خالی است!")
        return
    status = await respond(event, f"📤 <b>در حال ارسال 0 از {len(rows)} فایل...</b>")
    ok = fail = 0
    t0 = time.monotonic()
    for i, rec in enumerate(rows, 1):
        try:
            msg = await bot.get_messages(ADMIN_CHAT_ID, ids=rec["admin_msg_id"])
            if msg and msg.document:
                try:
                    await bot.send_file(ADMIN_CHAT_ID, msg.media, force_document=True)
                    ok += 1
                except FloodWaitError as e:
                    await asyncio.sleep(e.seconds + 2)
                    await bot.send_file(ADMIN_CHAT_ID, msg.media, force_document=True)
                    ok += 1
            else:
                fail += 1
        except Exception:
            fail += 1
        if i % 3 == 0 or i == len(rows):
            await safe_edit(status, f"📤 <b>در حال ارسال {i} از {len(rows)} فایل...</b>\n"
                                    f"✅ موفق: {ok}   |   ⚠️ ناموفق: {fail}")
        await asyncio.sleep(0.6)   # جلوگیری از فلود
    try:
        await status.edit(
            f"🎉 {LINE}\n✅ <b>ارسال همه فایل‌ها تمام شد!</b>\n{LINE}\n"
            f"✅ موفق: <b>{ok}</b>\n⚠️ ناموفق: <b>{fail}</b>\n"
            f"⏱ مدت: {fmt_dur(time.monotonic() - t0)}",
            buttons=[[Button.inline("🔙 بازگشت به منو", b"menu")]])
    except Exception:
        pass


# ═══════════════════ ظاهر و منوها ═══════════════════
def main_menu_text():
    s = get_stats()
    return (
        f"✨{LINE}✨\n"
        f"      📂 <b>PDF  MANAGER</b> 🤖\n"
        f"   مدیر حرفه‌ای فایل‌های PDF شما\n"
        f"✨{LINE}✨\n\n"
        f"🗂 فایل‌های آرشیو: <b>{s['total_n']}</b>\n"
        f"📦 مجموع حجم: <b>{human_size(s['total_s'])}</b>\n"
        f"📅 امروز: <b>{s['today_n']}</b> فایل ({human_size(s['today_s'])})\n\n"
        f"👇 <b>یک گزینه انتخاب کن:</b>"
    )


def main_menu_kb(is_admin: bool):
    kb = [
        [Button.inline("📊 آمار", b"stats"), Button.inline("🗂 آرشیو فایل‌ها", b"files")],
        [Button.inline("📤 ارسال همه فایل‌ها", b"sendall")],
        [Button.inline("❓ راهنما", b"help"), Button.inline("🆔 آیدی من", b"myid")],
    ]
    if is_admin:
        kb.append([Button.inline("🗑 پاک کردن آرشیو", b"wipe")])
    return kb


def help_text():
    return (
        f"❓ <b>راهنمای {APP_NAME}</b>\n{LINE}\n"
        "🌐 <b>ارسال با لینک وب:</b>\n"
        "     لینک مستقیم PDF را بفرست (تا ۲ گیگابایت)\n"
        "     ← می‌پرسم اسم دلخواه می‌خوای یا نه\n\n"
        "📩 <b>ارسال با فوروارد:</b>\n"
        "     فایل PDF را فوروارد کن\n"
        "     ← می‌پرسم اسمش رو عوض کنی یا نه، بعد می‌فرستم\n\n"
        "🔗 <b>ارسال با لینک تلگرام:</b>\n"
        "     لینک پیام (t.me/name/123 یا t.me/c/.../123) را بفرست\n"
        "     ← کپی <b>فوری بدون آپلود</b>، حتی با تغییر اسم!\n\n"
        "🗂 <b>آرشیو:</b>\n"
        "     همه فایل‌ها با اسم و اطلاعات ذخیره می‌شوند\n"
        "     ← با یک دکمه همه را دوباره بفرست\n\n"
        f"📦 حداکثر حجم: <b>{human_size(MAX_SIZE)}</b>\n"
        "⚡ چند لینک همزمان هم پشتیبانی می‌شود\n\n"
        "🛠 <b>دستورات:</b>\n"
        "/stats — آمار  |  /files — آرشیو  |  /sendall — ارسال همه\n"
        "/id — آیدی من  |  /cancel — لغو عملیات"
    )


def stats_text():
    s = get_stats()
    medals = ["🥇", "🥈", "🥉", "🏅", "🎖"]
    top = "\n".join(
        f"{medals[i]} {html.escape(short(r['sender_name'] or '—', 20))} — "
        f"<b>{r['n']}</b> فایل ({human_size(r['s'] or 0)})"
        for i, r in enumerate(s["top"])) or "—"
    src = s["src"]
    big = (f"<code>{html.escape(short(s['big']['file_name'], 30))}</code> "
           f"({human_size(s['big']['file_size'])})") if s["big"] else "—"
    last = (f"<code>{html.escape(short(s['last']['file_name'], 30))}</code>\n"
            f"     🕒 {s['last']['sent_at']}") if s["last"] else "—"
    return (
        f"📊 <b>آمار دقیق {APP_NAME}</b>\n{LINE}\n"
        f"🗂 کل فایل‌ها: <b>{s['total_n']}</b>\n"
        f"📦 کل حجم: <b>{human_size(s['total_s'])}</b>\n"
        f"📈 میانگین حجم: {human_size(s['avg'])}\n"
        f"🔥 بزرگ‌ترین فایل: {big}\n{LINE}\n"
        f"📅 امروز: <b>{s['today_n']}</b> فایل ({human_size(s['today_s'])})\n"
        f"👥 کاربران فعال: <b>{s['users']}</b>\n{LINE}\n"
        f"📡 <b>منابع:</b>\n"
        f"     🌐 لینک وب: {src.get('url', 0)}\n"
        f"     📩 فوروارد: {src.get('forward', 0)}\n"
        f"     🔗 پیام تلگرام: {src.get('tme', 0)}\n{LINE}\n"
        f"🏆 <b>برترین ارسال‌کننده‌ها:</b>\n{top}\n{LINE}\n"
        f"🕐 آخرین فایل: {last}\n"
        f"⏱ مدت فعالیت ربات: <b>{fmt_dur(time.time() - START_TS)}</b>"
    )


async def show_files_view(event, edit=False):
    rows = get_all_files()
    if not rows:
        text = (f"🗂 <b>آرشیو {APP_NAME}</b>\n{LINE}\n"
                "📭 هنوز فایلی در آرشیو ثبت نشده است.")
        kb = [[Button.inline("🔙 بازگشت به منو", b"menu")]]
    else:
        total_s = sum(r["file_size"] for r in rows)
        recent = rows[:10]
        lines = [f"🗂 <b>آرشیو {APP_NAME}</b>", LINE,
                 f"📦 تعداد کل: <b>{len(rows)}</b>   |   حجم: <b>{human_size(total_s)}</b>",
                 LINE, "🕐 <b>۱۰ فایل آخر:</b>"]
        for i, r in enumerate(recent, 1):
            lines.append(f"{i}. <code>{html.escape(short(r['file_name'], 28))}</code> "
                         f"— {human_size(r['file_size'])}")
        text = "\n".join(lines)
        kb = []
        for r in recent[:8]:
            kb.append([Button.inline(f"📤 {short(r['file_name'], 20)}", f"re:{r['id']}".encode()),
                       Button.inline("✏️", f"rn:{r['id']}".encode())])
        kb.append([Button.inline("📤 ارسال همه فایل‌ها", b"sendall"),
                   Button.inline("🔙 بازگشت", b"menu")])
    if edit:
        try:
            await event.edit(text, buttons=kb)
            return
        except Exception:
            pass
    await respond(event, text, buttons=kb)


REPLY_KB = [[Button.text("🏠 منو", resize=True), Button.text("📊 آمار", resize=True)],
            [Button.text("🗂 آرشیو", resize=True), Button.text("❓ راهنما", resize=True)],
            [Button.text("🆔 آیدی من", resize=True)]]


# ═══════════════════ مدیریت حالت اسم ═══════════════════
CANCEL_WORDS = ("خیر", "نه", "no", "n", "-")


async def handle_state_text(event, st, text: str):
    uid = event.sender_id
    action = st.get("action")
    user_states.pop(uid, None)

    if text.strip().lower() in CANCEL_WORDS:
        if action == "url_name":
            await event.reply("👍 با <b>اسم اصلی</b> ادامه می‌دهم...")
            asyncio.create_task(process_url(event, st["url"], None))
        elif action == "fwd_name":
            await event.reply("👍 با <b>اسم اصلی</b> ادامه می‌دهم...")
            asyncio.create_task(process_forward_copy(event, st))
        elif action == "tg_name":
            await event.reply("👍 با <b>اسم اصلی</b> ادامه می‌دهم...")
            asyncio.create_task(process_tg(event, st, None))
        else:
            await event.reply("🚫 <b>تغییر نام لغو شد.</b>")
        return

    if action == "url_name":
        await event.reply(f"✅ اسم انتخابی: <b>{html.escape(text)}</b>\n🚀 شروع پردازش...")
        asyncio.create_task(process_url(event, st["url"], text))
    elif action == "fwd_name":
        await event.reply(f"✅ اسم جدید: <b>{html.escape(text)}</b>\n🚀 شروع دریافت و ارسال...")
        asyncio.create_task(process_forward_rename(event, st, text))
    elif action == "tg_name":
        await event.reply(f"✅ اسم جدید: <b>{html.escape(text)}</b>\n🚀 شروع دریافت و ارسال...")
        asyncio.create_task(process_tg(event, st, text))
    elif action == "stored_name":
        await event.reply(f"✅ اسم جدید: <b>{html.escape(text)}</b>\n🚀 شروع تغییر نام فایل آرشیو...")
        asyncio.create_task(process_stored_rename(event, st, text))


# ═══════════════════ هندلر دستورات ═══════════════════
@bot.on(events.NewMessage(pattern=r"^/(start|menu|help|id|cancel|stats|files|sendall|wipe|panel|add|remove|list|allowall)(?:\s+(\S+))?"))
async def commands_handler(event):
    parts = event.raw_text.split()
    cmd = parts[0].split("@")[0].lower()
    arg = parts[1] if len(parts) > 1 else None
    uid = event.sender_id
    is_admin = uid == ADMIN_CHAT_ID

    if cmd == "/cancel":
        if user_states.pop(uid, None):
            await event.reply("🚫 <b>عملیات قبلی لغو شد.</b>")
        else:
            await event.reply("🚫 فعلاً عملیاتی در جریان نیست.")
        return

    if cmd in ("/start", "/menu"):
        await event.reply(main_menu_text(), buttons=main_menu_kb(is_admin), link_preview=False)
        if cmd == "/start":
            await event.reply("👇 <b>دکمه‌های سریع</b> (همیشه زیر چت در دسترس‌اند):",
                              buttons=REPLY_KB)
        return

    if cmd == "/help":
        await event.reply(help_text(), buttons=[[Button.inline("🔙 بازگشت به منو", b"menu")]])
        return

    if cmd == "/id":
        await event.reply(f"🆔 آیدی عددی شما:\n<code>{uid}</code>")
        return

    # ───── فقط ادمین ─────
    if not is_admin:
        await event.reply("⛔ این بخش فقط برای ادمین است.")
        return

    if cmd == "/stats":
        await event.reply(stats_text(), buttons=[[Button.inline("🔙 بازگشت به منو", b"menu")]])

    elif cmd == "/files":
        await show_files_view(event)

    elif cmd == "/sendall":
        rows = get_all_files()
        if not rows:
            await event.reply("📭 آرشیو خالی است!")
            return
        tot = sum(r["file_size"] for r in rows)
        await event.reply(
            f"📤 <b>ارسال همه فایل‌ها</b>\n{LINE}\n"
            f"🗂 تعداد: <b>{len(rows)}</b> فایل\n📦 حجم: {human_size(tot)}\n\n"
            "همه فایل‌ها دوباره برای ادمین ارسال شود؟",
            buttons=[[Button.inline("✅ بله، ارسال کن", b"sendall_go")],
                     [Button.inline("🔙 بازگشت", b"menu")]])

    elif cmd == "/wipe":
        await event.reply(
            "🗑 <b>پاک کردن آرشیو</b>\n\nهمه سوابق فایل‌ها حذف شود؟\n"
            "(فایل‌های خود چت ادمین حذف نمی‌شوند)",
            buttons=[[Button.inline("🗑 بله، پاک کن", b"wipe_go")],
                     [Button.inline("🔙 بازگشت", b"menu")]])

    elif cmd == "/panel":
        mode = "همه کاربران مجاز هستند" if not allowed_users else f"{len(allowed_users)} کاربر مجاز"
        await event.reply(
            f"🛠 <b>پنل مدیریت {APP_NAME}</b>\n\n"
            f"وضعیت دسترسی: <b>{mode}</b>\n\n"
            "<b>دستورات:</b>\n"
            "• <code>/add 123456789</code> — افزودن کاربر\n"
            "• <code>/remove 123456789</code> — حذف کاربر\n"
            "• <code>/list</code> — لیست کاربران مجاز\n"
            "• <code>/allowall</code> — اجازه به همه")

    elif cmd == "/list":
        if not allowed_users:
            await event.reply("📋 لیست خالی است → <b>همه کاربران</b> می‌توانند استفاده کنند.")
        else:
            users = "\n".join(f"• <code>{u}</code>" for u in sorted(allowed_users))
            await event.reply(f"📋 <b>کاربران مجاز:</b>\n\n{users}")

    elif cmd == "/allowall":
        allowed_users.clear()
        save_allowed()
        await event.reply("✅ لیست پاک شد. الان <b>همه کاربران</b> می‌توانند استفاده کنند.")

    elif cmd in ("/add", "/remove"):
        if not arg or not arg.isdigit():
            await event.reply("❌ فرمت درست: <code>/add 123456789</code>")
            return
        target = int(arg)
        if cmd == "/add":
            allowed_users.add(target)
            save_allowed()
            await event.reply(f"✅ کاربر <code>{target}</code> اضافه شد.")
        else:
            if target in allowed_users:
                allowed_users.discard(target)
                save_allowed()
                await event.reply(f"✅ کاربر <code>{target}</code> حذف شد.")
            else:
                await event.reply("ℹ️ این کاربر در لیست نبود.")


# ═══════════════════ هندلر دکمه‌های شیشه‌ای ═══════════════════
@bot.on(events.CallbackQuery)
async def callbacks(event):
    data = (event.data or b"").decode("utf-8", "ignore")
    uid = event.sender_id
    is_admin = uid == ADMIN_CHAT_ID

    def owner_ok():
        st = user_states.get(uid)
        return st and st.get("owner") == uid

    try:
        if data == "menu":
            await event.edit(main_menu_text(), buttons=main_menu_kb(is_admin))

        elif data == "help":
            await event.edit(help_text(),
                             buttons=[[Button.inline("🔙 بازگشت به منو", b"menu")]])

        elif data == "stats":
            if not is_admin:
                await event.answer("⛔ فقط ادمین به آمار دسترسی دارد.", alert=True)
                return
            await event.edit(stats_text(),
                             buttons=[[Button.inline("🔙 بازگشت به منو", b"menu")]])

        elif data == "files":
            if not is_admin:
                await event.answer("⛔ فقط ادمین.", alert=True)
                return
            await show_files_view(event, edit=True)

        elif data == "myid":
            await event.answer(f"🆔 آیدی شما: {uid}", alert=True)

        elif data == "sendall":
            if not is_admin:
                await event.answer("⛔ فقط ادمین.", alert=True)
                return
            rows = get_all_files()
            if not rows:
                await event.answer("📭 آرشیو خالی است!", alert=True)
                return
            tot = sum(r["file_size"] for r in rows)
            await event.edit(
                f"📤 <b>ارسال همه فایل‌ها</b>\n{LINE}\n"
                f"🗂 تعداد: <b>{len(rows)}</b> فایل\n📦 حجم: {human_size(tot)}\n\n"
                "همه فایل‌ها دوباره برای ادمین ارسال شود؟",
                buttons=[[Button.inline("✅ بله، ارسال کن", b"sendall_go")],
                         [Button.inline("🔙 بازگشت", b"menu")]])

        elif data == "sendall_go":
            if not is_admin:
                await event.answer("⛔ فقط ادمین.", alert=True)
                return
            await event.edit("📤 <b>در حال آماده‌سازی...</b>")
            await send_all_files(event)

        elif data == "wipe":
            if not is_admin:
                await event.answer("⛔ فقط ادمین.", alert=True)
                return
            await event.edit(
                "🗑 <b>پاک کردن آرشیو</b>\n\nهمه سوابق فایل‌ها حذف شود؟",
                buttons=[[Button.inline("🗑 بله، پاک کن", b"wipe_go")],
                         [Button.inline("🔙 بازگشت", b"menu")]])

        elif data == "wipe_go":
            if not is_admin:
                await event.answer("⛔ فقط ادمین.", alert=True)
                return
            wipe_files()
            await event.edit("✅ <b>آرشیو پاک شد.</b>",
                             buttons=[[Button.inline("🔙 بازگشت به منو", b"menu")]])

        elif data == "cancel":
            if user_states.pop(uid, None):
                await event.edit("🚫 <b>لغو شد.</b>")
            else:
                await event.answer("چیزی برای لغو نبود.", alert=True)

        # ───── بدون تغییر اسم → اقدام فوری ─────
        elif data in ("u_no", "f_no", "t_no"):
            st = user_states.pop(uid, None)
            if not st or st.get("owner") != uid:
                await event.answer("⛔ این دکمه مال شما نیست!", alert=True)
                return
            await event.answer("⏳...")
            if data == "u_no":
                await event.edit("👍 با <b>اسم اصلی</b> ادامه می‌دهم...")
                await process_url(event, st["url"], None)
            elif data == "f_no":
                await event.edit("⚡ <b>کپی فوری بدون تغییر اسم...</b>")
                await process_forward_copy(event, st)
            else:
                await event.edit("⚡ <b>کپی فوری بدون تغییر اسم...</b>")
                await process_tg(event, st, None)

        # ───── با تغییر اسم → منتظر اسم ─────
        elif data in ("u_yes", "f_yes", "t_yes"):
            st = user_states.get(uid)
            if not st or st.get("owner") != uid:
                await event.answer("⛔ این دکمه مال شما نیست!", alert=True)
                return
            st["action"] = {"u_yes": "url_name", "f_yes": "fwd_name", "t_yes": "tg_name"}[data]
            st["ts"] = time.time()
            await event.answer()
            await event.edit(
                "✏️ <b>اسم جدید فایل را بنویس:</b>\n\n"
                "(پسوند <code>.pdf</code> خودکار اضافه می‌شود)",
                buttons=[[Button.inline("❌ انصراف", b"cancel")]])

        elif data.startswith("re:"):
            if not is_admin:
                await event.answer("⛔ فقط ادمین.", alert=True)
                return
            try:
                rec = get_file(int(data[3:]))
            except ValueError:
                rec = None
            if not rec:
                await event.answer("❌ فایل پیدا نشد.", alert=True)
                return
            await event.answer("⏳ در حال ارسال...")
            try:
                await resend_one(event, rec)
                await respond(event, f"✅ <code>{html.escape(rec['file_name'])}</code> دوباره ارسال شد.")
            except Exception as e:
                await respond(event, f"❌ خطا: {html.escape(str(e))}")

        elif data.startswith("rn:"):
            if not is_admin:
                await event.answer("⛔ فقط ادمین.", alert=True)
                return
            try:
                rec = get_file(int(data[3:]))
            except ValueError:
                rec = None
            if not rec:
                await event.answer("❌ فایل پیدا نشد.", alert=True)
                return
            user_states[uid] = {"action": "stored_name", "fid": rec["id"],
                                "owner": uid, "ts": time.time()}
            await event.edit(
                f"✏️ <b>تغییر نام:</b> <code>{html.escape(rec['file_name'])}</code>\n\n"
                "اسم جدید را بنویس:\n"
                "<i>(نیاز به دریافت و آپلود مجدد دارد)</i>",
                buttons=[[Button.inline("❌ انصراف", b"cancel")]])

    except Exception as e:
        log.error("Callback error: %s", e)
        try:
            await event.answer("⚠️ خطایی رخ داد!", alert=True)
        except Exception:
            pass


# ═══════════════════ هندلر فایل فورواردی / داکیومنت ═══════════════════
@bot.on(events.NewMessage(incoming=True, func=lambda e: e.is_private and e.document is not None))
async def handle_document(event):
    uid = event.sender_id
    if not is_allowed(uid):
        await event.reply("⛔ شما مجاز به استفاده از این ربات نیستید.")
        return
    user_states.pop(uid, None)

    doc = event.document
    name = doc_filename(doc)
    size = doc.size

    if size > MAX_SIZE:
        await event.reply(f"❌ حجم فایل ({human_size(size)}) از سقف مجاز "
                          f"({human_size(MAX_SIZE)}) بیشتر است.")
        return
    if not is_pdf_doc(doc, name):
        await event.reply("❗ فقط فایل‌های <b>PDF</b> پذیرفته می‌شوند.")
        return

    user_states[uid] = {"action": "fwd", "chat_id": event.chat_id,
                        "msg_id": event.message.id, "name": name, "size": size,
                        "owner": uid, "ts": time.time()}
    await event.reply(
        f"📩 <b>فایل دریافت شد!</b>\n{LINE}\n"
        f"🗂 <b>نام:</b> <code>{html.escape(name)}</code>\n"
        f"📦 <b>حجم:</b> {human_size(size)}\n{LINE}\n"
        "😎 <b>می‌خوای اسمش رو عوض کنی؟</b>",
        buttons=[
            [Button.inline("✏️ بله، اسم جدید می‌ذارم", b"f_yes"),
             Button.inline("📤 نه، همینه", b"f_no")],
            [Button.inline("❌ انصراف", b"cancel")],
        ])


# ═══════════════════ هندلر پیام‌های متنی ═══════════════════
@bot.on(events.NewMessage(incoming=True, func=lambda e: e.is_private))
async def handle_message(event):
    uid = event.sender_id
    text = (event.raw_text or "").strip()

    # ───── دکمه‌های کیبورد ثابت ─────
    if text == "🏠 منو":
        await event.reply(main_menu_text(), buttons=main_menu_kb(uid == ADMIN_CHAT_ID),
                          link_preview=False)
        return
    if text == "📊 آمار":
        if uid != ADMIN_CHAT_ID:
            await event.reply("⛔ فقط ادمین."); return
        await event.reply(stats_text(),
                          buttons=[[Button.inline("🔙 بازگشت به منو", b"menu")]])
        return
    if text == "🗂 آرشیو":
        if uid != ADMIN_CHAT_ID:
            await event.reply("⛔ فقط ادمین."); return
        await show_files_view(event)
        return
    if text == "❓ راهنما":
        await event.reply(help_text(),
                          buttons=[[Button.inline("🔙 بازگشت به منو", b"menu")]])
        return
    if text == "🆔 آیدی من":
        await event.reply(f"🆔 آیدی عددی شما:\n<code>{uid}</code>")
        return

    if text.startswith("/"):
        return

    # ───── اگر منتظر اسم دلخواه هستیم ─────
    st = user_states.get(uid)
    if st:
        if time.time() - st.get("ts", 0) > STATE_TTL:
            user_states.pop(uid, None)
        elif not URL_RE.search(text):
            await handle_state_text(event, st, text)
            return
        else:
            user_states.pop(uid, None)   # به‌جای اسم، لینک جدید فرستاد → شروع تازه

    if not is_allowed(uid):
        await event.reply("⛔ شما مجاز به استفاده از این ربات نیستید.")
        return

    urls = URL_RE.findall(text)
    if not urls:
        return

    tg_urls  = [u for u in urls if TG_LINK_RE.match(u)]
    web_urls = [u for u in urls if not TG_LINK_RE.match(u)]

    # ───── لینک‌های تلگرام ─────
    for u in tg_urls:
        parsed = parse_tg_link(u)
        if not parsed:
            await event.reply(
                "❌ این لینک تلگرامی قابل پردازش نیست!\n"
                "فقط لینک پیام: <code>t.me/name/123</code> یا <code>t.me/c/xxxx/123</code>",
                link_preview=False)
            continue
        kind, peer, ids = parsed
        try:
            msgs = await bot.get_messages(peer, ids=ids)
        except Exception:
            await event.reply("❌ به این چت دسترسی ندارم!\n"
                              "برای لینک‌های خصوصی، ربات را <b>ادمین</b> آن کانال/گروه کنید.")
            continue
        docs = [m for m in msgs if m and m.document]
        if not docs:
            await event.reply("❗ در این پیام فایلی پیدا نشد.")
            continue

        if len(tg_urls) == 1 and len(urls) == 1:
            d0 = docs[0]
            user_states[uid] = {"action": "tg", "peer": peer,
                                "ids": [m.id for m in docs],
                                "name": doc_filename(d0.document),
                                "size": d0.document.size,
                                "detail": u, "owner": uid, "ts": time.time()}
            await event.reply(
                f"🔗 <b>پیام تلگرام پیدا شد!</b>\n{LINE}\n"
                f"🗂 <b>نام:</b> <code>{html.escape(doc_filename(d0.document))}</code>\n"
                f"📦 <b>حجم:</b> {human_size(d0.document.size)}\n"
                f"🗂 <b>تعداد فایل:</b> {len(docs)}\n{LINE}\n"
                "😎 <b>می‌خوای اسمش رو عوض کنی؟</b>",
                buttons=[
                    [Button.inline("✏️ بله، اسم جدید می‌ذارم", b"t_yes"),
                     Button.inline("⚡ نه، کپی فوری", b"t_no")],
                    [Button.inline("❌ انصراف", b"cancel")],
                ], link_preview=False)
        else:
            stx = {"peer": peer, "ids": [m.id for m in docs], "detail": u, "owner": uid}
            await event.reply(f"🔗 {len(docs)} فایل تلگرامی پیدا شد — کپی فوری ⚡")
            asyncio.create_task(process_tg(event, stx, None))

    # ───── لینک‌های وب ─────
    if len(web_urls) == 1 and len(urls) == 1:
        user_states[uid] = {"action": "url", "url": web_urls[0], "owner": uid, "ts": time.time()}
        await event.reply(
            f"🌐 <b>لینک دریافت شد!</b>\n{LINE}\n"
            "😎 <b>اسم دلخواه برای فایل می‌خوای بذاری؟</b>",
            buttons=[
                [Button.inline("✏️ بله، اسم می‌ذارم", b"u_yes"),
                 Button.inline("📤 نه، اسم اصلی", b"u_no")],
                [Button.inline("❌ انصراف", b"cancel")],
            ], link_preview=False)
    elif web_urls:
        await event.reply(f"🔗 {len(web_urls)} لینک پیدا شد — پردازش همزمان با اسم اصلی 🚀")
        for u in web_urls:
            asyncio.create_task(process_url(event, u, None))


# ═══════════════════ دکمه استارت کنار فیلد چت ═══════════════════
async def set_commands():
    cmds = [
        BotCommand("start",   "🏠 نمایش منوی اصلی"),
        BotCommand("stats",   "📊 آمار دقیق"),
        BotCommand("files",   "🗂 آرشیو فایل‌ها"),
        BotCommand("sendall", "📤 ارسال مجدد همه فایل‌ها"),
        BotCommand("help",    "❓ راهنما"),
        BotCommand("id",      "🆔 آیدی عددی من"),
        BotCommand("cancel",  "⛔ لغو عملیات جاری"),
    ]
    try:
        await bot(SetBotCommandsRequest(scope=BotCommandScopeDefault(),
                                        lang_code="", commands=cmds))
        log.info("✅ دکمه استارت/منو کنار فیلد چت فعال شد")
    except Exception as e:
        log.warning("set commands failed: %s", e)


# ═══════════════════ اجرا ═══════════════════
async def main():
    if not all([API_ID, API_HASH, BOT_TOKEN, ADMIN_CHAT_ID]):
        log.error("❌ متغیرهای محیطی (API_ID, API_HASH, BOT_TOKEN, ADMIN_CHAT_ID) تنظیم نشده‌اند!")
        return
    db_init()
    load_allowed()
    await bot.start(bot_token=BOT_TOKEN)
    await set_commands()
    me = await bot.get_me()
    log.info("🤖 %s روشن شد: @%s | سقف: %s | آپلود همزمان: %d",
             APP_NAME, me.username, human_size(MAX_SIZE), MAX_CONCURRENT)
    await bot.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
