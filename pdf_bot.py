#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🤖 ربات ارسال PDF به ادمین + پنل مدیریت کاربران
نسخه نهایی — چند فایل همزمان + سرعت بالا + اسم دلخواه
"""

import os
import re
import json
import html
import time
import shutil
import logging
import tempfile
import asyncio
from datetime import datetime
from urllib.parse import urlparse, unquote

import aiohttp
from telethon import TelegramClient, events, Button
from telethon.network.connection.tcpabridged import ConnectionTcpAbridged
from telethon.tl.types import DocumentAttributeFilename

# ═══════════════════════ تنظیمات از Environment Variables ═══════════════════════
API_ID        = int(os.getenv("API_ID", "0"))
API_HASH      = os.getenv("API_HASH", "")
BOT_TOKEN     = os.getenv("BOT_TOKEN", "")
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0"))

MAX_SIZE          = 2 * 1024 * 1024 * 1024   # ۲ گیگابایت
CHUNK_SIZE        = 512 * 1024
PROGRESS_EVERY    = 3.5
MAX_CONCURRENT    = 3                        # حداکثر آپلود همزمان (سرعت کم نشه)
ALLOWED_FILE      = "allowed_users.json"
# ═══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("pdf-bot")

URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)

# وضعیت‌ها
user_states = {}          # {user_id: {"url": str}}
allowed_users = set()     # کاربران مجاز
upload_semaphore = asyncio.Semaphore(MAX_CONCURRENT)

bot = TelegramClient(
    "pdf_bot_session",
    API_ID,
    API_HASH,
    connection=ConnectionTcpAbridged,
    connection_retries=5,
    retry_delay=1,
    timeout=30,
    auto_reconnect=True,
)
bot.parse_mode = "html"


# ─────────────────────────── مدیریت کاربران مجاز ───────────────────────────
def load_allowed():
    global allowed_users
    try:
        if os.path.exists(ALLOWED_FILE):
            with open(ALLOWED_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                allowed_users = set(data)
                log.info("کاربران مجاز بارگذاری شد: %s", allowed_users)
    except Exception as e:
        log.warning("خطا در بارگذاری کاربران مجاز: %s", e)
        allowed_users = set()


def save_allowed():
    try:
        with open(ALLOWED_FILE, "w", encoding="utf-8") as f:
            json.dump(list(allowed_users), f)
    except Exception as e:
        log.error("خطا در ذخیره کاربران مجاز: %s", e)


def is_allowed(user_id: int) -> bool:
    if user_id == ADMIN_CHAT_ID:
        return True
    if not allowed_users:          # لیست خالی = همه مجاز
        return True
    return user_id in allowed_users


# ─────────────────────────── ابزارها ───────────────────────────
def human_size(n: float) -> str:
    for unit in ("بایت", "KB", "MB", "GB"):
        if n < 1024:
            return f"{int(n)} بایت" if unit == "بایت" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def make_bar(pct: int, length: int = 16) -> str:
    filled = round(length * pct / 100)
    return "█" * filled + "░" * (length - filled)


def sanitize(name: str) -> str:
    name = re.sub(r'[\\/:*?"<>|\r\n]+', "_", name).strip()
    if not name.lower().endswith(".pdf"):
        name += ".pdf"
    return name or "document.pdf"


def get_filename_from_url(url: str, content_disposition: str | None = None) -> str:
    if content_disposition:
        m = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^";]+)"?', content_disposition, re.I)
        if m:
            name = unquote(m.group(1)).strip()
            if name:
                return name if name.lower().endswith(".pdf") else name + ".pdf"
    name = unquote(os.path.basename(urlparse(url).path)) or "document"
    return name if name.lower().endswith(".pdf") else name + ".pdf"


def build_caption(filename: str, size: int, url: str, sender) -> str:
    uname = f"@{sender.username}" if sender and sender.username else "—"
    first = html.escape(sender.first_name) if sender else "ناشناس"
    short_url = url if len(url) <= 70 else url[:70] + "…"
    return (
        "📥 <b>فایل جدید دریافت شد</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"🗂 <b>نام فایل:</b> <code>{html.escape(filename)}</code>\n"
        f"📦 <b>حجم:</b> {human_size(size)}\n"
        f"🕒 <b>زمان:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 <b>ارسال‌کننده:</b> {first} ({uname})\n"
        f"🆔 <b>آیدی:</b> <code>{sender.id if sender else '—'}</code>\n"
        f"🔗 <b>لینک:</b> <code>{html.escape(short_url)}</code>"
    )


# ─────────────────────── پیشرفت‌ها ───────────────────────
async def edit_progress(status, state, title, done: int, total: int):
    now = time.monotonic()
    if total > 0:
        pct = min(int(done * 100 / total), 100)
        if now - state["t"] < PROGRESS_EVERY and pct != 100:
            return
        if pct == state.get("p") and pct != 100:
            return
        state["t"], state["p"] = now, pct
        text = (
            f"{title}\n\n"
            f"{make_bar(pct)}  <b>{pct}%</b>\n"
            f"📦 {human_size(done)} از {human_size(total)}"
        )
    else:
        if now - state["t"] < PROGRESS_EVERY and done != 0:
            return
        state["t"] = now
        text = f"{title}\n\n📦 {human_size(done)}"
    try:
        await status.edit(text)
    except Exception:
        pass


# ─────────────────────── دانلود فایل ───────────────────────
async def download_pdf(url: str, dest: str, status) -> tuple[str, int]:
    timeout = aiohttp.ClientTimeout(total=None, connect=30, sock_read=600)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        async with session.get(url) as resp:
            if resp.status != 200:
                raise RuntimeError(f"سرور کد {resp.status} برگرداند (لینک منقضی یا نامعتبر؟)")

            cl = resp.headers.get("Content-Length", "")
            total = int(cl) if cl.isdigit() else 0

            if total > MAX_SIZE:
                raise RuntimeError(
                    f"حجم فایل ({human_size(total)}) از سقف مجاز ({human_size(MAX_SIZE)}) بیشتر است"
                )

            state = {"t": 0.0, "p": -1}
            done = 0
            first_chunk = True

            with open(dest, "wb") as f:
                async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                    if first_chunk:
                        if b"%PDF" not in chunk[:2048]:
                            raise RuntimeError("محتوای این لینک یک فایل PDF معتبر نیست ❗")
                        first_chunk = False
                    f.write(chunk)
                    done += len(chunk)
                    await edit_progress(status, state, "⬇️ <b>در حال دانلود</b>", done, total)

            if first_chunk:
                raise RuntimeError("فایل خالی است")

            filename = get_filename_from_url(url, resp.headers.get("Content-Disposition"))
            return filename, done


# ─────────────────────── پردازش اصلی ───────────────────────
async def process_link(event, url: str, custom_name: str | None = None):
    status = await event.reply("🔎 در حال بررسی لینک...")
    tmpdir = tempfile.mkdtemp(prefix="pdfbot_")

    try:
        fd, raw_path = tempfile.mkstemp(dir=tmpdir, suffix=".pdf")
        os.close(fd)

        original_name, size = await download_pdf(url, raw_path, status)

        final_name = sanitize(custom_name) if custom_name else sanitize(original_name)
        final_path = os.path.join(tmpdir, final_name)
        os.rename(raw_path, final_path)

        await status.edit("⬆️ <b>دانلود کامل شد!</b>\nدر صف ارسال به ادمین...")

        # محدودیت همزمانی آپلود برای حفظ سرعت
        async with upload_semaphore:
            await status.edit("⬆️ <b>در حال ارسال سریع به ادمین</b>...")

            up_state = {"t": 0.0, "p": -1}

            async def upload_progress(current: int, total: int):
                await edit_progress(status, up_state, "⬆️ <b>در حال ارسال سریع به ادمین</b>", current, total)

            input_file = await bot.upload_file(
                final_path,
                progress_callback=upload_progress,
                part_size_kb=512
            )

            sender = await event.get_sender()
            await bot.send_file(
                ADMIN_CHAT_ID,
                input_file,
                caption=build_caption(final_name, size, url, sender),
                force_document=True,
                attributes=[DocumentAttributeFilename(final_name)],
                file_name=final_name
            )

        await status.edit(
            f"✅ <b>انجام شد!</b>\n\n"
            f"🗂 {html.escape(final_name)}\n"
            f"📦 {human_size(size)} — برای ادمین ارسال شد."
        )
        log.info("✅ %s (%s) ارسال شد", final_name, human_size(size))

    except Exception as e:
        await status.edit(f"❌ <b>خطا:</b>\n{html.escape(str(e))}")
        log.error("Error: %s", e)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ─────────────────────── پنل ادمین ───────────────────────
@bot.on(events.NewMessage(pattern=r"^/(panel|admin|add|remove|list|allowall)"))
async def admin_panel(event):
    if event.sender_id != ADMIN_CHAT_ID:
        return

    text = event.raw_text.strip()
    parts = text.split()
    cmd = parts[0].lower()

    if cmd in ("/panel", "/admin"):
        count = len(allowed_users)
        mode = "همه کاربران مجاز هستند" if count == 0 else f"{count} کاربر مجاز"
        await event.reply(
            f"🛠 <b>پنل مدیریت ربات</b>\n\n"
            f"وضعیت فعلی: <b>{mode}</b>\n\n"
            f"<b>دستورات:</b>\n"
            f"• <code>/add 123456789</code> — اضافه کردن کاربر\n"
            f"• <code>/remove 123456789</code> — حذف کاربر\n"
            f"• <code>/list</code> — لیست کاربران مجاز\n"
            f"• <code>/allowall</code> — اجازه به همه (پاک کردن لیست)"
        )
        return

    if cmd == "/list":
        if not allowed_users:
            await event.reply("📋 لیست خالی است → <b>همه کاربران</b> می‌توانند استفاده کنند.")
        else:
            users = "\n".join(f"• <code>{uid}</code>" for uid in sorted(allowed_users))
            await event.reply(f"📋 <b>کاربران مجاز:</b>\n\n{users}")
        return

    if cmd == "/allowall":
        allowed_users.clear()
        save_allowed()
        await event.reply("✅ لیست پاک شد. الان <b>همه کاربران</b> می‌توانند از ربات استفاده کنند.")
        return

    if cmd == "/add" and len(parts) == 2:
        try:
            uid = int(parts[1])
            allowed_users.add(uid)
            save_allowed()
            await event.reply(f"✅ کاربر <code>{uid}</code> اضافه شد.")
        except ValueError:
            await event.reply("❌ آیدی باید عدد باشد.")
        return

    if cmd == "/remove" and len(parts) == 2:
        try:
            uid = int(parts[1])
            if uid in allowed_users:
                allowed_users.discard(uid)
                save_allowed()
                await event.reply(f"✅ کاربر <code>{uid}</code> حذف شد.")
            else:
                await event.reply("ℹ️ این کاربر در لیست نبود.")
        except ValueError:
            await event.reply("❌ آیدی باید عدد باشد.")
        return

    await event.reply("❌ دستور ناقص است. از /panel استفاده کنید.")


# ─────────────────────── هندلرهای عمومی ───────────────────────
@bot.on(events.NewMessage(pattern=r"^/(start|id)$"))
async def cmd_handler(event):
    if event.raw_text == "/start":
        await event.reply(
            "👋 <b>سلام!</b>\n\n"
            "من ربات ارسال PDF به ادمین هستم 🤖\n\n"
            "📌 <b>طریقه استفاده:</b>\n"
            "• لینک مستقیم PDF را بفرست\n"
            "• اگر یک لینک باشه می‌تونی اسم دلخواه بدی\n"
            "• چند لینک همزمان هم پشتیبانی می‌شه\n\n"
            f"📦 حداکثر حجم: <b>{human_size(MAX_SIZE)}</b>"
        )
    else:
        await event.reply(f"🆔 آیدی عددی شما:\n<code>{event.sender_id}</code>")


@bot.on(events.NewMessage(incoming=True, func=lambda e: e.is_private))
async def handle_message(event):
    user_id = event.sender_id
    text = (event.raw_text or "").strip()

    if text.startswith("/"):
        return

    # در حال وارد کردن اسم دلخواه
    if user_id in user_states:
        state = user_states.pop(user_id)
        url = state["url"]

        if text.lower() in ("خیر", "no", "n", "-"):
            custom_name = None
            await event.reply("👍 با اسم اصلی فایل ادامه می‌دم...")
        else:
            custom_name = text
            await event.reply(f"✅ اسم انتخابی: <b>{html.escape(custom_name)}</b>")

        asyncio.create_task(process_link(event, url, custom_name))
        return

    # بررسی دسترسی
    if not is_allowed(user_id):
        await event.reply("⛔ شما مجاز به استفاده از این ربات نیستید.")
        return

    urls = URL_RE.findall(text)
    if not urls:
        return

    # یک لینک → پرسیدن اسم دلخواه
    if len(urls) == 1:
        url = urls[0]
        await event.reply(
            "📝 <b>آیا می‌خواید اسم دلخواه برای این فایل بذارید؟</b>\n\n"
            "• اگر بله → الان اسم مورد نظرتون رو بنویسید\n"
            "• اگر نه → بنویسید: <code>خیر</code>",
            buttons=[[Button.text("خیر (اسم اصلی)")]]
        )
        user_states[user_id] = {"url": url}
        return

    # چند لینک → همزمان با اسم اصلی
    await event.reply(f"🔗 {len(urls)} لینک پیدا شد. در حال پردازش همزمان...")
    for url in urls:
        asyncio.create_task(process_link(event, url, None))


# ─────────────────────── اجرا ───────────────────────
async def main():
    if not all([API_ID, API_HASH, BOT_TOKEN, ADMIN_CHAT_ID]):
        log.error("❌ متغیرهای محیطی تنظیم نشده‌اند!")
        return

    load_allowed()
    await bot.start(bot_token=BOT_TOKEN)
    me = await bot.get_me()
    log.info("🤖 ربات روشن شد: @%s | سقف همزمانی آپلود: %d", me.username, MAX_CONCURRENT)
    await bot.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
