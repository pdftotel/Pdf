#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🤖 ربات ارسال PDF به ادمین — نسخه فایل‌های حجیم + اسم دلخواه
آماده برای Railway / GitHub
"""

import os
import re
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

# ═══════════════════════ تنظیمات از Environment Variables ═══════════════════════
API_ID        = int(os.getenv("API_ID", "0"))
API_HASH      = os.getenv("API_HASH", "")
BOT_TOKEN     = os.getenv("BOT_TOKEN", "")
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0"))

ALLOWED_USERS = set()  # مثال: {111111, 222222}

MAX_SIZE       = 2 * 1024 * 1024 * 1024   # ۲ گیگابایت
CHUNK_SIZE     = 512 * 1024
PROGRESS_EVERY = 1.8
# ═══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("pdf-bot")

URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)

# وضعیت کاربران (برای پرسیدن اسم)
user_states = {}  # {user_id: {"url": str, "status": Message}}

bot = TelegramClient("pdf_bot_session", API_ID, API_HASH)
bot.parse_mode = "html"


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

        # اگر اسم دلخواه داده شده باشه از اون استفاده کن
        final_name = sanitize(custom_name) if custom_name else sanitize(original_name)

        final_path = os.path.join(tmpdir, final_name)
        os.rename(raw_path, final_path)

        await status.edit("⬆️ <b>دانلود کامل شد!</b>\nدر حال ارسال به ادمین...")

        up_state = {"t": 0.0, "p": -1}

        async def upload_progress(current: int, total: int):
            await edit_progress(status, up_state, "⬆️ <b>در حال ارسال به ادمین</b>", current, total)

        sender = await event.get_sender()
        await bot.send_file(
            ADMIN_CHAT_ID,
            final_path,
            caption=build_caption(final_name, size, url, sender),
            force_document=True,
            progress_callback=upload_progress,
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


# ─────────────────────── هندلرها ───────────────────────
@bot.on(events.NewMessage(pattern=r"^/(start|id)$"))
async def cmd_handler(event):
    if event.raw_text == "/start":
        await event.reply(
            "👋 <b>سلام!</b>\n\n"
            "من ربات ارسال PDF به ادمین هستم 🤖\n\n"
            "📌 <b>طریقه استفاده:</b>\n"
            "۱. لینک مستقیم فایل PDF را بفرست\n"
            "۲. اگر خواستی اسم دلخواه بده، وگرنه بنویس <code>خیر</code>\n\n"
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

    # اگر کاربر در حال وارد کردن اسم دلخواه است
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

    # پیدا کردن لینک
    urls = URL_RE.findall(text)
    if not urls:
        return

    if ALLOWED_USERS and user_id not in ALLOWED_USERS:
        log.warning("کاربر غیرمجاز: %s", user_id)
        return

    # فعلاً فقط اولین لینک رو پردازش می‌کنیم (برای سادگی و جلوگیری از شلوغی)
    url = urls[0]

    # سوال در مورد اسم دلخواه
    msg = await event.reply(
        "📝 <b>آیا می‌خواید اسم دلخواه برای این فایل بذارید؟</b>\n\n"
        "• اگر بله → الان اسم مورد نظرتون رو بنویسید\n"
        "• اگر نه → فقط بنویسید: <code>خیر</code>",
        buttons=[
            [Button.text("خیر (اسم اصلی)")],
        ]
    )

    user_states[user_id] = {"url": url, "status": msg}


# ─────────────────────── اجرا ───────────────────────
async def main():
    if not all([API_ID, API_HASH, BOT_TOKEN, ADMIN_CHAT_ID]):
        log.error("❌ متغیرهای محیطی (API_ID, API_HASH, BOT_TOKEN, ADMIN_CHAT_ID) تنظیم نشده‌اند!")
        return

    await bot.start(bot_token=BOT_TOKEN)
    me = await bot.get_me()
    log.info("🤖 ربات روشن شد: @%s | سقف حجم: %s", me.username, human_size(MAX_SIZE))
    await bot.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())