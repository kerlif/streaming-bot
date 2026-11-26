"""
Streaming Bot - Hardened Edition v2.4.0
=======================================
ميزات v2.4.0:
- لوحة رئيسية ذكية (تُظهر بدء أو إيقاف/إعادة تشغيل حسب الحالة) + زر 🏠 + زر تحديث للحالة.
- حذف رسالة مفتاح البث من المحادثة بعد استلامها.
- زر 🧪 اختبار الرابط (ffprobe) قبل الحفظ.
- انتهاء مهلة المعالج تلقائياً (SETUP_TIMEOUT) + استرداد سليم عند فقدان المسودة.
- أمر /revoke للمدراء (يسحب التفويض ويوقف بث المستخدم).
- حد أقصى للبثوث المتزامنة (MAX_STREAMS).

إصلاحات v2.3.0 (الأزرار + الاستقرار):
0. [الأهم] إصلاح regex حالة STATE_SETUP_MENU: كان "^(setup:|cmd:cancel)$" فلا يطابق أي زر setup:*
   فكانت أزرار (استخدام نفس الإعدادات / تعديل ... / إعداد جديد) ميتة تماماً.
1. safe_edit: تجاهل "Message is not modified" بدل أن يبدو الزر معطّلاً.
2. إصلاح reply_text على CallbackQuery (كان يرمي AttributeError صامتاً).
3. FFmpeg: لم يعد stderr=PIPE بلا قارئ (كان يجمّد البث)، + قراءة آخر أسطر الخطأ.
4. detect_stream_type: m3u8 لم يعد يُعامل كراديو + يعتمد على path لا على الرابط كاملاً.
5. أوامر FFmpeg حسب البروتوكول (rtmp/rtsp لا تأخذ خيارات http) + صوت AAC دائماً.
6. تخزين اللغة في الذاكرة (كان يكتب في SQLite عند كل ترجمة).
7. فحص صلاحيات القناة لم يعد يمنع الحفظ.
8. معالج للأزرار القديمة (بعد إعادة التشغيل) + إيقاف نظيف + backoff + إشعار عند تكرار الفشل.

إصلاحات v2.2.0:
  1. إعادة ضبط ذكية: يسأل المستخدم إن كان يريد reuse أو تعديل حقل معين
  2. إصلاح تبديل البث: launch_stream(force=True) يوقف القديم قبل الجديد
  3. إصلاح race condition في is_stream_running (يعتمد على task.done())
  4. إعادة تشغيل تلقائية للبث عند تغيير الإعدادات أثناء التشغيل
  5. زر "إعادة تشغيل" في القائمة الرئيسية
  6. تحسين السلاسة: stop متزامن + انتظار نظيف

إصلاحات v2.1.0 (محفوظة):
  - HTML بدل MarkdownV2
  - query.answer() في كل callbacks
  - معالج أخطاء عام
  - تقليص الكود عبر _send_or_edit
"""

from __future__ import annotations

import asyncio
import functools
import json
import logging
import os
import sqlite3
import subprocess
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlparse

from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import BadRequest, TelegramError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    TypeHandler,
    filters,
)

# ---------------------------------------------------------------------------
# تكوين النظام
# ---------------------------------------------------------------------------
load_dotenv()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=getattr(logging, LOG_LEVEL, logging.INFO),
)
logger = logging.getLogger(__name__)

DB_NAME = os.getenv("DATABASE_PATH", "stream_manager.db")
FFMPEG_TIMEOUT = int(os.getenv("FFMPEG_TIMEOUT", "15000000"))
RECONNECT_DELAY = int(os.getenv("RECONNECT_DELAY", "5"))
DEFAULT_LANGUAGE = os.getenv("DEFAULT_LANGUAGE", "ar").lower()

ADMIN_IDS: set[int] = {
    int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()
}

MAX_BACKOFF = int(os.getenv("MAX_BACKOFF", "60"))
NOTIFY_AFTER_FAILURES = int(os.getenv("NOTIFY_AFTER_FAILURES", "3"))
MAX_STREAMS = int(os.getenv("MAX_STREAMS", "5"))  # 0 = بلا حد
SETUP_TIMEOUT = int(os.getenv("SETUP_TIMEOUT", "600"))  # ثوانٍ
PROBE_TIMEOUT = 25
MIN_HEALTHY_RUN = 30  # ثوانٍ: تشغيل أطول من هذا يُعدّ ناجحاً ويصفّر عداد الفشل

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

AUDIO_EXTENSIONS = (
    ".mp3", ".aac", ".ogg", ".opus", ".pls", ".asx", ".xspf",
)
RADIO_DOMAINS = (
    "aymane.xyz", "qurango.net", "radiojar.com", "radiocoast.com", "listenradio.org",
)

# حالات ConversationHandler - مشروطة بدالة one-line لكل حالة
(
    STATE_SETUP_MENU,      # 0: قائمة reuse/modify (جديد)
    STATE_STREAM_URL,      # 1
    STATE_SERVER_URL,      # 2
    STATE_STREAM_KEY,      # 3
    STATE_CHANNEL_ID,      # 4
    STATE_CONFIRM,         # 5
) = range(6)

# ---------------------------------------------------------------------------
# نظام الترجمة
# ---------------------------------------------------------------------------
TRANSLATIONS: dict[str, dict[str, str]] = {
    "ar": {
        "welcome": "👋 <b>أهلاً بك في بوت إدارة البث المباشر</b>\n\nاختر إحدى العمليات:",
        "btn_setup": "⚙️ ضبط الإعدادات",
        "btn_start": "▶️ بدء البث",
        "btn_stop": "⏹️ إيقاف البث",
        "btn_restart": "🔄 إعادة التشغيل",
        "btn_status": "📊 الحالة",
        "btn_language": "🌐 اللغة",
        "btn_help": "❓ المساعدة",
        "btn_cancel": "❌ إلغاء",
        "btn_home": "🏠 القائمة الرئيسية",
        "btn_test": "🧪 اختبار الرابط",
        "btn_refresh": "🔄 تحديث",
        "test_running": "🧪 جاري فحص الرابط (حتى 25 ثانية)...",
        "test_ok": "✅ الرابط يعمل: <code>{summary}</code>",
        "test_failed": "❌ فشل فحص الرابط: <code>{error}</code>",
        "stream_limit": "🚫 وصل البوت للحد الأقصى من البثوث المتزامنة ({limit}). حاول لاحقاً.",
        "session_expired": "⌛ انتهت جلسة الإعداد. ابدأ من جديد عبر الزر أو /setup.",
        "revoke_ok": "✅ تم سحب التفويض من {uid} وإيقاف بثه.",
        "revoke_usage": "الاستخدام: /revoke <user_id>",
        "revoke_admin": "⚠️ لا يمكن سحب تفويض مدير معرّف في ADMIN_IDS.",
        "btn_confirm": "✅ تأكيد وحفظ",
        "btn_reuse": "♻️ استخدام نفس الإعدادات",
        "btn_modify_url": "📡 تعديل رابط البث",
        "btn_modify_server": "🖥️ تعديل الخادم",
        "btn_modify_key": "🔑 تعديل المفتاح",
        "btn_modify_channel": "📣 تعديل القناة",
        "btn_new_setup": "🆕 إعداد جديد كامل",
        "setup_intro": "⚙️ <b>معالج ضبط الإعدادات</b>\n\nأرسل <b>رابط البث</b> (YouTube / m3u8 / راديو):",
        "setup_menu_title": "⚙️ <b>لديك إعدادات محفوظة</b>\n\nاختر ما تريد فعله:",
        "setup_current_label": "<b>الإعدادات الحالية:</b>",
        "setup_modify_url_prompt": "📡 أرسل <b>رابط البث الجديد</b>:",
        "setup_modify_server_prompt": "🖥️ أرسل <b>عنوان الخادم الجديد</b>:",
        "setup_modify_key_prompt": "🔑 أرسل <b>المفتاح الجديد</b>:",
        "setup_modify_channel_prompt": "📣 أرسل <b>معرّف القناة الجديد</b>:",
        "setup_stream_url_prompt": "📡 أرسل <b>رابط البث</b> (مثال: <code>https://youtu.be/xxx</code>):",
        "setup_server_url_prompt": "🖥️ أرسل <b>عنوان خادم RTMP</b> (مثال: <code>rtmp://server.com/live/</code>):",
        "setup_stream_key_prompt": "🔑 أرسل <b>مفتاح البث</b> (stream key):",
        "setup_channel_id_prompt": "📣 أرسل <b>معرّف القناة</b> (مثال: <code>-1001234567890</code> أو <code>@mychannel</code>):",
        "setup_invalid_url": "❌ رابط غير صالح. يجب أن يبدأ بـ <code>http(s)://</code> أو <code>rtmp(s)://</code> أو <code>rtsp://</code>. حاول مجدداً:",
        "setup_invalid_server": "❌ عنوان الخادم غير صالح. يجب أن يبدأ بـ <code>rtmp://</code> أو <code>rtmps://</code>. حاول مجدداً:",
        "setup_invalid_channel": "❌ معرّف القناة غير صالح. استخدم <code>-100xxxxxxxxxx</code> أو <code>@username</code>. حاول مجدداً:",
        "setup_checking_perms": "🔄 جاري التحقق من صلاحيات البوت في القناة...",
        "setup_detected_type": "📡 نوع البث المكتشف: <b>{type_label}</b>",
        "setup_perms_failed": "❌ فشل التحقق! تأكد من رفع البوت آدمن في القناة ومنحه صلاحية نشر الرسائل.",
        "setup_review_title": "📋 <b>مراجعة الإعدادات قبل الحفظ:</b>",
        "setup_review_stream_url": "📡 رابط البث",
        "setup_review_server": "🖥️ الخادم",
        "setup_review_key": "🔑 المفتاح",
        "setup_review_channel": "📣 القناة",
        "setup_review_type": "📡 نوع البث",
        "setup_saved": "✅ تم حفظ الإعدادات بنجاح!",
        "setup_saved_restart": "✅ تم حفظ الإعدادات وإعادة تشغيل البث بالرابط الجديد!",
        "setup_cancelled": "❌ تم إلغاء المعالج. لم يُحفظ شيء.",
        "type_youtube": "🎬 يوتيوب مباشر",
        "type_radio": "🎵 إذاعة / راديو",
        "type_iptv": "📺 IPTV",
        "stream_already_running": "⚠️ البث يعمل بالفعل! أوقفه أولاً أو استخدم زر إعادة التشغيل.",
        "stream_starting": "🚀 جاري تشغيل البث...",
        "stream_restarting": "🔄 جاري إيقاف البث القديم وتشغيل الجديد...",
        "stream_started": "🟢 تم تشغيل البث بنجاح وهو تحت المراقبة.",
        "stream_restarted": "🟢 تم إعادة تشغيل البث بالإعدادات الجديدة.",
        "stream_no_settings": "❌ لا توجد إعدادات محفوظة. استخدم معالج الضبط أولاً.",
        "stream_not_running": "⚪ لا يوجد بث يعمل حالياً.",
        "stream_stopped": "⏹️ تم إيقاف البث وتحرير الموارد.",
        "stream_failed": "❌ فشل تشغيل البث: <code>{error}</code>",
        "status_title": "📊 <b>تقرير حالة المنظومة</b>",
        "status_db": "▪️ حالة قاعدة البيانات",
        "status_running": "🟢 قيد العمل",
        "status_stopped": "🔴 متوقف",
        "status_server": "▪️ خادم البث",
        "status_type": "▪️ نوع البث",
        "status_process": "▪️ عملية FFmpeg",
        "status_active": "🟢 نشط",
        "status_inactive": "🔴 غير نشط",
        "status_none": "— غير محدد —",
        "not_authorized": "🚫 غير مصرّح لك باستخدام هذا البوت. تواصل مع المدير.",
        "language_changed": "✅ تم تغيير اللغة إلى <b>{lang_name}</b>.",
        "language_select": "🌐 اختر اللغة:",
        "lang_ar": "العربية",
        "lang_en": "English",
        "help_text": (
            "❓ <b>المساعدة</b>\n\n"
            "هذا البوت يدير بث مباشر من مصدر (YouTube / m3u8 / راديو) إلى خادم RTMP.\n\n"
            "<b>الأوامر:</b>\n"
            "• /start - القائمة الرئيسية\n"
            "• /setup - معالج ضبط الإعدادات (ذكي: يعيد استخدام القديم)\n"
            "• /start_stream - بدء البث\n"
            "• /stop_stream - إيقاف البث\n"
            "• /status - عرض الحالة\n"
            "• /language - تغيير اللغة\n"
            "• /help - هذه الرسالة\n"
            "• /authorize و /revoke - للمدراء فقط\n\n"
            "<b>ملاحظة قانونية:</b> المستخدم مسؤول عن امتلاك حقوق المصدر وإعادة بثه."
        ),
    },
    "en": {
        "welcome": "👋 <b>Welcome to the Live Stream Manager Bot</b>\n\nChoose an action:",
        "btn_setup": "⚙️ Setup",
        "btn_start": "▶️ Start Stream",
        "btn_stop": "⏹️ Stop Stream",
        "btn_restart": "🔄 Restart",
        "btn_status": "📊 Status",
        "btn_language": "🌐 Language",
        "btn_help": "❓ Help",
        "btn_cancel": "❌ Cancel",
        "btn_home": "🏠 Main menu",
        "btn_test": "🧪 Test link",
        "btn_refresh": "🔄 Refresh",
        "test_running": "🧪 Probing the link (up to 25s)...",
        "test_ok": "✅ Link works: <code>{summary}</code>",
        "test_failed": "❌ Link probe failed: <code>{error}</code>",
        "stream_limit": "🚫 The bot reached its concurrent stream limit ({limit}). Try again later.",
        "session_expired": "⌛ Setup session expired. Start again via the button or /setup.",
        "revoke_ok": "✅ Revoked {uid} and stopped their stream.",
        "revoke_usage": "Usage: /revoke <user_id>",
        "revoke_admin": "⚠️ Cannot revoke an admin defined in ADMIN_IDS.",
        "btn_confirm": "✅ Confirm & Save",
        "btn_reuse": "♻️ Reuse same settings",
        "btn_modify_url": "📡 Modify stream URL",
        "btn_modify_server": "🖥️ Modify server",
        "btn_modify_key": "🔑 Modify key",
        "btn_modify_channel": "📣 Modify channel",
        "btn_new_setup": "🆕 Full new setup",
        "setup_intro": "⚙️ <b>Setup Wizard</b>\n\nSend the <b>stream URL</b> (YouTube / m3u8 / radio):",
        "setup_menu_title": "⚙️ <b>You have saved settings</b>\n\nChoose what to do:",
        "setup_current_label": "<b>Current settings:</b>",
        "setup_modify_url_prompt": "📡 Send the <b>new stream URL</b>:",
        "setup_modify_server_prompt": "🖥️ Send the <b>new server URL</b>:",
        "setup_modify_key_prompt": "🔑 Send the <b>new stream key</b>:",
        "setup_modify_channel_prompt": "📣 Send the <b>new channel ID</b>:",
        "setup_stream_url_prompt": "📡 Send the <b>stream URL</b> (e.g. <code>https://youtu.be/xxx</code>):",
        "setup_server_url_prompt": "🖥️ Send the <b>RTMP server URL</b> (e.g. <code>rtmp://server.com/live/</code>):",
        "setup_stream_key_prompt": "🔑 Send the <b>stream key</b>:",
        "setup_channel_id_prompt": "📣 Send the <b>channel ID</b> (e.g. <code>-1001234567890</code> or <code>@mychannel</code>):",
        "setup_invalid_url": "❌ Invalid URL. Must start with <code>http://</code> or <code>https://</code>. Try again:",
        "setup_invalid_server": "❌ Invalid server URL. Must start with <code>rtmp://</code> or <code>rtmps://</code>. Try again:",
        "setup_invalid_channel": "❌ Invalid channel ID. Use <code>-100xxxxxxxxxx</code> or <code>@username</code>. Try again:",
        "setup_checking_perms": "🔄 Verifying bot permissions in target channel...",
        "setup_detected_type": "📡 Detected stream type: <b>{type_label}</b>",
        "setup_perms_failed": "❌ Permission check failed! Make sure the bot is admin in the channel with Post Messages permission.",
        "setup_review_title": "📋 <b>Review settings before saving:</b>",
        "setup_review_stream_url": "📡 Stream URL",
        "setup_review_server": "🖥️ Server",
        "setup_review_key": "🔑 Key",
        "setup_review_channel": "📣 Channel",
        "setup_review_type": "📡 Stream type",
        "setup_saved": "✅ Settings saved successfully!",
        "setup_saved_restart": "✅ Settings saved and stream restarted with new URL!",
        "setup_cancelled": "❌ Wizard cancelled. Nothing was saved.",
        "type_youtube": "🎬 YouTube Live",
        "type_radio": "🎵 Radio / Audio",
        "type_iptv": "📺 IPTV",
        "stream_already_running": "⚠️ Stream is already running! Stop it first or use Restart button.",
        "stream_starting": "🚀 Starting stream...",
        "stream_restarting": "🔄 Stopping old stream and starting new one...",
        "stream_started": "🟢 Stream started successfully and is being monitored.",
        "stream_restarted": "🟢 Stream restarted with new settings.",
        "stream_no_settings": "❌ No saved settings. Run the setup wizard first.",
        "stream_not_running": "⚪ No stream is currently running.",
        "stream_stopped": "⏹️ Stream stopped and resources released.",
        "stream_failed": "❌ Failed to start stream: <code>{error}</code>",
        "status_title": "📊 <b>System Status Report</b>",
        "status_db": "▪️ DB status",
        "status_running": "🟢 Running",
        "status_stopped": "🔴 Stopped",
        "status_server": "▪️ Stream server",
        "status_type": "▪️ Stream type",
        "status_process": "▪️ FFmpeg process",
        "status_active": "🟢 Active",
        "status_inactive": "🔴 Inactive",
        "status_none": "— none —",
        "not_authorized": "🚫 You are not authorized to use this bot. Contact the admin.",
        "language_changed": "✅ Language changed to <b>{lang_name}</b>.",
        "language_select": "🌐 Select language:",
        "lang_ar": "العربية",
        "lang_en": "English",
        "help_text": (
            "❓ <b>Help</b>\n\n"
            "This bot manages a live stream from a source (YouTube / m3u8 / radio) to an RTMP server.\n\n"
            "<b>Commands:</b>\n"
            "• /start - Main menu\n"
            "• /setup - Setup wizard (smart: reuses existing)\n"
            "• /start_stream - Start streaming\n"
            "• /stop_stream - Stop streaming\n"
            "• /status - Show status\n"
            "• /language - Change language\n"
            "• /help - This message\n"
            "• /authorize and /revoke - admins only\n\n"
            "<b>Legal note:</b> The user is responsible for owning the rights to the source."
        ),
    },
}


def t(user_id: int, key: str, **kwargs) -> str:
    lang = get_user_language(user_id)
    text = TRANSLATIONS.get(lang, TRANSLATIONS["ar"]).get(key, key)
    try:
        return text.format(**kwargs) if kwargs else text
    except (KeyError, IndexError):
        return text


def html_escape(text: str) -> str:
    if text is None:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# ---------------------------------------------------------------------------
# قاعدة البيانات
# ---------------------------------------------------------------------------
_db_lock = threading.Lock()


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with _db_lock, get_db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS settings (
                user_id INTEGER PRIMARY KEY,
                m3u_url TEXT NOT NULL,
                server_url TEXT NOT NULL,
                stream_key TEXT NOT NULL,
                channel_id TEXT NOT NULL,
                is_running INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                language TEXT DEFAULT 'ar',
                is_authorized INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        conn.commit()
    logger.info("✅ Database initialized")


def save_settings(user_id: int, m3u_url: str, server_url: str, stream_key: str, channel_id: str) -> None:
    with _db_lock, get_db() as conn:
        conn.execute(
            "INSERT INTO settings (user_id, m3u_url, server_url, stream_key, channel_id, is_running, updated_at) "
            "VALUES (?, ?, ?, ?, ?, 0, CURRENT_TIMESTAMP) "
            "ON CONFLICT(user_id) DO UPDATE SET "
            "m3u_url=excluded.m3u_url, server_url=excluded.server_url, "
            "stream_key=excluded.stream_key, channel_id=excluded.channel_id, "
            "is_running=0, updated_at=CURRENT_TIMESTAMP",
            (user_id, m3u_url, server_url, stream_key, channel_id),
        )
        conn.commit()


def update_status(user_id: int, is_running: bool) -> None:
    with _db_lock, get_db() as conn:
        conn.execute(
            "UPDATE settings SET is_running=?, updated_at=CURRENT_TIMESTAMP WHERE user_id=?",
            (1 if is_running else 0, user_id),
        )
        conn.commit()


def get_settings(user_id: int) -> Optional[sqlite3.Row]:
    with _db_lock, get_db() as conn:
        cur = conn.execute(
            "SELECT m3u_url, server_url, stream_key, channel_id, is_running FROM settings WHERE user_id=?",
            (user_id,),
        )
        return cur.fetchone()


def get_active_streams() -> list[sqlite3.Row]:
    with _db_lock, get_db() as conn:
        cur = conn.execute(
            "SELECT user_id, m3u_url, server_url, stream_key FROM settings WHERE is_running=1"
        )
        return cur.fetchall()


_lang_cache: dict[int, str] = {}


def get_user_language(user_id: int) -> str:
    cached = _lang_cache.get(user_id)
    if cached:
        return cached
    try:
        with _db_lock, get_db() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO users (user_id, language, is_authorized) VALUES (?, ?, 0)",
                (user_id, DEFAULT_LANGUAGE),
            )
            conn.commit()
            cur = conn.execute("SELECT language FROM users WHERE user_id=?", (user_id,))
            row = cur.fetchone()
            lang = row["language"] if row and row["language"] in TRANSLATIONS else DEFAULT_LANGUAGE
            _lang_cache[user_id] = lang
            return lang
    except Exception:
        return DEFAULT_LANGUAGE


def set_user_language(user_id: int, language: str) -> None:
    with _db_lock, get_db() as conn:
        conn.execute(
            "INSERT INTO users (user_id, language, is_authorized) VALUES (?, ?, 0) "
            "ON CONFLICT(user_id) DO UPDATE SET language=excluded.language",
            (user_id, language),
        )
        conn.commit()
    _lang_cache[user_id] = language


def is_user_authorized(user_id: int) -> bool:
    if user_id in ADMIN_IDS:
        return True
    try:
        with _db_lock, get_db() as conn:
            cur = conn.execute("SELECT is_authorized FROM users WHERE user_id=?", (user_id,))
            row = cur.fetchone()
            return bool(row and row["is_authorized"])
    except Exception:
        return False


def authorize_user(user_id: int) -> None:
    with _db_lock, get_db() as conn:
        conn.execute(
            "INSERT INTO users (user_id, language, is_authorized) VALUES (?, ?, 1) "
            "ON CONFLICT(user_id) DO UPDATE SET is_authorized=1",
            (user_id, DEFAULT_LANGUAGE),
        )
        conn.commit()


def revoke_user(user_id: int) -> None:
    with _db_lock, get_db() as conn:
        conn.execute("UPDATE users SET is_authorized=0 WHERE user_id=?", (user_id,))
        conn.commit()


# ---------------------------------------------------------------------------
# التحقق من المدخلات
# ---------------------------------------------------------------------------
def is_valid_stream_url(url: str) -> bool:
    try:
        r = urlparse(url)
        return r.scheme in ("http", "https", "rtmp", "rtmps", "rtsp") and bool(r.netloc)
    except Exception:
        return False


def is_valid_server_url(url: str) -> bool:
    # وجهة FFmpeg بصيغة flv يجب أن تكون RTMP/RTMPS فقط
    try:
        r = urlparse(url)
        return r.scheme in ("rtmp", "rtmps") and bool(r.netloc)
    except Exception:
        return False


def is_valid_channel_id(channel_id: str) -> bool:
    if not channel_id:
        return False
    if channel_id.startswith("@"):
        return len(channel_id) > 1 and all(c.isalnum() or c == "_" for c in channel_id[1:])
    if channel_id.startswith("-100"):
        return channel_id[4:].isdigit() and len(channel_id) >= 10
    if channel_id.lstrip("-").isdigit():
        return True
    return False


def detect_stream_type(url: str) -> str:
    low = url.lower()
    if "youtube.com" in low or "youtu.be" in low:
        return "youtube"
    parsed = urlparse(low)
    domain = parsed.netloc
    path = parsed.path
    if any(rd in domain for rd in RADIO_DOMAINS):
        return "radio"
    if "/radio/" in path:
        return "radio"
    # الاعتماد على path فقط (يتجاهل ?token=...) ، و m3u8 فيديو افتراضياً
    if path.endswith(AUDIO_EXTENSIONS):
        return "radio"
    return "iptv"


def type_label(user_id: int, stream_type: str) -> str:
    return {
        "youtube": t(user_id, "type_youtube"),
        "radio": t(user_id, "type_radio"),
        "iptv": t(user_id, "type_iptv"),
    }.get(stream_type, t(user_id, "type_iptv"))


# ---------------------------------------------------------------------------
# إدارة عمليات FFmpeg (لكل مستخدم)
# ---------------------------------------------------------------------------
@dataclass
class StreamProcess:
    user_id: int
    process: Optional["asyncio.subprocess.Process"]
    task: asyncio.Task
    stop_event: asyncio.Event
    stderr_tail: deque = field(default_factory=lambda: deque(maxlen=8))
    failures: int = 0
    started_at: float = field(default_factory=time.time)


_processes: dict[int, StreamProcess] = {}
_processes_lock = asyncio.Lock()
_notify_bot = None  # يُضبط في post_init لإرسال تنبيهات الأعطال


async def _notify_user(user_id: int, text: str) -> None:
    if _notify_bot is None:
        return
    try:
        await _notify_bot.send_message(chat_id=user_id, text=text, parse_mode=ParseMode.HTML)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"⚠️ notify failed for {user_id}: {e}")


async def _extract_youtube_live_url(yt_url: str) -> Optional[str]:
    """استخراج رابط البث الحي من يوتيوب عبر yt-dlp (list args - آمن)."""
    cmd = ["yt-dlp", "--no-update", "--no-playlist", "-f", "best/b", "-g", yt_url]
    cookies = os.getenv("YTDLP_COOKIES")
    if cookies and os.path.isfile(cookies):
        cmd[1:1] = ["--cookies", cookies]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=60)
        if proc.returncode == 0 and stdout:
            return stdout.decode("utf-8", errors="ignore").strip().splitlines()[0]
        logger.warning(f"⚠️ yt-dlp failed: {stderr.decode('utf-8', errors='ignore')[-300:]}")
    except asyncio.TimeoutError:
        logger.warning(f"⚠️ yt-dlp timeout for {yt_url}")
    except FileNotFoundError:
        logger.error("❌ yt-dlp غير مثبّت")
    except Exception as e:
        logger.error(f"❌ yt-dlp error: {e}")
    return None


_QUIET = ["-nostdin", "-nostats", "-loglevel", "warning"]


def _http_input_opts(url: str, reconnect_at_eof: str = "1", delay: str = "5") -> list[str]:
    """خيارات reconnect/headers تصلح لـ http(s) فقط؛ rtmp/rtsp ترفضها."""
    scheme = urlparse(url).scheme
    if scheme in ("http", "https"):
        return [
            "-headers", f"User-Agent: {DEFAULT_USER_AGENT}\r\n",
            "-timeout", str(FFMPEG_TIMEOUT),
            "-reconnect", "1", "-reconnect_at_eof", reconnect_at_eof,
            "-reconnect_streamed", "1", "-reconnect_delay_max", delay,
        ]
    if scheme == "rtsp":
        return ["-rtsp_transport", "tcp"]
    return []


def _build_ffmpeg_cmd_youtube(live_url: str, full_dest: str) -> list[str]:
    return [
        "ffmpeg", *_QUIET, *_http_input_opts(live_url),
        "-re", "-i", live_url,
        "-c:v", "copy", "-c:a", "aac", "-b:a", "128k", "-ar", "44100",
        "-f", "flv", full_dest,
    ]


def _build_ffmpeg_cmd_radio(m3u_url: str, full_dest: str) -> list[str]:
    return [
        "ffmpeg", *_QUIET, "-f", "lavfi", "-i", "color=c=black:s=640x360:r=10",
        *_http_input_opts(m3u_url),
        "-re", "-i", m3u_url,
        "-map", "0:v:0", "-map", "1:a:0",
        "-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency",
        "-pix_fmt", "yuv420p", "-b:v", "100k", "-g", "20",
        "-c:a", "aac", "-b:a", "128k", "-ar", "44100",
        "-shortest", "-f", "flv", full_dest,
    ]


def _build_ffmpeg_cmd_iptv(m3u_url: str, full_dest: str) -> list[str]:
    opts = _http_input_opts(m3u_url, reconnect_at_eof="0", delay="10")
    if urlparse(m3u_url).path.lower().endswith(".m3u8"):
        opts += ["-live_start_index", "-1"]  # خاص بـ HLS فقط
    return [
        "ffmpeg", *_QUIET, *opts,
        "-i", m3u_url,
        # الصوت يُعاد ترميزه إلى AAC لأن FLV لا يقبل MP2/AC3 الشائعة في IPTV
        "-c:v", "copy", "-c:a", "aac", "-b:a", "128k", "-ar", "44100",
        "-f", "flv", full_dest,
    ]


async def _drain_stderr(proc, sp: Optional[StreamProcess]) -> None:
    """يقرأ stderr باستمرار حتى لا يمتلئ الـ pipe فيتجمد FFmpeg."""
    try:
        while True:
            line = await proc.stderr.readline()
            if not line:
                break
            if sp is not None:
                sp.stderr_tail.append(line.decode("utf-8", errors="ignore").strip()[:200])
    except Exception:  # noqa: BLE001
        pass


async def _stream_worker(
    user_id: int, m3u_url: str, server_url: str, stream_key: str,
    stream_type: str, stop_event: asyncio.Event,
) -> None:
    """عامل بث يعمل في coroutine منفصلة لكل مستخدم."""
    if not server_url.endswith("/"):
        server_url += "/"
    full_dest = f"{server_url}{stream_key}"
    logger.info(f"🚀 Stream worker started for user {user_id} (type={stream_type})")
    failures = 0
    notified = False

    while not stop_event.is_set():
        try:
            if stream_type == "youtube":
                live_url = await _extract_youtube_live_url(m3u_url)
                if not live_url:
                    failures += 1
                    if failures >= NOTIFY_AFTER_FAILURES and not notified:
                        notified = True
                        await _notify_user(user_id, "⚠️ تعذّر استخراج رابط يوتيوب (yt-dlp). تأكد أن البث مباشر وأن yt-dlp محدّث.")
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=min(15, MAX_BACKOFF))
                    except asyncio.TimeoutError:
                        pass
                    continue
                cmd = _build_ffmpeg_cmd_youtube(live_url, full_dest)
            elif stream_type == "radio":
                cmd = _build_ffmpeg_cmd_radio(m3u_url, full_dest)
            else:
                cmd = _build_ffmpeg_cmd_iptv(m3u_url, full_dest)

            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            started = time.time()
            async with _processes_lock:
                sp = _processes.get(user_id)
                if sp:
                    sp.process = proc
                    sp.started_at = started
            drain = asyncio.create_task(_drain_stderr(proc, sp))
            logger.info(f"🎬 FFmpeg PID={proc.pid} running for user {user_id}")

            while not stop_event.is_set():
                try:
                    await asyncio.wait_for(proc.wait(), timeout=2.0)
                    break
                except asyncio.TimeoutError:
                    continue

            if stop_event.is_set():
                if proc.returncode is None:
                    try:
                        proc.terminate()
                        await asyncio.wait_for(proc.wait(), timeout=5)
                    except asyncio.TimeoutError:
                        proc.kill()
                        await proc.wait()
                drain.cancel()
                break

            await asyncio.gather(drain, return_exceptions=True)
            ran_for = time.time() - started
            tail = " | ".join(list(sp.stderr_tail)[-2:]) if sp else ""
            logger.info(f"ℹ️ FFmpeg exited (code {proc.returncode}, {ran_for:.0f}s) for user {user_id}: {tail}")

            if ran_for >= MIN_HEALTHY_RUN:
                failures = 0
                notified = False
            else:
                failures += 1
                if failures >= NOTIFY_AFTER_FAILURES and not notified:
                    notified = True
                    await _notify_user(
                        user_id,
                        "⚠️ البث يفشل بشكل متكرر.\n<code>" + html_escape(tail[-300:] or "no details") + "</code>",
                    )
            if sp:
                sp.failures = failures
            delay = min(RECONNECT_DELAY * (2 ** min(failures, 5)), MAX_BACKOFF) if failures else RECONNECT_DELAY
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass

        except asyncio.CancelledError:
            # تنظيف نظيف عند الإلغاء (لا نغيّر is_running ليُستأنف البث بعد إعادة التشغيل)
            async with _processes_lock:
                sp = _processes.get(user_id)
                proc_ = sp.process if sp else None
            if proc_ and proc_.returncode is None:
                try:
                    proc_.terminate()
                    await asyncio.wait_for(proc_.wait(), timeout=3)
                except Exception:  # noqa: BLE001
                    try:
                        proc_.kill()
                    except Exception:  # noqa: BLE001
                        pass
            raise
        except Exception as e:
            logger.error(f"❌ Stream worker error for user {user_id}: {e}")
            await asyncio.sleep(RECONNECT_DELAY)

    update_status(user_id, False)
    async with _processes_lock:
        cur = _processes.get(user_id)
        if cur is not None and cur.stop_event is stop_event:
            _processes.pop(user_id, None)
    logger.info(f"✅ Stream worker fully stopped for user {user_id}")


async def _stop_existing_stream(user_id: int, timeout: float = 10.0) -> bool:
    """إيقاف أي بث موجود للمستخدم. ينتظر نظيفاً حتى يتوقف العامل."""
    async with _processes_lock:
        sp = _processes.get(user_id)
        if not sp:
            return False
        sp.stop_event.set()
        task_to_wait = sp.task
        # لا نحذف من _processes هنا - العامل سيحذف نفسه عند الانتهاء

    # انتظر خارج القفل لتفادي deadlock
    try:
        await asyncio.wait_for(task_to_wait, timeout=timeout)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        # إذا انتهت المهلة، ألغِ المهمة قسرياً
        task_to_wait.cancel()
        try:
            await asyncio.wait_for(task_to_wait, timeout=3)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            pass
        # تنظيف يدوي
        async with _processes_lock:
            _processes.pop(user_id, None)

    update_status(user_id, False)
    return True


class StreamLimitError(Exception):
    """تجاوز الحد الأقصى للبثوث المتزامنة."""


def is_running_sync(user_id: int) -> bool:
    """فحص خفيف بدون await (آمن لأن كل شيء في حلقة asyncio واحدة)."""
    sp = _processes.get(user_id)
    return sp is not None and not sp.task.done()


def _launch_error_text(user_id: int, e: Exception) -> str:
    if isinstance(e, StreamLimitError):
        return t(user_id, "stream_limit").format(limit=MAX_STREAMS)
    return t(user_id, "stream_failed").format(error=html_escape(str(e)))


async def launch_stream(
    user_id: int, m3u_url: str, server_url: str, stream_key: str,
    force: bool = False,
) -> bool:
    """إطلاق بث جديد للمستخدم.

    Args:
        force: إذا True، يوقف أي بث موجود أولاً ثم يطلق الجديد.
               هذا يحل مشكلة "عدم تبديل البث عند تغيير الرابط".
    """
    # إيقاف البث القديم إذا force=True أو إذا كان يعمل
    if force:
        await _stop_existing_stream(user_id)
    else:
        async with _processes_lock:
            existing = _processes.get(user_id)
            if existing and not existing.task.done():
                return False  # البث يعمل بالفعل

    # حد البثوث المتزامنة (لا يُحتسب بث المستخدم نفسه)
    if MAX_STREAMS > 0:
        async with _processes_lock:
            others = sum(1 for uid, sp in _processes.items() if uid != user_id and not sp.task.done())
        if others >= MAX_STREAMS:
            raise StreamLimitError(str(MAX_STREAMS))

    # إطلاق البث الجديد
    stream_type = detect_stream_type(m3u_url)
    stop_event = asyncio.Event()
    task = asyncio.create_task(
        _stream_worker(user_id, m3u_url, server_url, stream_key, stream_type, stop_event)
    )
    async with _processes_lock:
        _processes[user_id] = StreamProcess(
            user_id=user_id,
            process=None,  # سيُحدَّث داخل العامل
            task=task,
            stop_event=stop_event,
        )
    update_status(user_id, True)
    return True


async def stop_stream_for_user(user_id: int) -> bool:
    """إيقاف بث مستخدم معيّن."""
    return await _stop_existing_stream(user_id)


async def is_stream_running(user_id: int) -> bool:
    """فحص موثوق: يعتمد على task.done() وليس process.returncode (يتفادى race condition)."""
    async with _processes_lock:
        sp = _processes.get(user_id)
        return sp is not None and not sp.task.done()


async def get_stream_pid(user_id: int) -> Optional[int]:
    async with _processes_lock:
        sp = _processes.get(user_id)
        if sp and sp.process and sp.process.returncode is None:
            return sp.process.pid
    return None


# ---------------------------------------------------------------------------
# التحقق من صلاحيات القناة
# ---------------------------------------------------------------------------
async def _probe_source(url: str, stream_type: str) -> tuple[bool, str]:
    """يفحص الرابط عبر ffprobe ويعيد (نجاح, ملخص/خطأ). لا يستخدم shell."""
    target = url
    if stream_type == "youtube":
        target = await _extract_youtube_live_url(url)
        if not target:
            return False, "yt-dlp could not extract a live URL"
    cmd = ["ffprobe", "-v", "error", "-print_format", "json", "-show_streams"]
    if urlparse(target).scheme in ("http", "https"):
        cmd += ["-user_agent", DEFAULT_USER_AGENT, "-rw_timeout", "15000000"]
    cmd.append(target)
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        try:
            out, err = await asyncio.wait_for(proc.communicate(), timeout=PROBE_TIMEOUT)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return False, "timeout"
        if proc.returncode != 0:
            return False, (err.decode("utf-8", errors="ignore").strip().splitlines() or ["ffprobe error"])[-1][:200]
        streams = json.loads(out.decode("utf-8", errors="ignore") or "{}").get("streams", [])
    except FileNotFoundError:
        return False, "ffprobe not installed"
    except Exception as e:  # noqa: BLE001
        return False, str(e)[:200]
    video = next((x for x in streams if x.get("codec_type") == "video"), None)
    audio = next((x for x in streams if x.get("codec_type") == "audio"), None)
    parts = []
    if video:
        parts.append(f"{video.get('codec_name', '?')} {video.get('width', '?')}x{video.get('height', '?')}")
    if audio:
        parts.append(f"audio {audio.get('codec_name', '?')}")
    if not parts:
        return False, "no audio/video streams found"
    if stream_type != "radio" and not video:
        parts.append("(no video)")
    return True, " · ".join(parts)


async def check_bot_permissions(context: ContextTypes.DEFAULT_TYPE, channel_id: str) -> bool:
    try:
        member = await context.bot.get_chat_member(chat_id=channel_id, user_id=context.bot.id)
        if member.status in ("administrator", "creator"):
            if member.status == "creator" or getattr(member, "can_post_messages", None) in (True, None):
                return True
        return False
    except Exception as e:
        logger.error(f"❌ Error checking bot permissions: {e}")
        return False


# ---------------------------------------------------------------------------
# لوحات المفاتيح
# ---------------------------------------------------------------------------
def main_menu_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """لوحة ذكية: تعرض بدء البث إن كان متوقفاً، أو إيقاف/إعادة تشغيل إن كان يعمل."""
    if is_running_sync(user_id):
        stream_row = [
            InlineKeyboardButton(t(user_id, "btn_stop"), callback_data="cmd:stop_stream"),
            InlineKeyboardButton(t(user_id, "btn_restart"), callback_data="cmd:restart_stream"),
        ]
    else:
        stream_row = [InlineKeyboardButton(t(user_id, "btn_start"), callback_data="cmd:start_stream")]
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(t(user_id, "btn_setup"), callback_data="cmd:setup"),
            InlineKeyboardButton(t(user_id, "btn_status"), callback_data="cmd:status"),
        ],
        stream_row,
        [
            InlineKeyboardButton(t(user_id, "btn_language"), callback_data="cmd:language"),
            InlineKeyboardButton(t(user_id, "btn_help"), callback_data="cmd:help"),
        ],
    ])


def status_keyboard(user_id: int) -> InlineKeyboardMarkup:
    rows = [list(r) for r in main_menu_keyboard(user_id).inline_keyboard]
    rows.insert(0, [InlineKeyboardButton(t(user_id, "btn_refresh"), callback_data="cmd:status")])
    return InlineKeyboardMarkup(rows)


def cancel_keyboard(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(t(user_id, "btn_cancel"), callback_data="cmd:cancel")]])


def confirm_cancel_keyboard(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(t(user_id, "btn_test"), callback_data="cmd:test")],
        [
            InlineKeyboardButton(t(user_id, "btn_confirm"), callback_data="cmd:confirm"),
            InlineKeyboardButton(t(user_id, "btn_cancel"), callback_data="cmd:cancel"),
        ],
    ])


def setup_menu_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """لوحة قائمة الإعداد الذكي: reuse / تعديل حقل / إعداد جديد."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(t(user_id, "btn_reuse"), callback_data="setup:reuse")],
        [
            InlineKeyboardButton(t(user_id, "btn_modify_url"), callback_data="setup:modify_url"),
            InlineKeyboardButton(t(user_id, "btn_modify_server"), callback_data="setup:modify_server"),
        ],
        [
            InlineKeyboardButton(t(user_id, "btn_modify_key"), callback_data="setup:modify_key"),
            InlineKeyboardButton(t(user_id, "btn_modify_channel"), callback_data="setup:modify_channel"),
        ],
        [
            InlineKeyboardButton(t(user_id, "btn_new_setup"), callback_data="setup:new"),
            InlineKeyboardButton(t(user_id, "btn_cancel"), callback_data="cmd:cancel"),
        ],
    ])


def language_keyboard(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🇸🇦 العربية", callback_data="lang:ar"),
            InlineKeyboardButton("🇬🇧 English", callback_data="lang:en"),
        ],
        [InlineKeyboardButton(t(user_id, "btn_home"), callback_data="cmd:home")],
    ])


# ---------------------------------------------------------------------------
# Decorator للتفويض + مساعدات الإرسال
# ---------------------------------------------------------------------------
async def safe_edit(target, text: str, **kwargs) -> None:
    """edit_message_text آمن: يتجاهل 'Message is not modified' ويرسل رسالة جديدة إن تعذّر التعديل."""
    try:
        await target.edit_message_text(text, **kwargs)
    except BadRequest as e:
        msg = str(e).lower()
        if "not modified" in msg:
            return
        logger.warning(f"⚠️ edit failed ({e}); sending a new message")
        try:
            await target.message.reply_text(text, **kwargs)
        except Exception as e2:  # noqa: BLE001
            logger.error(f"❌ fallback send failed: {e2}")


def _reply_target(update: Update):
    """الكائن الذي يملك reply_text (CallbackQuery لا يملكها)."""
    if update.callback_query and update.callback_query.message:
        return update.callback_query.message
    return update.message


def authorized_only(handler):
    """Decorator يمنع المستخدمين غير المصرّح لهم."""
    @functools.wraps(handler)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user = update.effective_user
        if not user:
            return
        if not is_user_authorized(user.id):
            if update.callback_query:
                try:
                    await update.callback_query.answer()
                except Exception:
                    pass
            target = _reply_target(update)
            if target:
                try:
                    await target.reply_text(t(user.id, "not_authorized"), parse_mode=ParseMode.HTML)
                except Exception:
                    pass
            return
        return await handler(update, context, *args, **kwargs)
    return wrapper


async def _send_or_edit(update: Update, text: str, reply_markup=None, edit: bool = False) -> None:
    """إرسال رسالة جديدة أو تعديل الحالية حسب السياق."""
    if edit and update.callback_query:
        await safe_edit(
            update.callback_query, text, parse_mode=ParseMode.HTML, reply_markup=reply_markup
        )
    else:
        target = _reply_target(update)
        if target:
            await target.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)


async def _answer_callback(update: Update) -> None:
    """الإجابة على callback_query لمنع تجمد الزر."""
    if update.callback_query:
        try:
            await update.callback_query.answer()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Handlers - القائمة الرئيسية
# ---------------------------------------------------------------------------
@authorized_only
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    get_user_language(user_id)
    await update.message.reply_text(
        t(user_id, "welcome"),
        parse_mode=ParseMode.HTML,
        reply_markup=main_menu_keyboard(user_id),
    )


@authorized_only
async def on_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """معالج أزرار القائمة الرئيسية."""
    query = update.callback_query
    await _answer_callback(update)
    user_id = query.from_user.id
    data = query.data

    if data == "cmd:setup":
        await start_setup_conversation(update, context)
    elif data == "cmd:start_stream":
        await do_start_stream(update, context, edit=True)
    elif data == "cmd:stop_stream":
        await do_stop_stream(update, context, edit=True)
    elif data == "cmd:restart_stream":
        await do_restart_stream(update, context, edit=True)
    elif data == "cmd:status":
        await do_status(update, context, edit=True)
    elif data == "cmd:language":
        await safe_edit(query, 
            t(user_id, "language_select"),
            parse_mode=ParseMode.HTML,
            reply_markup=language_keyboard(user_id),
        )
    elif data == "cmd:help":
        await safe_edit(query, 
            t(user_id, "help_text"),
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu_keyboard(user_id),
        )
    elif data == "cmd:cancel":
        # إلغاء خارج سياق setup
        await safe_edit(query, 
            t(user_id, "setup_cancelled"),
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu_keyboard(user_id),
        )
    elif data == "cmd:confirm":
        await confirm_setup(update, context)
    elif data == "cmd:home":
        await safe_edit(
            query, t(user_id, "welcome"),
            parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard(user_id),
        )
    else:
        # زر قديم/غير معروف (مثل cmd:test خارج المعالج)
        await on_stale_callback(update, context)


@authorized_only
async def on_language_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await _answer_callback(update)
    user_id = query.from_user.id
    lang = query.data.split(":")[1]
    set_user_language(user_id, lang)
    lang_name = t(user_id, "lang_ar" if lang == "ar" else "lang_en")
    await safe_edit(query, 
        t(user_id, "language_changed").format(lang_name=html_escape(lang_name)),
        parse_mode=ParseMode.HTML,
        reply_markup=main_menu_keyboard(user_id),
    )


# ---------------------------------------------------------------------------
# Setup Wizard - مع الإعداد الذكي (إعادة الاستخدام/التعديل الجزئي)
# ---------------------------------------------------------------------------
_setup_drafts: dict[int, dict[str, str]] = {}


def _build_review_message(user_id: int, draft: dict) -> str:
    """بناء رسالة مراجعة الإعدادات (مُستخدمة في reuse و confirm)."""
    stream_type = draft.get("stream_type", detect_stream_type(draft["m3u_url"]))
    return (
        f"{t(user_id, 'setup_review_title')}\n\n"
        f"{t(user_id, 'setup_review_stream_url')}: <code>{html_escape(draft['m3u_url'])}</code>\n"
        f"{t(user_id, 'setup_review_server')}: <code>{html_escape(draft['server_url'])}</code>\n"
        f"{t(user_id, 'setup_review_key')}: <code>{html_escape('•' * min(len(draft['stream_key']), 12))}</code>\n"
        f"{t(user_id, 'setup_review_channel')}: <code>{html_escape(draft['channel_id'])}</code>\n"
        f"{t(user_id, 'setup_review_type')}: <b>{html_escape(type_label(user_id, stream_type))}</b>"
    )


def _build_current_settings_message(user_id: int, settings: sqlite3.Row) -> str:
    """بناء رسالة عرض الإعدادات الحالية المحفوظة."""
    stream_type = detect_stream_type(settings["m3u_url"])
    return (
        f"{t(user_id, 'setup_current_label')}\n\n"
        f"{t(user_id, 'setup_review_stream_url')}: <code>{html_escape(settings['m3u_url'])}</code>\n"
        f"{t(user_id, 'setup_review_server')}: <code>{html_escape(settings['server_url'])}</code>\n"
        f"{t(user_id, 'setup_review_key')}: <code>{html_escape('•' * min(len(settings['stream_key']), 12))}</code>\n"
        f"{t(user_id, 'setup_review_channel')}: <code>{html_escape(settings['channel_id'])}</code>\n"
        f"{t(user_id, 'setup_review_type')}: <b>{html_escape(type_label(user_id, stream_type))}</b>"
    )


@authorized_only
async def start_setup_conversation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """بدء معالج الضبط - ذكي: يفحص الإعدادات الموجودة أولاً."""
    user_id = update.effective_user.id
    existing = get_settings(user_id)

    if existing:
        # لدى المستخدم إعدادات محفوظة → اعرض قائمة reuse/modify
        _setup_drafts[user_id] = {
            "m3u_url": existing["m3u_url"],
            "server_url": existing["server_url"],
            "stream_key": existing["stream_key"],
            "channel_id": existing["channel_id"],
            "stream_type": detect_stream_type(existing["m3u_url"]),
            "modify_mode": False,
        }
        msg = (
            f"{_build_current_settings_message(user_id, existing)}\n\n"
            f"{t(user_id, 'setup_menu_title')}"
        )
        if update.callback_query:
            await safe_edit(update.callback_query, 
                msg, parse_mode=ParseMode.HTML, reply_markup=setup_menu_keyboard(user_id),
            )
        else:
            await update.message.reply_text(
                msg, parse_mode=ParseMode.HTML, reply_markup=setup_menu_keyboard(user_id),
            )
        return STATE_SETUP_MENU
    else:
        # لا توجد إعدادات → ابدأ المعالج الكامل
        _setup_drafts[user_id] = {"modify_mode": False}
        if update.callback_query:
            await safe_edit(update.callback_query, 
                t(user_id, "setup_intro"),
                parse_mode=ParseMode.HTML,
                reply_markup=cancel_keyboard(user_id),
            )
        else:
            await update.message.reply_text(
                t(user_id, "setup_intro"),
                parse_mode=ParseMode.HTML,
                reply_markup=cancel_keyboard(user_id),
            )
        return STATE_STREAM_URL


@authorized_only
async def on_setup_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """معالج أزرار قائمة الإعداد الذكي."""
    query = update.callback_query
    await _answer_callback(update)
    user_id = query.from_user.id
    data = query.data
    draft = _setup_drafts.get(user_id, {})

    if data == "setup:reuse":
        # استخدم الإعدادات المحفوظة كما هي → انتقل للمراجعة
        await safe_edit(query, 
            _build_review_message(user_id, draft),
            parse_mode=ParseMode.HTML,
            reply_markup=confirm_cancel_keyboard(user_id),
        )
        return STATE_CONFIRM

    elif data == "setup:new":
        # ابدأ من الصفر
        _setup_drafts[user_id] = {"modify_mode": False}
        await safe_edit(query, 
            t(user_id, "setup_stream_url_prompt"),
            parse_mode=ParseMode.HTML,
            reply_markup=cancel_keyboard(user_id),
        )
        return STATE_STREAM_URL

    elif data == "setup:modify_url":
        draft["modify_mode"] = True
        draft["modify_field"] = "m3u_url"
        await safe_edit(query, 
            t(user_id, "setup_modify_url_prompt"),
            parse_mode=ParseMode.HTML,
            reply_markup=cancel_keyboard(user_id),
        )
        return STATE_STREAM_URL

    elif data == "setup:modify_server":
        draft["modify_mode"] = True
        draft["modify_field"] = "server_url"
        await safe_edit(query, 
            t(user_id, "setup_modify_server_prompt"),
            parse_mode=ParseMode.HTML,
            reply_markup=cancel_keyboard(user_id),
        )
        return STATE_SERVER_URL

    elif data == "setup:modify_key":
        draft["modify_mode"] = True
        draft["modify_field"] = "stream_key"
        await safe_edit(query, 
            t(user_id, "setup_modify_key_prompt"),
            parse_mode=ParseMode.HTML,
            reply_markup=cancel_keyboard(user_id),
        )
        return STATE_STREAM_KEY

    elif data == "setup:modify_channel":
        draft["modify_mode"] = True
        draft["modify_field"] = "channel_id"
        await safe_edit(query, 
            t(user_id, "setup_modify_channel_prompt"),
            parse_mode=ParseMode.HTML,
            reply_markup=cancel_keyboard(user_id),
        )
        return STATE_CHANNEL_ID

    elif data == "cmd:cancel":
        _setup_drafts.pop(user_id, None)
        await safe_edit(query, 
            t(user_id, "setup_cancelled"),
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu_keyboard(user_id),
        )
        return ConversationHandler.END

    return STATE_SETUP_MENU


async def _session_expired(update: Update, user_id: int) -> int:
    target = _reply_target(update)
    if target:
        try:
            await target.reply_text(
                t(user_id, "session_expired"), parse_mode=ParseMode.HTML,
                reply_markup=main_menu_keyboard(user_id),
            )
        except Exception:  # noqa: BLE001
            pass
    return ConversationHandler.END


@authorized_only
async def test_link(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """زر 🧪: يفحص رابط المصدر بـ ffprobe ثم يعيد شاشة المراجعة."""
    query = update.callback_query
    await _answer_callback(update)
    user_id = query.from_user.id
    draft = _setup_drafts.get(user_id)
    if not draft or "m3u_url" not in draft:
        return await _session_expired(update, user_id)
    await safe_edit(query, t(user_id, "test_running"), parse_mode=ParseMode.HTML)
    stream_type = draft.get("stream_type") or detect_stream_type(draft["m3u_url"])
    ok, info = await _probe_source(draft["m3u_url"], stream_type)
    head = (
        t(user_id, "test_ok").format(summary=html_escape(info)) if ok
        else t(user_id, "test_failed").format(error=html_escape(info))
    )
    await safe_edit(
        query, f"{head}\n\n{_build_review_message(user_id, draft)}",
        parse_mode=ParseMode.HTML, reply_markup=confirm_cancel_keyboard(user_id),
    )
    return STATE_CONFIRM


async def on_conversation_timeout(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
        return
    _setup_drafts.pop(user.id, None)
    chat = update.effective_chat
    if chat:
        try:
            await context.bot.send_message(
                chat_id=chat.id, text=t(user.id, "session_expired"),
                parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard(user.id),
            )
        except Exception:  # noqa: BLE001
            pass


async def on_stale_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """زر قديم (بعد إعادة تشغيل البوت أو انتهاء المحادثة): أجب فوراً وأعد القائمة."""
    query = update.callback_query
    if not query:
        return
    try:
        await query.answer("⌛ انتهت صلاحية هذه الشاشة", show_alert=False)
    except Exception:  # noqa: BLE001
        pass
    user = update.effective_user
    if user and is_user_authorized(user.id):
        await safe_edit(
            query, t(user.id, "welcome"),
            parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard(user.id),
        )


async def _after_field_entered(update: Update, user_id: int, next_state_or_confirm: int) -> int:
    """بعد إدخال حقل في وضع modify، انتقل مباشرة لـ confirm.
    في وضع new كامل، انتقل للحقل التالي."""
    draft = _setup_drafts.get(user_id, {})
    if draft.get("modify_mode"):
        # في وضع التعديل، اعرض المراجعة مباشرة
        await update.message.reply_text(
            _build_review_message(user_id, draft),
            parse_mode=ParseMode.HTML,
            reply_markup=confirm_cancel_keyboard(user_id),
        )
        return STATE_CONFIRM
    else:
        # وضع الإعداد الكامل، انتقل للحقل التالي
        return next_state_or_confirm


@authorized_only
async def on_stream_url(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    text = (update.message.text or "").strip()
    if not is_valid_stream_url(text):
        await update.message.reply_text(
            t(user_id, "setup_invalid_url"),
            parse_mode=ParseMode.HTML,
            reply_markup=cancel_keyboard(user_id),
        )
        return STATE_STREAM_URL
    _setup_drafts.setdefault(user_id, {})["m3u_url"] = text
    _setup_drafts[user_id]["stream_type"] = detect_stream_type(text)

    next_state = await _after_field_entered(update, user_id, STATE_SERVER_URL)
    if next_state == STATE_SERVER_URL:
        await update.message.reply_text(
            t(user_id, "setup_server_url_prompt"),
            parse_mode=ParseMode.HTML,
            reply_markup=cancel_keyboard(user_id),
        )
    return next_state


@authorized_only
async def on_server_url(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    text = (update.message.text or "").strip()
    if not is_valid_server_url(text):
        await update.message.reply_text(
            t(user_id, "setup_invalid_server"),
            parse_mode=ParseMode.HTML,
            reply_markup=cancel_keyboard(user_id),
        )
        return STATE_SERVER_URL
    if user_id not in _setup_drafts:
        return await _session_expired(update, user_id)
    _setup_drafts[user_id]["server_url"] = text

    next_state = await _after_field_entered(update, user_id, STATE_STREAM_KEY)
    if next_state == STATE_STREAM_KEY:
        await update.message.reply_text(
            t(user_id, "setup_stream_key_prompt"),
            parse_mode=ParseMode.HTML,
            reply_markup=cancel_keyboard(user_id),
        )
    return next_state


@authorized_only
async def on_stream_key(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    text = (update.message.text or "").strip()
    if not text:
        await update.message.reply_text(
            t(user_id, "setup_stream_key_prompt"),
            parse_mode=ParseMode.HTML,
            reply_markup=cancel_keyboard(user_id),
        )
        return STATE_STREAM_KEY
    if user_id not in _setup_drafts:
        return await _session_expired(update, user_id)
    _setup_drafts[user_id]["stream_key"] = text
    try:  # لا نترك مفتاح البث ظاهراً في المحادثة
        await update.message.delete()
    except Exception:  # noqa: BLE001
        pass

    next_state = await _after_field_entered(update, user_id, STATE_CHANNEL_ID)
    if next_state == STATE_CHANNEL_ID:
        await update.message.reply_text(
            t(user_id, "setup_channel_id_prompt"),
            parse_mode=ParseMode.HTML,
            reply_markup=cancel_keyboard(user_id),
        )
    return next_state


@authorized_only
async def on_channel_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    text = (update.message.text or "").strip()
    if not is_valid_channel_id(text):
        await update.message.reply_text(
            t(user_id, "setup_invalid_channel"),
            parse_mode=ParseMode.HTML,
            reply_markup=cancel_keyboard(user_id),
        )
        return STATE_CHANNEL_ID
    if user_id not in _setup_drafts:
        return await _session_expired(update, user_id)
    _setup_drafts[user_id]["channel_id"] = text

    # سواء وضع modify أو كامل، انتقل للمراجعة
    await update.message.reply_text(
        _build_review_message(user_id, _setup_drafts[user_id]),
        parse_mode=ParseMode.HTML,
        reply_markup=confirm_cancel_keyboard(user_id),
    )
    return STATE_CONFIRM


@authorized_only
async def confirm_setup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """تأكيد وحفظ الإعدادات. يعيد تشغيل البث تلقائياً إذا كان يعمل."""
    query = update.callback_query
    await _answer_callback(update)
    user_id = query.from_user.id

    draft = _setup_drafts.get(user_id, {})
    if not draft or "m3u_url" not in draft:
        await safe_edit(query, 
            t(user_id, "setup_cancelled"),
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu_keyboard(user_id),
        )
        return ConversationHandler.END

    m3u_url = draft["m3u_url"]
    server_url = draft["server_url"]
    stream_key = draft["stream_key"]
    channel_id = draft["channel_id"]
    stream_type = draft.get("stream_type", "iptv")

    # هل البث كان يعمل قبل الحفظ؟ (لإعادة التشغيل تلقائياً)
    was_running = await is_stream_running(user_id)

    await safe_edit(query, 
        f"{t(user_id, 'setup_checking_perms')}\n"
        f"{t(user_id, 'setup_detected_type').format(type_label=html_escape(type_label(user_id, stream_type)))}",
        parse_mode=ParseMode.HTML,
    )

    if not await check_bot_permissions(context, channel_id):
        # تحذير فقط: القناة لا تُستخدم في البث نفسه، فلا نمنع حفظ الإعدادات
        await context.bot.send_message(
            chat_id=user_id,
            text=t(user_id, "setup_perms_failed"),
            parse_mode=ParseMode.HTML,
        )

    try:
        save_settings(user_id, m3u_url, server_url, stream_key, channel_id)

        # إذا كان البث يعمل، أعد تشغيله بالإعدادات الجديدة
        if was_running:
            await context.bot.send_message(
                chat_id=user_id,
                text=t(user_id, "stream_restarting"),
                parse_mode=ParseMode.HTML,
            )
            try:
                await launch_stream(user_id, m3u_url, server_url, stream_key, force=True)
                final_msg = t(user_id, "setup_saved_restart")
            except Exception as e:
                final_msg = _launch_error_text(user_id, e)
        else:
            final_msg = t(user_id, "setup_saved")

        await context.bot.send_message(
            chat_id=user_id,
            text=final_msg,
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu_keyboard(user_id),
        )
    except Exception as e:
        await context.bot.send_message(
            chat_id=user_id,
            text=_launch_error_text(user_id, e),
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu_keyboard(user_id),
        )
    finally:
        _setup_drafts.pop(user_id, None)
    return ConversationHandler.END


@authorized_only
async def cancel_setup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.callback_query:
        await _answer_callback(update)
        user_id = update.callback_query.from_user.id
        _setup_drafts.pop(user_id, None)
        await safe_edit(update.callback_query, 
            t(user_id, "setup_cancelled"),
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu_keyboard(user_id),
        )
    return ConversationHandler.END


# ---------------------------------------------------------------------------
# أوامر البث
# ---------------------------------------------------------------------------
@authorized_only
async def do_start_stream(update: Update, context: ContextTypes.DEFAULT_TYPE, edit: bool = False) -> None:
    user_id = update.effective_user.id
    settings = get_settings(user_id)

    if not settings:
        await _send_or_edit(update, t(user_id, "stream_no_settings"), main_menu_keyboard(user_id), edit)
        return

    if await is_stream_running(user_id):
        await _send_or_edit(update, t(user_id, "stream_already_running"), main_menu_keyboard(user_id), edit)
        return

    await _send_or_edit(update, t(user_id, "stream_starting"), None, edit)

    try:
        ok = await launch_stream(user_id, settings["m3u_url"], settings["server_url"], settings["stream_key"])
        final_msg = t(user_id, "stream_started") if ok else t(user_id, "stream_already_running")
    except Exception as e:
        final_msg = _launch_error_text(user_id, e)

    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=final_msg,
        parse_mode=ParseMode.HTML,
        reply_markup=main_menu_keyboard(user_id),
    )


@authorized_only
async def do_restart_stream(update: Update, context: ContextTypes.DEFAULT_TYPE, edit: bool = False) -> None:
    """إعادة تشغيل البث: يوقف القديم ويطلق الجديد بنفس الإعدادات (أو الجديدة إن حُفظت)."""
    user_id = update.effective_user.id
    settings = get_settings(user_id)

    if not settings:
        await _send_or_edit(update, t(user_id, "stream_no_settings"), main_menu_keyboard(user_id), edit)
        return

    await _send_or_edit(update, t(user_id, "stream_restarting"), None, edit)

    try:
        ok = await launch_stream(
            user_id, settings["m3u_url"], settings["server_url"], settings["stream_key"], force=True
        )
        final_msg = t(user_id, "stream_restarted") if ok else t(user_id, "stream_failed").format(error="unknown")
    except Exception as e:
        final_msg = _launch_error_text(user_id, e)

    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=final_msg,
        parse_mode=ParseMode.HTML,
        reply_markup=main_menu_keyboard(user_id),
    )


@authorized_only
async def do_stop_stream(update: Update, context: ContextTypes.DEFAULT_TYPE, edit: bool = False) -> None:
    user_id = update.effective_user.id
    stopped = await stop_stream_for_user(user_id)
    msg = t(user_id, "stream_stopped") if stopped else t(user_id, "stream_not_running")
    await _send_or_edit(update, msg, main_menu_keyboard(user_id), edit)


@authorized_only
async def do_status(update: Update, context: ContextTypes.DEFAULT_TYPE, edit: bool = False) -> None:
    user_id = update.effective_user.id
    settings = get_settings(user_id)

    if settings:
        db_status = t(user_id, "status_running") if settings["is_running"] else t(user_id, "status_stopped")
        server = settings["server_url"]
        stream_type = detect_stream_type(settings["m3u_url"])
    else:
        db_status = t(user_id, "status_stopped")
        server = t(user_id, "status_none")
        stream_type = "iptv"

    pid = await get_stream_pid(user_id)
    running = await is_stream_running(user_id)
    proc_status = f"{t(user_id, 'status_active')} (PID {pid})" if running else t(user_id, "status_inactive")

    msg = (
        f"{t(user_id, 'status_title')}\n\n"
        f"{t(user_id, 'status_db')}: {html_escape(db_status)}\n"
        f"{t(user_id, 'status_server')}: <code>{html_escape(server)}</code>\n"
        f"{t(user_id, 'status_type')}: {html_escape(type_label(user_id, stream_type))}\n"
        f"{t(user_id, 'status_process')}: {html_escape(proc_status)}"
    )
    async with _processes_lock:
        sp = _processes.get(user_id)
        if sp and running:
            mins = int((time.time() - sp.started_at) // 60)
            msg += f"\n⏱️ {mins} min"
            if sp.failures:
                msg += f" · ⚠️ {sp.failures}"
            if sp.stderr_tail:
                msg += "\n<code>" + html_escape(" | ".join(list(sp.stderr_tail)[-2:])[-300:]) + "</code>"
    msg += f"\n🕒 {time.strftime('%H:%M:%S')}"  # يمنع 'Message is not modified' عند تكرار الضغط
    await _send_or_edit(update, msg, status_keyboard(user_id), edit)


@authorized_only
async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    await update.message.reply_text(
        t(user_id, "help_text"),
        parse_mode=ParseMode.HTML,
        reply_markup=main_menu_keyboard(user_id),
    )


# ---------------------------------------------------------------------------
# معالج الأخطاء العام
# ---------------------------------------------------------------------------
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """معالج الأخطاء العام - يمنع تجمد البوت عند حدوث استثناء."""
    err = context.error
    if isinstance(err, BadRequest) and "not modified" in str(err).lower():
        return
    logger.error("❌ Exception while handling update:", exc_info=err)

    if isinstance(update, Update) and update.callback_query:
        try:
            await update.callback_query.answer()
        except Exception:
            pass

    if isinstance(update, Update) and update.effective_chat:
        try:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="⚠️ حدث خطأ غير متوقع. حاول مرة أخرى.",
            )
        except Exception:
            pass


# ---------------------------------------------------------------------------
# استئناف البث التلقائي
# ---------------------------------------------------------------------------
async def _post_init(application: Application) -> None:
    global _notify_bot
    _notify_bot = application.bot
    await resume_active_streams(application)


async def _post_shutdown(application: Application) -> None:
    """إيقاف نظيف عند SIGTERM: نلغي العمال دون تصفير is_running ليُستأنف البث عند الإقلاع."""
    async with _processes_lock:
        tasks = [sp.task for sp in _processes.values()]
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    logger.info("🛑 All stream workers cancelled")


async def resume_active_streams(application: Application) -> None:
    try:
        active = get_active_streams()
        if not active:
            logger.info("ℹ️ No active streams to resume")
            return
        logger.info(f"🔄 Resuming {len(active)} active stream(s)...")
        for row in active:
            user_id = row["user_id"]
            try:
                await launch_stream(user_id, row["m3u_url"], row["server_url"], row["stream_key"])
                logger.info(f"🔄 Resumed stream for user {user_id}")
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"❌ Error resuming stream for user {user_id}: {e}")
                update_status(user_id, False)
    except Exception as e:
        logger.error(f"❌ Error in resume_active_streams: {e}")


# ---------------------------------------------------------------------------
# أوامر الإدارة
# ---------------------------------------------------------------------------
async def cmd_authorize(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text(t(user_id, "not_authorized"), parse_mode=ParseMode.HTML)
        return
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usage: /authorize <user_id>")
        return
    target_uid = int(context.args[0])
    authorize_user(target_uid)
    await update.message.reply_text(f"✅ User {target_uid} authorized.")


async def cmd_revoke(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text(t(user_id, "not_authorized"), parse_mode=ParseMode.HTML)
        return
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text(t(user_id, "revoke_usage"))
        return
    target_uid = int(context.args[0])
    if target_uid in ADMIN_IDS:
        await update.message.reply_text(t(user_id, "revoke_admin"))
        return
    revoke_user(target_uid)
    await _stop_existing_stream(target_uid)
    await update.message.reply_text(t(user_id, "revoke_ok").format(uid=target_uid))


# ---------------------------------------------------------------------------
# نقطة الدخول
# ---------------------------------------------------------------------------
def main() -> None:
    init_db()

    bot_token = os.getenv("BOT_TOKEN")
    if not bot_token:
        raise ValueError("❌ BOT_TOKEN environment variable is required")

    if not ADMIN_IDS:
        logger.warning(
            "⚠️ ADMIN_IDS is empty! No one will be able to use the bot. "
            "Set ADMIN_IDS=123456,789012 in .env"
        )

    logger.info("🤖 Starting Streaming Bot (hardened v2.4.0)...")

    application = Application.builder().token(bot_token).build()
    application.add_error_handler(error_handler)

    # ConversationHandler لمعالج /setup مع الحالة الجديدة STATE_SETUP_MENU
    setup_conv = ConversationHandler(
        entry_points=[
            CommandHandler("setup", start_setup_conversation),
            CallbackQueryHandler(start_setup_conversation, pattern="^cmd:setup$"),
        ],
        states={
            STATE_SETUP_MENU: [
                CallbackQueryHandler(on_setup_menu_callback, pattern="^(setup:.+|cmd:cancel)$"),
            ],
            STATE_STREAM_URL: [MessageHandler(filters.TEXT & ~filters.COMMAND, on_stream_url)],
            STATE_SERVER_URL: [MessageHandler(filters.TEXT & ~filters.COMMAND, on_server_url)],
            STATE_STREAM_KEY: [MessageHandler(filters.TEXT & ~filters.COMMAND, on_stream_key)],
            STATE_CHANNEL_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, on_channel_id)],
            STATE_CONFIRM: [
                CallbackQueryHandler(test_link, pattern="^cmd:test$"),
                CallbackQueryHandler(confirm_setup, pattern="^cmd:confirm$"),
                CallbackQueryHandler(cancel_setup, pattern="^cmd:cancel$"),
            ],
            ConversationHandler.TIMEOUT: [TypeHandler(Update, on_conversation_timeout)],
        },
        fallbacks=[
            CommandHandler("cancel", cancel_setup),
            CallbackQueryHandler(cancel_setup, pattern="^cmd:cancel$"),
        ],
        per_user=True,
        per_chat=True,
        name="setup_conversation",
        allow_reentry=True,
        # يحتاج: pip install "python-telegram-bot[job-queue]"؛ بدونه يُعطَّل بأمان
        conversation_timeout=SETUP_TIMEOUT if (application.job_queue and SETUP_TIMEOUT > 0) else None,
    )
    application.add_handler(setup_conv)

    application.add_handler(CommandHandler("authorize", cmd_authorize))
    application.add_handler(CommandHandler("revoke", cmd_revoke))
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("help", cmd_help))
    application.add_handler(CommandHandler("start_stream", do_start_stream))
    application.add_handler(CommandHandler("stop_stream", do_stop_stream))
    application.add_handler(CommandHandler("restart_stream", do_restart_stream))
    application.add_handler(CommandHandler("status", do_status))

    # لاحظ: callbacks تبدأ بـ cmd: أو lang: فقط (setup: يُعالَج داخل ConversationHandler)
    application.add_handler(CallbackQueryHandler(on_language_callback, pattern="^lang:"))
    application.add_handler(CallbackQueryHandler(on_menu_callback, pattern="^cmd:"))
    # أي زر لم يطابقه شيء (setup:* بعد إعادة التشغيل مثلاً) لا يبقى معلقاً
    application.add_handler(CallbackQueryHandler(on_stale_callback))

    application.post_init = _post_init
    application.post_shutdown = _post_shutdown

    logger.info("🤖 Bot is running and listening for commands...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("🛑 Bot stopped by user")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        raise
