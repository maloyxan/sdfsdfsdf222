import asyncio
import aiosqlite
import re
import os
import logging
import aiohttp
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, Dict, Any, List

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, ChatMemberUpdatedFilter, IS_NOT_MEMBER, IS_MEMBER
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    BotCommand, ChatMemberUpdated,
    BotCommandScopeDefault, BotCommandScopeAllPrivateChats, BotCommandScopeAllGroupChats,
    BotCommandScopeChat
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

# ================== НАСТРОЙКА ЛОГИРОВАНИЯ ==================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ================== ВРЕМЯ (МСК = UTC+3) ==================
MSK = timezone(timedelta(hours=3))


def now_msk() -> datetime:
    return datetime.now(MSK)


def fmt_msk(dt: Optional[datetime] = None) -> str:
    if dt is None:
        dt = now_msk()
    return dt.strftime("%d.%m.%Y %H:%M")


def utc_str(dt_aware: datetime) -> str:
    """Строка UTC как SQLite CURRENT_TIMESTAMP: YYYY-MM-DD HH:MM:SS"""
    return dt_aware.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def bounds_today_utc() -> Tuple[str, str]:
    """Границы 'сегодня' по МСК, но в UTC строках для SQLite."""
    msk = now_msk()
    start_msk = msk.replace(hour=0, minute=0, second=0, microsecond=0)
    end_msk = start_msk + timedelta(days=1)
    return utc_str(start_msk), utc_str(end_msk)


def bounds_rolling_utc(days: int) -> Tuple[str, str]:
    """Окно последних N дней по МСК (rolling), границы в UTC строках."""
    end_msk = now_msk()
    start_msk = end_msk - timedelta(days=days)
    return utc_str(start_msk), utc_str(end_msk)


# ================== КОНФИГУРАЦИЯ ==================
# ВАЖНО: НЕ ХАРДКОДЬ токен. Положи в переменную окружения BOT_TOKEN.
BOT_TOKEN = "8482913583:AAG66RQJo8cOOJE98aJ6Iwhmyiru4J0ysVk"

# Настройки супергруппы
SUPERGROUP_ID = -1003650560814

# Темы супергруппы (как было)
GROUP_TOPICS = {
    "profits": 8,   # ТЕПЕРЬ: платежи
    "cash": 7,      # касса
    "welcome": 7    # приветствие
}

# /leadshow работает только в этом thread
LEADSHOW_THREAD_ID = 7

# ID администраторов
ADMIN_IDS = [6731763080]

# Инициализация бота и диспетчера
bot: Optional[Bot] = None
dp = Dispatcher()

# ================== STATES (как было + новые) ==================
class AddMammothStates(StatesGroup):  # mammoths = leads
    waiting_for_mammoth = State()

class CheckMammothStates(StatesGroup):
    waiting_for_mammoth = State()

class AddProfitStates(StatesGroup):  # profits = payments
    waiting_for_worker = State()
    waiting_for_amount = State()   # USD
    waiting_for_percent = State()

class EditContactStates(StatesGroup):
    waiting_for_role = State()
    waiting_for_username = State()

# ================== КУРС USD->RUB (КЭШ + ФОЛБЭК) ==================
_USD_RUB_CACHE_RATE: Optional[float] = None
_USD_RUB_CACHE_TS: Optional[float] = None
_USD_RUB_CACHE_TTL_SECONDS = 600  # 10 минут


async def get_usd_rub_rate(force: bool = False) -> float:
    global _USD_RUB_CACHE_RATE, _USD_RUB_CACHE_TS
    now_ts = datetime.now().timestamp()

    if (
        not force
        and _USD_RUB_CACHE_RATE is not None
        and _USD_RUB_CACHE_TS is not None
        and (now_ts - _USD_RUB_CACHE_TS) < _USD_RUB_CACHE_TTL_SECONDS
    ):
        return float(_USD_RUB_CACHE_RATE)

    # 1) open.er-api.com
    try:
        url = "https://open.er-api.com/v6/latest/USD"
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"open.er-api HTTP {resp.status}")
                data = await resp.json()

        if data.get("result") != "success":
            raise RuntimeError(f"open.er-api result={data.get('result')}")

        rate = float(data["rates"]["RUB"])
        _USD_RUB_CACHE_RATE = rate
        _USD_RUB_CACHE_TS = now_ts
        return rate
    except Exception as e:
        logger.warning(f"open.er-api failed, fallback to CBR: {e}")

    # 2) CBR XML
    try:
        url = "https://www.cbr.ru/scripts/XML_daily.asp"
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"CBR HTTP {resp.status}")
                xml_text = await resp.text()

        root = ET.fromstring(xml_text)
        for valute in root.findall("Valute"):
            code = valute.findtext("CharCode")
            if code == "USD":
                nominal = float(valute.findtext("Nominal").replace(",", "."))
                value = float(valute.findtext("Value").replace(",", "."))
                rate = value / nominal
                _USD_RUB_CACHE_RATE = rate
                _USD_RUB_CACHE_TS = now_ts
                return rate

        raise RuntimeError("USD not found in CBR XML")
    except Exception as e:
        logger.error(f"CBR rate fetch failed: {e}")

    if _USD_RUB_CACHE_RATE is not None:
        return float(_USD_RUB_CACHE_RATE)

    return 80.0


def rub_fmt(value_rub: float) -> str:
    s = f"{value_rub:,.0f}".replace(",", " ")
    return f"{s}Р"


def usd_rub_pair(value_usd: float, rate: float) -> str:
    return f"{value_usd:.2f} $ / {rub_fmt(value_usd * rate)}"


# ================== DB ==================
async def init_db():
    async with aiosqlite.connect('workers.db') as db:
        # workers
        await db.execute('''
            CREATE TABLE IF NOT EXISTS workers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER UNIQUE,
                username TEXT,
                is_admin BOOLEAN DEFAULT 0,
                registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # applications
        await db.execute('''
            CREATE TABLE IF NOT EXISTS applications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER UNIQUE,
                username TEXT,
                application_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'pending'
            )
        ''')

        # mammoths (лиды)
        await db.execute('''
            CREATE TABLE IF NOT EXISTS mammoths (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER UNIQUE,
                username TEXT,
                worker_id INTEGER,
                added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (worker_id) REFERENCES workers (id)
            )
        ''')

        # profits (платежи) - amount и worker_amount в USD
        await db.execute('''
            CREATE TABLE IF NOT EXISTS profits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                worker_id INTEGER,
                amount REAL,
                percent INTEGER,
                worker_amount REAL,
                date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (worker_id) REFERENCES workers (id)
            )
        ''')

        # contacts
        await db.execute('''
            CREATE TABLE IF NOT EXISTS contacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                role TEXT UNIQUE,
                username TEXT
            )
        ''')

        # init contacts (как было, но buyer=менеджер, shop=магазин по смыслу)
        await db.execute('''
            INSERT OR IGNORE INTO contacts (role, username) VALUES 
            ('owner', '@owner'),
            ('buyer', '@manager'),
            ('curator', '@curator'),
            ('shop', '@store')
        ''')

        # миграция для RUB полей и fx_rate
        cur = await db.execute("PRAGMA table_info(profits)")
        cols = [row[1] for row in await cur.fetchall()]

        if "fx_rate" not in cols:
            await db.execute("ALTER TABLE profits ADD COLUMN fx_rate REAL")
        if "amount_rub" not in cols:
            await db.execute("ALTER TABLE profits ADD COLUMN amount_rub REAL")
        if "worker_amount_rub" not in cols:
            await db.execute("ALTER TABLE profits ADD COLUMN worker_amount_rub REAL")
        if "project_amount_rub" not in cols:
            await db.execute("ALTER TABLE profits ADD COLUMN project_amount_rub REAL")

        # backfill
        await db.execute("""
            UPDATE profits
            SET amount_rub = amount * fx_rate
            WHERE amount_rub IS NULL AND fx_rate IS NOT NULL AND amount IS NOT NULL
        """)
        await db.execute("""
            UPDATE profits
            SET worker_amount_rub = worker_amount * fx_rate
            WHERE worker_amount_rub IS NULL AND fx_rate IS NOT NULL AND worker_amount IS NOT NULL
        """)
        await db.execute("""
            UPDATE profits
            SET project_amount_rub = (amount - worker_amount) * fx_rate
            WHERE project_amount_rub IS NULL AND fx_rate IS NOT NULL AND amount IS NOT NULL AND worker_amount IS NOT NULL
        """)

        # add admins
        for admin_id in ADMIN_IDS:
            await db.execute(
                'INSERT OR IGNORE INTO workers (user_id, username, is_admin) VALUES (?, ?, ?)',
                (admin_id, f'@admin_{admin_id}', 1)
            )

        await db.commit()


async def get_user_info(user_id: int):
    async with aiosqlite.connect('workers.db') as db:
        cur = await db.execute(
            'SELECT id, username, is_admin, registration_date FROM workers WHERE user_id = ?',
            (user_id,)
        )
        return await cur.fetchone()


async def get_application(user_id: int):
    async with aiosqlite.connect('workers.db') as db:
        cur = await db.execute(
            'SELECT id, status FROM applications WHERE user_id = ?',
            (user_id,)
        )
        return await cur.fetchone()


async def add_worker(user_id: int, username: str, is_admin: bool = False):
    async with aiosqlite.connect('workers.db') as db:
        try:
            await db.execute(
                'INSERT OR REPLACE INTO workers (user_id, username, is_admin) VALUES (?, ?, ?)',
                (user_id, username, is_admin)
            )
            await db.commit()
            return True
        except Exception as e:
            logger.error(f"Error adding worker: {e}")
            return False


async def create_application(user_id: int, username: str):
    async with aiosqlite.connect('workers.db') as db:
        try:
            await db.execute(
                'INSERT OR REPLACE INTO applications (user_id, username) VALUES (?, ?)',
                (user_id, username)
            )
            await db.commit()
            return True
        except Exception as e:
            logger.error(f"Error creating application: {e}")
            return False


async def accept_application(application_id: int):
    async with aiosqlite.connect('workers.db') as db:
        try:
            cur = await db.execute(
                'SELECT user_id, username FROM applications WHERE id = ?',
                (application_id,)
            )
            application = await cur.fetchone()
            if not application:
                return False, None, None

            user_id, username = application

            await db.execute(
                'INSERT OR REPLACE INTO workers (user_id, username) VALUES (?, ?)',
                (user_id, username)
            )
            await db.execute('DELETE FROM applications WHERE id = ?', (application_id,))
            await db.commit()
            return True, user_id, username
        except Exception as e:
            logger.error(f"Error accepting application: {e}")
            return False, None, None


async def reject_application(application_id: int):
    async with aiosqlite.connect('workers.db') as db:
        try:
            cur = await db.execute(
                'SELECT user_id FROM applications WHERE id = ?',
                (application_id,)
            )
            application = await cur.fetchone()
            if not application:
                return False, None

            user_id = application[0]
            await db.execute('DELETE FROM applications WHERE id = ?', (application_id,))
            await db.commit()
            return True, user_id
        except Exception as e:
            logger.error(f"Error rejecting application: {e}")
            return False, None


async def get_pending_applications():
    async with aiosqlite.connect('workers.db') as db:
        cur = await db.execute(
            'SELECT id, user_id, username, application_date FROM applications ORDER BY application_date'
        )
        return await cur.fetchall()


def normalize_lead_identifier(text: str) -> str:
    t = text.strip()
    if t.isdigit():
        return t
    if not t.startswith("@"):
        t = "@" + t
    return t


async def add_mammoth(mammoth_identifier: str, worker_id: int):
    mammoth_identifier = normalize_lead_identifier(mammoth_identifier)

    async with aiosqlite.connect('workers.db') as db:
        try:
            cur = await db.execute(
                'SELECT worker_id FROM mammoths WHERE user_id = ? OR username = ?',
                (mammoth_identifier, mammoth_identifier)
            )
            existing = await cur.fetchone()
            if existing:
                return False, existing[0]

            await db.execute(
                'INSERT INTO mammoths (user_id, username, worker_id) VALUES (?, ?, ?)',
                (mammoth_identifier if mammoth_identifier.isdigit() else None,
                 mammoth_identifier if not mammoth_identifier.isdigit() else None,
                 worker_id)
            )
            await db.commit()
            return True, worker_id
        except Exception as e:
            logger.error(f"Error adding lead: {e}")
            return False, None


async def check_mammoth(mammoth_identifier: str):
    mammoth_identifier = normalize_lead_identifier(mammoth_identifier)

    async with aiosqlite.connect('workers.db') as db:
        cur = await db.execute('''
            SELECT m.user_id, m.username, w.username 
            FROM mammoths m
            LEFT JOIN workers w ON m.worker_id = w.id
            WHERE m.user_id = ? OR m.username = ?
        ''', (mammoth_identifier, mammoth_identifier))
        return await cur.fetchone()


async def get_contacts():
    async with aiosqlite.connect('workers.db') as db:
        cur = await db.execute('SELECT role, username FROM contacts')
        rows = await cur.fetchall()
        return {row[0]: row[1] for row in rows}


async def update_contact(role: str, username: str):
    async with aiosqlite.connect('workers.db') as db:
        await db.execute(
            'UPDATE contacts SET username = ? WHERE role = ?',
            (username, role)
        )
        await db.commit()


async def add_profit(worker_username: str, amount_usd: float, percent: int):
    """
    Добавление платежа (USD) + сохранение RUB по актуальному курсу.
    """
    async with aiosqlite.connect('workers.db') as db:
        try:
            wu = worker_username.strip()
            if wu.startswith('@'):
                wu = wu[1:]

            cur = await db.execute(
                'SELECT id FROM workers WHERE username LIKE ?',
                (f'%{wu}%',)
            )
            worker = await cur.fetchone()

            if not worker and wu.isdigit():
                cur = await db.execute(
                    'SELECT id FROM workers WHERE user_id = ?',
                    (int(wu),)
                )
                worker = await cur.fetchone()

            if not worker:
                return False, "Воркер не найден"

            worker_id = worker[0]
            rate = await get_usd_rub_rate()

            worker_amount_usd = amount_usd * (percent / 100.0)
            project_amount_usd = amount_usd - worker_amount_usd

            amount_rub = amount_usd * rate
            worker_amount_rub = worker_amount_usd * rate
            project_amount_rub = project_amount_usd * rate

            await db.execute(
                'INSERT INTO profits (worker_id, amount, percent, worker_amount, fx_rate, amount_rub, worker_amount_rub, project_amount_rub) '
                'VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                (worker_id, amount_usd, percent, worker_amount_usd, rate, amount_rub, worker_amount_rub, project_amount_rub)
            )
            await db.commit()
            return True, {
                "rate": rate,
                "worker_amount_usd": worker_amount_usd,
                "project_amount_usd": project_amount_usd,
                "amount_rub": amount_rub,
                "worker_amount_rub": worker_amount_rub,
                "project_amount_rub": project_amount_rub
            }
        except Exception as e:
            logger.error(f"Error adding payment: {e}")
            return False, str(e)


async def get_cash_stats(period: str = None) -> Tuple[float, float]:
    """
    Касса проекта в USD+RUB.
    Периоды считаются по МСК (UTC+3) корректно, несмотря на UTC в SQLite.
    """
    rate = await get_usd_rub_rate()
    where = ""
    params: List[Any] = [rate]  # 1-й ? всегда для COALESCE(fx_rate, ?)

    if period == 'today':
        start_utc, end_utc = bounds_today_utc()
        where = "WHERE datetime(date) >= datetime(?) AND datetime(date) < datetime(?)"
        params.extend([start_utc, end_utc])
    elif period == 'week':
        start_utc, end_utc = bounds_rolling_utc(7)
        where = "WHERE datetime(date) >= datetime(?) AND datetime(date) < datetime(?)"
        params.extend([start_utc, end_utc])
    elif period == 'month':
        start_utc, end_utc = bounds_rolling_utc(30)
        where = "WHERE datetime(date) >= datetime(?) AND datetime(date) < datetime(?)"
        params.extend([start_utc, end_utc])
    else:
        where = ""
        # только rate

    query = f"""
        SELECT
            COALESCE(SUM(amount), 0) AS usd_sum,
            COALESCE(SUM(COALESCE(amount_rub, amount * COALESCE(fx_rate, ?))), 0) AS rub_sum
        FROM profits
        {where}
    """

    async with aiosqlite.connect('workers.db') as db:
        cur = await db.execute(query, tuple(params))
        usd_sum, rub_sum = await cur.fetchone()
        return float(usd_sum or 0), float(rub_sum or 0)


async def get_project_stats():
    rate = await get_usd_rub_rate()
    async with aiosqlite.connect('workers.db') as db:
        cur = await db.execute('SELECT COUNT(*) FROM mammoths')
        mammoth_count = (await cur.fetchone())[0]

        cur = await db.execute('SELECT COUNT(*) FROM workers WHERE is_admin = 0')
        worker_count = (await cur.fetchone())[0]

        cur = await db.execute('SELECT COUNT(*) FROM applications')
        pending_applications = (await cur.fetchone())[0]

        cur = await db.execute('SELECT COALESCE(SUM(amount), 0) FROM profits')
        total_usd = float((await cur.fetchone())[0] or 0)

        cur = await db.execute('SELECT COALESCE(SUM(worker_amount), 0) FROM profits')
        payouts_usd = float((await cur.fetchone())[0] or 0)

        cur = await db.execute(
            'SELECT COALESCE(SUM(COALESCE(amount_rub, amount * COALESCE(fx_rate, ?))), 0) FROM profits',
            (rate,)
        )
        total_rub = float((await cur.fetchone())[0] or 0)

        cur = await db.execute(
            'SELECT COALESCE(SUM(COALESCE(worker_amount_rub, worker_amount * COALESCE(fx_rate, ?))), 0) FROM profits',
            (rate,)
        )
        payouts_rub = float((await cur.fetchone())[0] or 0)

        return {
            'mammoths': mammoth_count,  # лиды
            'workers': worker_count,
            'pending_applications': pending_applications,
            'total_profits_usd': total_usd,       # платежи USD
            'total_profits_rub': total_rub,       # платежи RUB
            'total_payouts_usd': payouts_usd,
            'total_payouts_rub': payouts_rub,
            'project_income_usd': total_usd - payouts_usd,
            'project_income_rub': total_rub - payouts_rub
        }


async def get_all_workers():
    async with aiosqlite.connect('workers.db') as db:
        cur = await db.execute('SELECT username, user_id FROM workers WHERE is_admin = 0')
        return await cur.fetchall()


async def get_all_admins():
    async with aiosqlite.connect('workers.db') as db:
        cur = await db.execute('SELECT user_id FROM workers WHERE is_admin = 1')
        rows = await cur.fetchall()
        return [row[0] for row in rows]


async def get_worker_profile_stats(user_id: int) -> Optional[Dict[str, Any]]:
    """
    /profile:
    - id профиля = user_id
    - платежи: день/месяц/всё время (по МСК)
    - доля от всех платежей
    """
    rate = await get_usd_rub_rate()
    async with aiosqlite.connect("workers.db") as db:
        cur = await db.execute(
            "SELECT id, username, registration_date FROM workers WHERE user_id = ?",
            (user_id,)
        )
        w = await cur.fetchone()
        if not w:
            return None

        worker_db_id, username, reg_date = w

        # day (today msk bounds)
        day_start_utc, day_end_utc = bounds_today_utc()
        cur = await db.execute("""
            SELECT
              COUNT(*),
              COALESCE(SUM(amount), 0),
              COALESCE(SUM(COALESCE(amount_rub, amount * COALESCE(fx_rate, ?))), 0)
            FROM profits
            WHERE worker_id = ?
              AND datetime(date) >= datetime(?)
              AND datetime(date) <  datetime(?)
        """, (rate, worker_db_id, day_start_utc, day_end_utc))
        day_cnt, day_usd, day_rub = await cur.fetchone()

        # month (rolling 30)
        mon_start_utc, mon_end_utc = bounds_rolling_utc(30)
        cur = await db.execute("""
            SELECT
              COUNT(*),
              COALESCE(SUM(amount), 0),
              COALESCE(SUM(COALESCE(amount_rub, amount * COALESCE(fx_rate, ?))), 0)
            FROM profits
            WHERE worker_id = ?
              AND datetime(date) >= datetime(?)
              AND datetime(date) <  datetime(?)
        """, (rate, worker_db_id, mon_start_utc, mon_end_utc))
        mon_cnt, mon_usd, mon_rub = await cur.fetchone()

        # all
        cur = await db.execute("""
            SELECT
              COUNT(*),
              COALESCE(SUM(amount), 0),
              COALESCE(SUM(COALESCE(amount_rub, amount * COALESCE(fx_rate, ?))), 0)
            FROM profits
            WHERE worker_id = ?
        """, (rate, worker_db_id))
        all_cnt, all_usd, all_rub = await cur.fetchone()

        # total project usd
        cur = await db.execute("SELECT COALESCE(SUM(amount), 0) FROM profits")
        total_usd = float((await cur.fetchone())[0] or 0)
        share = 0.0 if total_usd == 0 else (float(all_usd) / total_usd) * 100.0

        return {
            "user_id": user_id,
            "username": username,
            "registration_date": reg_date,
            "day_cnt": int(day_cnt),
            "day_usd": float(day_usd),
            "day_rub": float(day_rub),
            "mon_cnt": int(mon_cnt),
            "mon_usd": float(mon_usd),
            "mon_rub": float(mon_rub),
            "all_cnt": int(all_cnt),
            "all_usd": float(all_usd),
            "all_rub": float(all_rub),
            "share": float(share),
        }


async def get_top_workers(period: str = "day", limit: int = 10) -> List[tuple]:
    """
    period: day | month
    возвращает: (username, user_id, count, sum_usd, sum_rub)
    """
    rate = await get_usd_rub_rate()

    where = "WHERE w.is_admin = 0"
    params: List[Any] = [rate]

    if period == "day":
        start_utc, end_utc = bounds_today_utc()
        where += " AND datetime(p.date) >= datetime(?) AND datetime(p.date) < datetime(?)"
        params.extend([start_utc, end_utc])
    elif period == "month":
        start_utc, end_utc = bounds_rolling_utc(30)
        where += " AND datetime(p.date) >= datetime(?) AND datetime(p.date) < datetime(?)"
        params.extend([start_utc, end_utc])

    query = f"""
        SELECT
            COALESCE(w.username, CAST(w.user_id AS TEXT)) AS worker_display,
            w.user_id,
            COUNT(p.id) AS payments_count,
            COALESCE(SUM(p.amount), 0) AS sum_usd,
            COALESCE(SUM(COALESCE(p.amount_rub, p.amount * COALESCE(p.fx_rate, ?))), 0) AS sum_rub
        FROM profits p
        JOIN workers w ON p.worker_id = w.id
        {where}
        GROUP BY w.id
        HAVING sum_usd > 0
        ORDER BY sum_usd DESC
        LIMIT ?
    """
    params.append(limit)

    async with aiosqlite.connect("workers.db") as db:
        cur = await db.execute(query, tuple(params))
        return await cur.fetchall()


# ================== СУПЕРГРУППА / ОТПРАВКА В ТЕМЫ ==================
async def send_to_topic(thread_id: Optional[int], text: str, parse_mode: Optional[str] = None):
    global bot
    try:
        if bot is None:
            return False
        if thread_id is None:
            await bot.send_message(chat_id=SUPERGROUP_ID, text=text, parse_mode=parse_mode)
        else:
            await bot.send_message(chat_id=SUPERGROUP_ID, message_thread_id=thread_id, text=text, parse_mode=parse_mode)
        return True
    except Exception as e:
        logger.error(f"Error sending to topic {thread_id}: {e}")
        return False


async def add_profit_to_channel(profit_message: str):
    return await send_to_topic(GROUP_TOPICS["profits"], profit_message, "Markdown")


async def send_cash_to_channel(cash_message: str):
    return await send_to_topic(GROUP_TOPICS["cash"], cash_message, "Markdown")


async def send_welcome_message(user):
    welcome_text = (
        f"👋 Приветствуем в HELLCASH TEAM, {user.first_name}!\n\n"
        f"Мы - авторы лучших направлений. У нас ты получишь:\n"
        f"• Поддержку в дружном чате\n"
        f"• Советы от профессиональной команды и кураторов\n"
        f"• Стратегию для выхода на $500+ в первую неделю (при соблюдении всех советов).\n\n"
        f"🔥 Совет от бывалых: Если ты полный ноль — не бойся брать куратора! Это инвестиция, которая окупится в разы.\n\n"
        f"Все команды и возможности — в /help\n\n"
        f"Рады видеть в команде!"
    )
    thread_id = GROUP_TOPICS.get("welcome")
    return await send_to_topic(thread_id, welcome_text)


# ================== ФОРМАТИРОВАНИЕ ==================
def get_contacts_message(contacts: dict) -> str:
    return (
        "<b>📞 КОНТАКТЫ</b>\n\n"
        f"👑 Владелец: {contacts.get('owner', 'Не указан')}\n"
        f"💼 Менеджер: {contacts.get('buyer', 'Не указан')}\n"
        f"👨‍💼 Куратор: {contacts.get('curator', 'Не указан')}\n"
        f"🛒 Магазин: {contacts.get('shop', 'Не указан')}\n\n"
        "➖➖➖➖➖➖➖➖"
    )


def escape_markdown(text: str) -> str:
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)


def format_top_message(rows: List[tuple], title: str) -> str:
    if not rows:
        return f"🏆 *{title}*\n\nПока нет данных.\n\n➖➖➖➖➖➖➖➖"
    lines = [f"🏆 *{title}*\n"]
    for i, (display, _uid, cnt, sum_usd, sum_rub) in enumerate(rows, start=1):
        lines.append(f"{i}. {display} — *{float(sum_usd):.2f} $* / *{rub_fmt(float(sum_rub))}* _(платежей: {cnt})_")
    lines.append("\n➖➖➖➖➖➖➖➖")
    return "\n".join(lines)


# ================== КЛАВИАТУРЫ ==================
def get_main_keyboard(is_admin: bool = False):
    keyboard = [
        [InlineKeyboardButton(text="➕ Добавить мамонта", callback_data="add_mammoth")],
        [InlineKeyboardButton(text="🔍 Проверить мамонта", callback_data="check_mammoth")],
        [InlineKeyboardButton(text="💰 Касса", callback_data="cashbox")],
        [InlineKeyboardButton(text="📞 Контакты", callback_data="contacts")],
        [InlineKeyboardButton(text="📚 Мануалы", callback_data="manuals")],
        [InlineKeyboardButton(text="🌐 Проценты", callback_data="kurs")],
        [InlineKeyboardButton(text="👤 Профиль", callback_data="profile")],
        [InlineKeyboardButton(text="🏆 Топ (сегодня)", callback_data="top_day")],
        [InlineKeyboardButton(text="🏆 Топ (месяц)", callback_data="top_month")],
    ]

    if is_admin:
        keyboard.extend([
            [InlineKeyboardButton(text="💳 Добавить платеж", callback_data="add_profit")],
            [InlineKeyboardButton(text="✏️ Изменить контакт", callback_data="edit_contact")],
            [InlineKeyboardButton(text="📊 Статистика проекта", callback_data="project_stats")],
            [InlineKeyboardButton(text="📝 Заявки", callback_data="view_applications")]
        ])

    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def get_application_keyboard(user_id: int):
    keyboard = [[
        InlineKeyboardButton(text="✅ Принять", callback_data=f"accept_application_{user_id}"),
        InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_application_{user_id}")
    ]]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def get_contacts_keyboard():
    keyboard = [
        [InlineKeyboardButton(text="👑 Владелец", callback_data="edit_owner")],
        [InlineKeyboardButton(text="💼 Менеджер", callback_data="edit_buyer")],
        [InlineKeyboardButton(text="👨‍💼 Куратор", callback_data="edit_curator")],
        [InlineKeyboardButton(text="🛒 Магазин", callback_data="edit_shop")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def get_application_decision_keyboard():
    keyboard = [
        [InlineKeyboardButton(text="📝 Подать заявку", callback_data="submit_application")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def get_workers_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")]
        ]
    )

# ================== КОМАНДЫ БОТА (MENU) ==================
async def setup_bot_commands():
    global bot
    if bot is None:
        return

    # ВАЖНО: command БЕЗ "/" !
    bot_commands = [
        BotCommand(command="start", description="Начать работу с ботом"),
        BotCommand(command="contacts", description="Показать контакты"),
        BotCommand(command="cash", description="Показать кассу проекта"),
        #BotCommand(command="stats", description="Статистика проекта (для админов)"),
        #BotCommand(command="getid", description="Показать ID чата"),
        BotCommand(command="help", description="Помощь по использованию бота"),

        BotCommand(command="manuals", description="Показать все мануалы"),
        BotCommand(command="kurs", description="Текущие проценты + курс USD/RUB"),
        BotCommand(command="profile", description="Профиль воркера"),
        BotCommand(command="top_day", description="Топ воркеров за сегодня"),
        BotCommand(command="top_month", description="Топ воркеров за месяц"),
        BotCommand(command="leadshow", description="Количество мамонтов (только thread 7)"),

        #BotCommand(command="test", description="Тест (админы)"),
        #BotCommand(command="kassa", description="Касса в теме (группа)"),
    ]

    # ставим везде + отдельно в конкретный чат (часто решает “не отображается”)
    await bot.set_my_commands(bot_commands, scope=BotCommandScopeDefault())
    await bot.set_my_commands(bot_commands, scope=BotCommandScopeAllPrivateChats())
    await bot.set_my_commands(bot_commands, scope=BotCommandScopeAllGroupChats())
    await bot.set_my_commands(bot_commands, scope=BotCommandScopeChat(chat_id=SUPERGROUP_ID))

    logger.info("Команды бота установлены (default/private/group/chat)")

# ================== WELCOME ==================
@dp.chat_member(ChatMemberUpdatedFilter(member_status_changed=IS_NOT_MEMBER >> IS_MEMBER))
async def on_new_member(event: ChatMemberUpdated):
    if event.chat.id != SUPERGROUP_ID:
        return
    if event.new_chat_member.user.is_bot:
        return

    try:
        await send_welcome_message(event.new_chat_member.user)
        logger.info("Welcome sent")
    except Exception as e:
        logger.error(f"Welcome error: {e}")

# ================== COMMAND HANDLERS ==================
@dp.message(Command("start"))
async def cmd_start(message: Message):
    user_id = message.from_user.id
    username = message.from_user.username or f"user_{user_id}"
    display_username = f"@{username}" if message.from_user.username else str(user_id)

    user_info = await get_user_info(user_id)

    if user_info:
        is_admin = bool(user_info[2])
        await message.answer(
            "🤖 Добро пожаловать в систему управления!\n\n"
            "📋 Используйте кнопки ниже или команды:\n"
            "/contacts - контакты\n"
            "/cash - касса\n"
            "/manuals - мануалы\n"
            "/kurs - проценты + курс\n"
            "/profile - профиль\n"
            "/top_day /top_month - топы\n"
            "/leadshow - количество мамонтов (только thread 7)\n"
            "/help - помощь\n",
            reply_markup=get_main_keyboard(is_admin)
        )
    else:
        application = await get_application(user_id)

        if application:
            status = application[1]
            if status == 'pending':
                await message.answer(
                    "⏳ Ваша заявка уже отправлена и находится на рассмотрении.\n"
                    "Ожидайте решения администратора."
                )
            elif status == 'rejected':
                await message.answer(
                    "❌ Ваша заявка была отклонена.\n"
                    "Вы можете подать заявку снова, если хотите.",
                    reply_markup=get_application_decision_keyboard()
                )
        else:
            await message.answer(
                "🚫 Вас нет в базе данных воркеров.\n"
                "Хотите подать заявку на присоединение к команде?",
                reply_markup=get_application_decision_keyboard()
            )


@dp.message(Command("contacts"))
async def cmd_contacts(message: Message):
    contacts = await get_contacts()
    contacts_message = get_contacts_message(contacts)
    await message.answer(contacts_message, parse_mode="HTML")


@dp.message(Command("manuals"))
async def cmd_manuals(message: Message):
    text = (
        "<b>📚 Приступай к работе:</b>\n"
        "• ┣1.1 📌 <a href=\"https://telegra.ph/CHto-ty-sdelal-dlya-horoshej-zhizni--HellCash-Team-01-07\">Введение. Путь к успеху.</a>\n"
        "• ┣1.2 📖 <a href=\"https://teletype.in/@manualshikes/GVhBCQVXU_T\">Суть ворка. Как завести мамонта.</a>\n"
        "• ┣1.3 🦣 <a href=\"https://telegra.ph/Gde-iskat-trafik-01-07\">Мамонты. Поиск трафика.</a>\n"
        "• ┣1.4 ❗️ <a href=\"https://teletype.in/@manualshikes/tBMSrxqiOaj\">Мануал для мамонтов.</a>\n"
        "• ┣1.5 ♟ <a href=\"https://telegra.ph/Priemy-dlya-obshcheniya-Manipulyaciya-01-07\">Манипуляция. Приемы для общения.</a>\n"
    )
    await message.answer(text, parse_mode="HTML", disable_web_page_preview=True)


@dp.message(Command("kurs"))
async def cmd_kurs(message: Message):
    rate = await get_usd_rub_rate()
    text = (
        "🌐 *Проценты:*\n"
        "┣ Основной депозит - 70%\n"
        "┣ С ТП - 55%\n"
        "┣ Ограничение - 75%\n\n"
        f"💵 Курсы валют USD: *{rate:.2f}* RUB"
    )
    await message.answer(text, parse_mode="Markdown")


@dp.message(Command("profile"))
async def cmd_profile(message: Message):
    s = await get_worker_profile_stats(message.from_user.id)
    if not s:
        await message.answer("❌ Профиль не найден. Используйте /start.")
        return

    username_display = s["username"] or (f"@{message.from_user.username}" if message.from_user.username else str(message.from_user.id))

    text = (
        f"🆔 ID профиля: {s['user_id']}\n"
        f"👤 Пользователь: {username_display}\n"
        f"📅 Дата вступления: {s['registration_date']}\n\n"
        f"🤑 Ваши платежи\n"
        f"├ За день: {s['day_cnt']} платеж(ей) на сумму {s['day_usd']:.2f} $ / {rub_fmt(s['day_rub'])}\n"
        f"├ За месяц: {s['mon_cnt']} платеж(ей) на сумму {s['mon_usd']:.2f} $ / {rub_fmt(s['mon_rub'])}\n"
        f"└ За все время: {s['all_cnt']} платеж(ей) на сумму {s['all_usd']:.2f} $ / {rub_fmt(s['all_rub'])}\n\n"
        f"🏆 За все время {username_display} сделал ~{s['share']:.2f}% от суммы всех залетов"
    )
    await message.answer(text)


@dp.message(Command("top_day"))
async def cmd_top_day(message: Message):
    rows = await get_top_workers("day", 10)
    await message.answer(format_top_message(rows, "ТОП ВОРКЕРОВ ЗА СЕГОДНЯ (МСК)"), parse_mode="Markdown")


@dp.message(Command("top_month"))
async def cmd_top_month(message: Message):
    rows = await get_top_workers("month", 10)
    await message.answer(format_top_message(rows, "ТОП ВОРКЕРОВ ЗА МЕСЯЦ"), parse_mode="Markdown")


@dp.message(Command("leadshow"))
async def cmd_leadshow(message: Message):
    # только супергруппа + только thread 7
    if message.chat.id != SUPERGROUP_ID or message.message_thread_id != LEADSHOW_THREAD_ID:
        await message.reply("❌ Эта команда доступна только в теме #7.")
        return

    user_info = await get_user_info(message.from_user.id)
    if not user_info:
        await message.reply("❌ Доступ только для воркеров.")
        return

    total = await get_total_leads_count()

    text = (
        "📈 *ЛИДЫ*\n\n"
        f"🧾 Количество мамонтов за все время: *{total}*\n"
        f"🕒 Обновлено (МСК): `{fmt_msk()}`\n\n"
        "➖➖➖➖➖➖➖➖"
    )
    await message.answer(text, parse_mode="Markdown")



async def get_total_leads_count() -> int:
    """Количество лидов (mammoths) за всё время."""
    async with aiosqlite.connect("workers.db") as db:
        cur = await db.execute("SELECT COUNT(*) FROM mammoths")
        row = await cur.fetchone()
        return int(row[0] or 0)


@dp.message(Command("cash"))
async def cmd_cash(message: Message):
    today_usd, today_rub = await get_cash_stats('today')
    week_usd, week_rub = await get_cash_stats('week')
    month_usd, month_rub = await get_cash_stats('month')
    all_usd, all_rub = await get_cash_stats('all')

    cash_message = (
        "💰 *КАССА ПРОЕКТА*\n\n"
        f"▪️ Сегодня: *{today_usd:.2f} $* / *{rub_fmt(today_rub)}*\n"
        f"▪️ Неделя: *{week_usd:.2f} $* / *{rub_fmt(week_rub)}*\n"
        f"▪️ Месяц: *{month_usd:.2f} $* / *{rub_fmt(month_rub)}*\n"
        f"▪️ Всё время: *{all_usd:.2f} $* / *{rub_fmt(all_rub)}*\n\n"
        f"📅 Обновлено (МСК): {fmt_msk()}\n"
        "➖➖➖➖➖➖➖➖"
    )
    await message.answer(cash_message, parse_mode="Markdown")


@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    user_info = await get_user_info(message.from_user.id)
    if not user_info or not user_info[2]:
        await message.answer("❌ Эта команда доступна только администраторам.")
        return

    stats = await get_project_stats()
    stats_message = (
        "📊 *СТАТИСТИКА ПРОЕКТА*\n\n"
        f"▪️ Количество мамонтов: *{stats['mammoths']}*\n"
        f"▪️ Количество воркеров: *{stats['workers']}*\n"
        f"▪️ Ожидающих заявок: *{stats['pending_applications']}*\n"
        f"▪️ Общая сумма платежей: *{stats['total_profits_usd']:.2f} $* / *{rub_fmt(stats['total_profits_rub'])}*\n"
        f"▪️ Выплачено воркерам: *{stats['total_payouts_usd']:.2f} $* / *{rub_fmt(stats['total_payouts_rub'])}*\n"
        f"▪️ Доход проекта: *{stats['project_income_usd']:.2f} $* / *{rub_fmt(stats['project_income_rub'])}*\n\n"
        "➖➖➖➖➖➖➖➖"
    )
    await message.answer(stats_message, parse_mode="Markdown")


@dp.message(Command("help"))
async def cmd_help(message: Message):
    help_message = (
        "🆘 *ПОМОЩЬ ПО КОМАНДАМ*\n\n"
        "*Доступные команды:*\n"
        "`/contacts` - Контакты\n"
        "`/cash` - Касса\n"
        "`/manuals` - Мануалы\n"
        "`/kurs` - Проценты + курс\n"
        "`/profile` - Профиль\n"
        "`/top_day` - Топ за сегодня\n"
        "`/top_month` - Топ за месяц\n"
        "`/leadshow` - Общее кол-во мамонтов\n"
        "➖➖➖➖➖➖➖➖"
    )
    await message.answer(help_message, parse_mode="Markdown")


@dp.message(Command("getid"))
async def cmd_get_id(message: Message):
    chat_id = message.chat.id
    thread_id = message.message_thread_id
    chat_title = message.chat.title if message.chat.title else "Личные сообщения"

    response = (
        f"📊 Информация о чате:\n"
        f"🏷 Название: {chat_title}\n"
        f"🆔 Chat ID: {chat_id}\n"
        f"🧵 Thread ID: {thread_id if thread_id else 'Нет (основной чат)'}\n"
        f"📝 Тип чата: {message.chat.type}"
    )
    await message.answer(response)


@dp.message(Command("касса", ignore_case=True))
async def cmd_cash_in_group(message: Message):
    """Как было: /касса в супергруппе постит в тему кассы"""
    if message.chat.id != SUPERGROUP_ID:
        await message.answer("Эта команда работает только в супергруппе проекта")
        return

    today_usd, today_rub = await get_cash_stats('today')
    week_usd, week_rub = await get_cash_stats('week')
    month_usd, month_rub = await get_cash_stats('month')
    all_usd, all_rub = await get_cash_stats('all')

    cash_message = (
        "💰 *КАССА ПРОЕКТА*\n\n"
        f"▪️ Сегодня: *{today_usd:.2f} $* / *{rub_fmt(today_rub)}*\n"
        f"▪️ Неделя: *{week_usd:.2f} $* / *{rub_fmt(week_rub)}*\n"
        f"▪️ Месяц: *{month_usd:.2f} $* / *{rub_fmt(month_rub)}*\n"
        f"▪️ Всё время: *{all_usd:.2f} $* / *{rub_fmt(all_rub)}*\n\n"
        f"📅 Обновлено (МСК): {fmt_msk()}\n"
        "➖➖➖➖➖➖➖➖"
    )

    try:
        await send_cash_to_channel(cash_message)
        if message.message_thread_id != GROUP_TOPICS["cash"]:
            await message.reply("✅ Касса отправлена в соответствующую тему!")
    except Exception as e:
        logger.error(f"Error sending cash to channel: {e}")
        await message.reply("❌ Ошибка при отправке кассы. Проверьте права бота.")


# ================== APPLICATIONS CALLBACKS ==================
@dp.callback_query(F.data == "submit_application")
async def process_submit_application(callback: CallbackQuery):
    global bot
    user_id = callback.from_user.id
    username = callback.from_user.username or f"user_{user_id}"
    display_username = f"@{username}" if callback.from_user.username else str(user_id)

    existing_application = await get_application(user_id)
    if existing_application:
        await callback.answer("Вы уже подали заявку!")
        return

    success = await create_application(user_id, display_username)
    if success:
        admins = await get_all_admins()
        for admin_id in admins:
            try:
                if bot:
                    await bot.send_message(
                        admin_id,
                        f"📝 Новая заявка на воркера!\n\n"
                        f"👤 Пользователь: {display_username}\n"
                        f"🆔 ID: {user_id}\n"
                        f"📅 Дата (МСК): {fmt_msk()}\n\n"
                        f"Выберите действие:",
                        reply_markup=get_application_keyboard(user_id)
                    )
            except Exception as e:
                logger.error(f"Error notifying admin {admin_id}: {e}")

        await callback.message.answer(
            "✅ Заявка успешно отправлена!\n"
            "Ожидайте решения администратора."
        )
        await callback.answer("Заявка отправлена!")
    else:
        await callback.message.answer("❌ Произошла ошибка при отправке заявки.")
        await callback.answer()


@dp.callback_query(F.data.startswith("accept_application_"))
async def process_accept_application(callback: CallbackQuery):
    global bot
    user_info = await get_user_info(callback.from_user.id)
    if not user_info or not user_info[2]:
        await callback.answer("❌ Доступ запрещен")
        return

    user_id = int(callback.data.split("_")[2])

    async with aiosqlite.connect('workers.db') as db:
        cur = await db.execute(
            'SELECT id, username FROM applications WHERE user_id = ?',
            (user_id,)
        )
        application = await cur.fetchone()

    if not application:
        await callback.answer("Заявка не найдена")
        return

    application_id, username = application
    success, accepted_user_id, accepted_username = await accept_application(application_id)

    if success:
        try:
            if bot:
                await bot.send_message(
                    accepted_user_id,
                    "🎉 Поздравляем! Ваша заявка принята!\n\n"
                    "Теперь вы воркер в системе. Используйте /start для начала работы."
                )
        except Exception as e:
            logger.error(f"Error notifying user {accepted_user_id}: {e}")

        await callback.message.edit_text(
            f"✅ Заявка от {accepted_username} принята!\n"
            f"Пользователь добавлен в воркеры."
        )

        # как было: уведомить остальных админов
        admins = await get_all_admins()
        for admin_id in admins:
            if admin_id != callback.from_user.id:
                try:
                    if bot:
                        await bot.send_message(
                            admin_id,
                            f"✅ Заявка от {accepted_username} была принята администратором @{callback.from_user.username or 'admin'}"
                        )
                except Exception as e:
                    logger.error(f"Error notifying admin {admin_id}: {e}")

        await callback.answer("Заявка принята!")
    else:
        await callback.message.answer("❌ Ошибка при принятии заявки")
        await callback.answer()


@dp.callback_query(F.data.startswith("reject_application_"))
async def process_reject_application(callback: CallbackQuery):
    global bot
    user_info = await get_user_info(callback.from_user.id)
    if not user_info or not user_info[2]:
        await callback.answer("❌ Доступ запрещен")
        return

    user_id = int(callback.data.split("_")[2])

    async with aiosqlite.connect('workers.db') as db:
        cur = await db.execute(
            'SELECT id, username FROM applications WHERE user_id = ?',
            (user_id,)
        )
        application = await cur.fetchone()

    if not application:
        await callback.answer("Заявка не найдена")
        return

    application_id, username = application
    success, rejected_user_id = await reject_application(application_id)

    if success:
        try:
            if bot:
                await bot.send_message(
                    rejected_user_id,
                    "❌ К сожалению, ваша заявка была отклонена администратором.\n\n"
                    "Вы можете попробовать подать заявку снова через /start."
                )
        except Exception as e:
            logger.error(f"Error notifying user {rejected_user_id}: {e}")

        await callback.message.edit_text(f"❌ Заявка от {username} отклонена.")

        # как было: уведомить остальных админов
        admins = await get_all_admins()
        for admin_id in admins:
            if admin_id != callback.from_user.id:
                try:
                    if bot:
                        await bot.send_message(
                            admin_id,
                            f"❌ Заявка от {username} была отклонена администратором @{callback.from_user.username or 'admin'}"
                        )
                except Exception as e:
                    logger.error(f"Error notifying admin {admin_id}: {e}")

        await callback.answer("Заявка отклонена!")
    else:
        await callback.message.answer("❌ Ошибка при отклонении заявки")
        await callback.answer()


# ================== ОСНОВНЫЕ CALLBACK HANDLERS ==================
@dp.callback_query(F.data == "add_mammoth")
async def process_add_mammoth(callback: CallbackQuery, state: FSMContext):
    user_info = await get_user_info(callback.from_user.id)
    if not user_info:
        await callback.answer("❌ Вы не воркер!")
        return

    await callback.message.answer("Введите @username или ID мамонта:")
    await state.set_state(AddMammothStates.waiting_for_mammoth)
    await callback.answer()


@dp.message(AddMammothStates.waiting_for_mammoth)
async def process_mammoth_input(message: Message, state: FSMContext):
    mammoth_identifier = message.text.strip()
    user_info = await get_user_info(message.from_user.id)

    if not user_info:
        await message.answer("Ошибка: пользователь не найден")
        await state.clear()
        return

    worker_id = user_info[0]
    success, _existing_worker = await add_mammoth(mammoth_identifier, worker_id)

    if success:
        await message.answer(f"✅ Мамонт {normalize_lead_identifier(mammoth_identifier)} успешно добавлен!")
    else:
        await message.answer("❌ Мамонт уже привязан к другому воркеру")

    await state.clear()


@dp.callback_query(F.data == "check_mammoth")
async def process_check_mammoth(callback: CallbackQuery, state: FSMContext):
    user_info = await get_user_info(callback.from_user.id)
    if not user_info:
        await callback.answer("❌ Вы не воркер!")
        return

    await callback.message.answer("Введите @username или ID мамонта для проверки:")
    await state.set_state(CheckMammothStates.waiting_for_mammoth)
    await callback.answer()


@dp.message(CheckMammothStates.waiting_for_mammoth)
async def process_check_mammoth_input(message: Message, state: FSMContext):
    mammoth_identifier = message.text.strip()
    mammoth_info = await check_mammoth(mammoth_identifier)

    if mammoth_info:
        mammoth_user_id, mammoth_username, worker_username = mammoth_info
        mammoth_display = mammoth_username or mammoth_user_id

        if worker_username:
            await message.answer(f"✅ Мамонт {mammoth_display} привязан к: {worker_username}")
        else:
            await message.answer(f"ℹ️ Мамонт {mammoth_display} найден, но не привязан к воркеру")
    else:
        await message.answer(f"❌ Мамонт {normalize_lead_identifier(mammoth_identifier)} не найден в базе данных")

    await state.clear()


@dp.callback_query(F.data == "cashbox")
async def process_cashbox(callback: CallbackQuery):
    today_usd, today_rub = await get_cash_stats('today')
    week_usd, week_rub = await get_cash_stats('week')
    month_usd, month_rub = await get_cash_stats('month')
    all_usd, all_rub = await get_cash_stats('all')

    cash_message = (
        "💰 *КАССА ПРОЕКТА*\n\n"
        f"▪️ Сегодня: *{today_usd:.2f} $* / *{rub_fmt(today_rub)}*\n"
        f"▪️ Неделя: *{week_usd:.2f} $* / *{rub_fmt(week_rub)}*\n"
        f"▪️ Месяц: *{month_usd:.2f} $* / *{rub_fmt(month_rub)}*\n"
        f"▪️ Всё время: *{all_usd:.2f} $* / *{rub_fmt(all_rub)}*\n\n"
        f"📅 Обновлено (МСК): {fmt_msk()}\n"
        "➖➖➖➖➖➖➖➖"
    )
    await callback.message.answer(cash_message, parse_mode="Markdown")
    await callback.answer()


@dp.callback_query(F.data == "contacts")
async def process_contacts(callback: CallbackQuery):
    contacts = await get_contacts()
    contacts_message = get_contacts_message(contacts)
    await callback.message.answer(contacts_message, parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data == "manuals")
async def cb_manuals(callback: CallbackQuery):
    await cmd_manuals(callback.message)
    await callback.answer()


@dp.callback_query(F.data == "kurs")
async def cb_kurs(callback: CallbackQuery):
    await cmd_kurs(callback.message)
    await callback.answer()


@dp.callback_query(F.data == "profile")
async def cb_profile(callback: CallbackQuery):
    await cmd_profile(callback.message)
    await callback.answer()


@dp.callback_query(F.data == "top_day")
async def cb_top_day(callback: CallbackQuery):
    rows = await get_top_workers("day", 10)
    await callback.message.answer(format_top_message(rows, "ТОП ВОРКЕРОВ ЗА СЕГОДНЯ (МСК)"), parse_mode="Markdown")
    await callback.answer()


@dp.callback_query(F.data == "top_month")
async def cb_top_month(callback: CallbackQuery):
    rows = await get_top_workers("month", 10)
    await callback.message.answer(format_top_message(rows, "ТОП ВОРКЕРОВ ЗА МЕСЯЦ"), parse_mode="Markdown")
    await callback.answer()


# ================== АДМИНСКИЕ CALLBACK HANDLERS ==================
@dp.callback_query(F.data == "add_profit")
async def process_add_profit(callback: CallbackQuery, state: FSMContext):
    user_info = await get_user_info(callback.from_user.id)
    if not user_info or not user_info[2]:
        await callback.answer("❌ Доступ запрещен")
        return

    workers = await get_all_workers()
    if not workers:
        await callback.message.answer("❌ Нет доступных воркеров", reply_markup=get_workers_keyboard())
        return

    workers_list = "\n".join([f"{w[0]} (ID: {w[1]})" for w in workers])
    await callback.message.answer(
        f"📋 Список воркеров:\n{workers_list}\n\n"
        f"Введите @username или ID воркера:"
    )
    await state.set_state(AddProfitStates.waiting_for_worker)
    await callback.answer()


@dp.message(AddProfitStates.waiting_for_worker)
async def process_profit_worker(message: Message, state: FSMContext):
    worker_username = message.text.strip()
    await state.update_data(worker_username=worker_username)
    await message.answer("Введите сумму платежа в $ (например: 10):")
    await state.set_state(AddProfitStates.waiting_for_amount)


@dp.message(AddProfitStates.waiting_for_amount)
async def process_profit_amount(message: Message, state: FSMContext):
    try:
        amount = float(message.text.strip())
        if amount <= 0:
            await message.answer("❌ Сумма должна быть положительной. Введите сумму платежа в $:")
            return

        await state.update_data(amount=amount)
        await message.answer("Введите процент воркера (например: 70):")
        await state.set_state(AddProfitStates.waiting_for_percent)
    except ValueError:
        await message.answer("❌ Пожалуйста, введите корректную сумму (число)")


@dp.message(AddProfitStates.waiting_for_percent)
async def process_profit_percent(message: Message, state: FSMContext):
    try:
        percent = int(message.text.strip())
        if percent < 0 or percent > 100:
            await message.answer("❌ Процент должен быть от 0 до 100. Введите процент воркера:")
            return

        data = await state.get_data()
        worker_username = data['worker_username']
        amount = data['amount']

        success, result = await add_profit(worker_username, amount, percent)

        if success:
            rate = result["rate"]
            worker_amount = result["worker_amount_usd"]
            project_amount = result["project_amount_usd"]

            profit_message = (
                "💳 *НОВЫЙ ПЛАТЕЖ!*\n\n"
                f"▪️ Воркер: {worker_username}\n"
                f"▪️ Сумма: *{usd_rub_pair(amount, rate)}*\n"
                f"▪️ Процент воркера: *{percent}%*\n"
                f"▪️ Выплата воркеру: *{usd_rub_pair(worker_amount, rate)}*\n"
                f"▪️ Доход проекта: *{usd_rub_pair(project_amount, rate)}*\n\n"
                f"📅 Дата (МСК): {fmt_msk()}\n"
                "➖➖➖➖➖➖➖➖"
            )

            try:
                await add_profit_to_channel(profit_message)
                await message.answer("✅ Платеж успешно добавлен и опубликован в супергруппе!")
            except Exception as e:
                logger.error(f"Error sending to channel: {e}")
                await message.answer("✅ Платеж добавлен, но произошла ошибка при отправке в супергруппу.")
        else:
            error_msg = result if isinstance(result, str) else "Неизвестная ошибка"
            await message.answer(f"❌ Ошибка при добавлении платежа: {error_msg}")

        await state.clear()
    except ValueError:
        await message.answer("❌ Пожалуйста, введите корректный процент (целое число)")


@dp.callback_query(F.data == "edit_contact")
async def process_edit_contact(callback: CallbackQuery):
    user_info = await get_user_info(callback.from_user.id)
    if not user_info or not user_info[2]:
        await callback.answer("❌ Доступ запрещен")
        return

    await callback.message.answer(
        "Выберите контакт для изменения:",
        reply_markup=get_contacts_keyboard()
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("edit_"))
async def process_contact_select(callback: CallbackQuery, state: FSMContext):
    user_info = await get_user_info(callback.from_user.id)
    if not user_info or not user_info[2]:
        await callback.answer("❌ Доступ запрещен")
        return

    role_map = {
        "edit_owner": "owner",
        "edit_buyer": "buyer",
        "edit_curator": "curator",
        "edit_shop": "shop"
    }

    role = role_map.get(callback.data)
    if role:
        await state.update_data(role=role)
        await callback.message.answer(f"Введите новый @username:")
        await state.set_state(EditContactStates.waiting_for_username)

    await callback.answer()


@dp.message(EditContactStates.waiting_for_username)
async def process_new_username(message: Message, state: FSMContext):
    data = await state.get_data()
    role = data['role']
    new_username = message.text.strip()

    if not new_username.startswith('@'):
        new_username = f"@{new_username}"

    await update_contact(role, new_username)

    contacts = await get_contacts()
    contacts_message = get_contacts_message(contacts)

    await message.answer(contacts_message, parse_mode="HTML")
    await state.clear()


@dp.callback_query(F.data == "project_stats")
async def process_project_stats(callback: CallbackQuery):
    user_info = await get_user_info(callback.from_user.id)
    if not user_info or not user_info[2]:
        await callback.answer("❌ Доступ запрещен")
        return

    stats = await get_project_stats()
    stats_message = (
        "📊 *СТАТИСТИКА ПРОЕКТА*\n\n"
        f"▪️ Количество мамонтов: *{stats['mammoths']}*\n"
        f"▪️ Количество воркеров: *{stats['workers']}*\n"
        f"▪️ Ожидающих заявок: *{stats['pending_applications']}*\n"
        f"▪️ Общая сумма платежей: *{stats['total_profits_usd']:.2f} $* / *{rub_fmt(stats['total_profits_rub'])}*\n"
        f"▪️ Выплачено воркерам: *{stats['total_payouts_usd']:.2f} $* / *{rub_fmt(stats['total_payouts_rub'])}*\n"
        f"▪️ Доход проекта: *{stats['project_income_usd']:.2f} $* / *{rub_fmt(stats['project_income_rub'])}*\n\n"
        "➖➖➖➖➖➖➖➖"
    )
    await callback.message.answer(stats_message, parse_mode="Markdown")
    await callback.answer()


@dp.callback_query(F.data == "view_applications")
async def process_view_applications(callback: CallbackQuery):
    user_info = await get_user_info(callback.from_user.id)
    if not user_info or not user_info[2]:
        await callback.answer("❌ Доступ запрещен")
        return

    applications = await get_pending_applications()

    if not applications:
        await callback.message.answer("📭 Нет ожидающих заявок.")
        await callback.answer()
        return

    for app in applications:
        app_id, user_id, username, app_date = app
        await callback.message.answer(
            f"📝 Заявка #{app_id}\n\n"
            f"👤 Пользователь: {username}\n"
            f"🆔 ID: {user_id}\n"
            f"📅 Дата подачи: {app_date}\n\n"
            f"Выберите действие:",
            reply_markup=get_application_keyboard(user_id)
        )

    await callback.answer()


@dp.callback_query(F.data == "back_to_main")
async def process_back_to_main(callback: CallbackQuery):
    user_info = await get_user_info(callback.from_user.id)
    is_admin = bool(user_info[2]) if user_info else False
    await callback.message.answer(
        "Главное меню:",
        reply_markup=get_main_keyboard(is_admin)
    )
    await callback.answer()


# ================== TEST COMMAND (как было) ==================
#@dp.message(Command("test"))
#async def cmd_test(message: Message):
  #  user_info = await get_user_info(message.from_user.id)
    #if not user_info or not user_info[2]:
    #    return

   # try:
     #   await send_to_topic(GROUP_TOPICS["profits"], "✅ Тестовое сообщение в тему платежей", "Markdown")
     #   await send_to_topic(GROUP_TOPICS["cash"], "✅ Тестовое сообщение в тему кассы", "Markdown")
      #  await message.answer("✅ Тестовые сообщения отправлены в супергруппу!")
   # except Exception as e:
    #    await message.answer(f"❌ Ошибка при отправке тестовых сообщений: {e}")


# ================== MAIN ==================
async def main():
    global bot
    if not BOT_TOKEN:
        print("❌ BOT_TOKEN пустой. Установи переменную окружения BOT_TOKEN и перезапусти.")
        return

    bot = Bot(token=BOT_TOKEN)

    await init_db()
    await setup_bot_commands()

    print("=" * 60)
    print("🤖 Бот запущен!")
    print(f"🔗 Супергруппа ID: {SUPERGROUP_ID}")
    print(f"💳 Тема платежей (profits): {GROUP_TOPICS['profits']}")
    print(f"💰 Тема кассы: {GROUP_TOPICS['cash']}")
    print(f"👋 Тема приветствий: {GROUP_TOPICS.get('welcome', 'основной чат')}")
    print(f"📌 /leadshow только в thread: {LEADSHOW_THREAD_ID}")
    print("🕒 Время: Московское (UTC+3)")
    print("=" * 60)

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
