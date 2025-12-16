import asyncio
import logging
import sys
from datetime import datetime, timedelta
import aiosqlite
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandStart
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.exceptions import TelegramForbiddenError

# --- КОНФИГУРАЦИЯ ---
TOKEN = "8475595381:AAEL6bTVFDXTx2qQ7KhRSREuhVKmgjqK5Fw"
ADMIN_ID = 8354527541  # Твой цифровой ID
TEAM_NAME = "HELLCASH"

DEFAULT_CONTACTS = {
    "owner": "@username",
    "buyer": "@username",
    "support": "@username",
    "curator": "@username"
}

# --- НАСТРОЙКА ЛОГОВ ---
logging.basicConfig(level=logging.INFO)

# --- СОСТОЯНИЯ (FSM) ---
class Form(StatesGroup):
    waiting_for_mamont = State()

# --- БАЗА ДАННЫХ ---
DB_NAME = 'team.db'

async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('''CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            join_date TEXT,
            is_approved INTEGER DEFAULT 0
        )''')
        await db.execute('''CREATE TABLE IF NOT EXISTS mamonts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mamont_data TEXT UNIQUE,
            worker_id INTEGER,
            date_added TEXT
        )''')
        await db.execute('''CREATE TABLE IF NOT EXISTS profits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            amount REAL,
            date_added TIMESTAMP
        )''')
        await db.execute('''CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT
        )''')
        for key, val in DEFAULT_CONTACTS.items():
            await db.execute("INSERT OR IGNORE INTO config (key, value) VALUES (?, ?)", (key, val))
        await db.commit()

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
def get_main_keyboard():
    kb = [
        [KeyboardButton(text="👤 Профиль"), KeyboardButton(text="🦣 Добавить мамонта")],
        [KeyboardButton(text="ℹ️ Инфо")]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

async def check_access(user_id):
    if user_id == ADMIN_ID: return True
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT is_approved FROM users WHERE user_id = ?", (user_id,))
        row = await cursor.fetchone()
        return row and row[0] == 1

async def broadcast_to_team(bot: Bot, text: str):
    """Функция для рассылки сообщений всем одобренным воркерам"""
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT user_id FROM users WHERE is_approved = 1")
        users = await cursor.fetchall()
    
    count = 0
    for user in users:
        try:
            await bot.send_message(user[0], text, parse_mode="HTML")
            count += 1
        except TelegramForbiddenError:
            # Юзер заблокировал бота
            pass
        except Exception as e:
            logging.error(f"Ошибка рассылки юзеру {user[0]}: {e}")
    return count

# --- ХЕНДЛЕРЫ ---
dp = Dispatcher()
bot = Bot(TOKEN)

@dp.message(CommandStart())
async def command_start(message: types.Message):
    user_id = message.from_user.id
    username = message.from_user.username or "Unknown"
    join_date = datetime.now().strftime("%Y-%m-%d")

    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT OR IGNORE INTO users (user_id, username, join_date) VALUES (?, ?, ?)", 
                         (user_id, username, join_date))
        await db.commit()

    if await check_access(user_id):
        await message.answer(f"🔥 Добро пожаловать в команду *{TEAM_NAME}*!\nРаботаем.", 
                             parse_mode="Markdown", reply_markup=get_main_keyboard())
    else:
        await message.answer(f"🔒 Привет! Твоя заявка в *{TEAM_NAME}* принята.\nОжидай подтверждения от администратора.", 
                             parse_mode="Markdown")
        # Уведомление админу
        await bot.send_message(ADMIN_ID, f"⚠️ Новый пользователь: @{username} (ID: `{user_id}`)\nДобавить: `/adduser {user_id}`")

# --- АДМИН ПАНЕЛЬ ---

@dp.message(Command("adduser"))
async def add_user_cmd(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
    try:
        target_id = int(message.text.split()[1])
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("UPDATE users SET is_approved = 1 WHERE user_id = ?", (target_id,))
            await db.commit()
        await message.answer(f"✅ Пользователь {target_id} принят в команду.")
        await bot.send_message(target_id, f"✅ *Доступ открыт!*\nДобро пожаловать в {TEAM_NAME}.", 
                               parse_mode="Markdown", reply_markup=get_main_keyboard())
    except IndexError:
        await message.answer("❌ Введи ID. Пример: `/adduser 12345`")

@dp.message(Command("banuser"))
async def ban_user_cmd(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
    try:
        target_id = int(message.text.split()[1])
        async with aiosqlite.connect(DB_NAME) as db:
            # Ставим is_approved = 0
            await db.execute("UPDATE users SET is_approved = 0 WHERE user_id = ?", (target_id,))
            await db.commit()
        await message.answer(f"⛔ Пользователь {target_id} заблокирован (кикнут).")
    except IndexError:
        await message.answer("❌ Введи ID. Пример: `/banuser 12345`")

@dp.message(Command("addprofit"))
async def add_profit_cmd(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
    try:
        amount = float(message.text.split()[1])
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("INSERT INTO profits (amount, date_added) VALUES (?, ?)", (amount, datetime.now()))
            await db.commit()
        
        await message.answer(f"💰 В казну добавлено: **${amount}**", parse_mode="Markdown")
        
        # Рассылка всем
        notification_text = (
            f"💸 <b>НОВЫЙ ПРОФИТ В {TEAM_NAME}!</b>\n\n"
            f"Сумма: <code>${amount}</code>\n"
            f"Работаем дальше! 🔥"
        )
        count = await broadcast_to_team(bot, notification_text)
        await message.answer(f"📢 Оповещение отправлено {count} воркерам.")
        
    except IndexError:
        await message.answer("❌ Пример: `/addprofit 1000`")
    except ValueError:
        await message.answer("❌ Сумма должна быть числом.")

@dp.message(Command("broadcast"))
async def broadcast_cmd(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
    
    # Берем текст после команды /broadcast
    text_parts = message.text.split(maxsplit=1)
    if len(text_parts) < 2:
        await message.answer("❌ Введите текст рассылки.\nПример: `/broadcast Скуп онлайн!`", parse_mode="Markdown")
        return
    
    text_to_send = f"📣 <b>ОБЪЯВЛЕНИЕ {TEAM_NAME}</b>\n\n" + text_parts[1]
    count = await broadcast_to_team(bot, text_to_send)
    await message.answer(f"✅ Рассылка завершена. Получили: {count} чел.")

@dp.message(Command("setcontact"))
async def set_contact_cmd(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
    try:
        _, role, link = message.text.split()
        if role not in ['owner', 'buyer', 'support', 'curator']:
            await message.answer("❌ Роли: owner, buyer, support, curator")
            return
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("UPDATE config SET value = ? WHERE key = ?", (link, role))
            await db.commit()
        await message.answer(f"✅ Контакт {role} обновлен.")
    except:
        await message.answer("❌ Пример: `/setcontact buyer @newbuy`")

@dp.message(Command("delmamont"))
async def delete_mamont_cmd(message: types.Message):
    """
    Удаляет мамонта.
    Админ может удалить любого.
    Воркер может удалить только своего.
    """
    if not await check_access(message.from_user.id): return
    
    try:
        target = message.text.split()[1].strip()
    except IndexError:
        await message.answer("❌ Укажи юзернейм или ID мамонта.\nПример: `/delmamont @mamont`", parse_mode="Markdown")
        return

    user_id = message.from_user.id
    
    async with aiosqlite.connect(DB_NAME) as db:
        # Сначала проверим, чей это мамонт
        cursor = await db.execute("SELECT worker_id FROM mamonts WHERE mamont_data = ?", (target,))
        row = await cursor.fetchone()
        
        if not row:
            await message.answer("❌ Такой мамонт не найден в базе.")
            return
        
        mamont_owner_id = row[0]
        
        # Если юзер админ ИЛИ это мамонт юзера
        if user_id == ADMIN_ID or user_id == mamont_owner_id:
            await db.execute("DELETE FROM mamonts WHERE mamont_data = ?", (target,))
            await db.commit()
            await message.answer(f"🗑 Мамонт <b>{target}</b> удален.", parse_mode="HTML")
        else:
            await message.answer("❌ Ты не можешь удалить чужого мамонта.")

# --- ПОЛЬЗОВАТЕЛЬСКИЕ ФУНКЦИИ ---

@dp.message(F.text == "👤 Профиль")
async def profile_handler(message: types.Message):
    if not await check_access(message.from_user.id): return
    
    user_id = message.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT join_date FROM users WHERE user_id = ?", (user_id,)) as cursor:
            res = await cursor.fetchone()
            join_date = res[0] if res else "N/A"
        async with db.execute("SELECT COUNT(*) FROM mamonts WHERE worker_id = ?", (user_id,)) as cursor:
            mamont_count = (await cursor.fetchone())[0]
        
        # КАЗНА
        now = datetime.now()
        day_start = now - timedelta(days=1)
        week_start = now - timedelta(weeks=1)
        month_start = now - timedelta(days=30)
        
        async with db.execute("SELECT amount, date_added FROM profits") as cursor:
            rows = await cursor.fetchall()
        
        total = sum(r[0] for r in rows)
        day = sum(r[0] for r in rows if datetime.fromisoformat(str(r[1])) > day_start)
        week = sum(r[0] for r in rows if datetime.fromisoformat(str(r[1])) > week_start)
        month = sum(r[0] for r in rows if datetime.fromisoformat(str(r[1])) > month_start)

    text = (
        f"👹 <b>Твой профиль в {TEAM_NAME}</b>\n"
        f"➖➖➖➖➖➖➖➖➖➖\n"
        f"🆔 <b>ID:</b> <code>{user_id}</code>\n"
        f"📅 <b>В команде с:</b> {join_date}\n"
        f"🦣 <b>Твои мамонты:</b> {mamont_count} шт.\n"
        f"➖➖➖➖➖➖➖➖➖➖\n"
        f"🏦 <b>КАЗНА КОМАНДЫ:</b>\n"
        f"🔹 За день: <code>${day:.2f}</code>\n"
        f"🔹 За неделю: <code>${week:.2f}</code>\n"
        f"🔹 За месяц: <code>${month:.2f}</code>\n"
        f"💀 <b>ВСЕГО:</b> <code>${total:.2f}</code>"
    )
    await message.answer(text, parse_mode="HTML")

@dp.message(F.text == "ℹ️ Инфо")
async def info_handler(message: types.Message):
    if not await check_access(message.from_user.id): return
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT key, value FROM config")
        contacts = {row[0]: row[1] for row in await cursor.fetchall()}
    
    text = (
        f"ℹ️ <b>Информация {TEAM_NAME}</b>\n\n"
        f"👑 <b>Владелец:</b> {contacts.get('owner', 'N/A')}\n"
        f"💵 <b>Скуп:</b> {contacts.get('buyer', 'N/A')}\n"
        f"👨‍💻 <b>ТП:</b> {contacts.get('support', 'N/A')}\n"
        f"🦅 <b>Куратор:</b> {contacts.get('curator', 'N/A')}"
    )
    await message.answer(text, parse_mode="HTML")

@dp.message(F.text == "🦣 Добавить мамонта")
async def add_mamont_start(message: types.Message, state: FSMContext):
    if not await check_access(message.from_user.id): return
    await message.answer("✍️ <b>Введите Username или ID мамонта:</b>", parse_mode="HTML")
    await state.set_state(Form.waiting_for_mamont)

@dp.message(Form.waiting_for_mamont)
async def process_mamont_add(message: types.Message, state: FSMContext):
    mamont_input = message.text.strip()
    user_id = message.from_user.id
    
    async with aiosqlite.connect(DB_NAME) as db:
        try:
            await db.execute("INSERT INTO mamonts (mamont_data, worker_id, date_added) VALUES (?, ?, ?)", 
                             (mamont_input, user_id, datetime.now().strftime("%Y-%m-%d %H:%M")))
            await db.commit()
            await message.answer(f"✅ Мамонт <b>{mamont_input}</b> привязан!", parse_mode="HTML")
        except aiosqlite.IntegrityError:
            await message.answer("❌ Этот мамонт уже занят!", parse_mode="Markdown")
            
    await state.clear()

@dp.message(Command("mymamont"))
async def my_mamonts_list(message: types.Message):
    if not await check_access(message.from_user.id): return
    user_id = message.from_user.id
    
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT mamont_data, date_added FROM mamonts WHERE worker_id = ?", (user_id,))
        rows = await cursor.fetchall()
    
    if not rows:
        await message.answer("У тебя нет привязанных мамонтов.")
        return
        
    text = "📋 <b>Твой список жертв:</b>\n\n"
    for row in rows:
        # Кнопка для быстрого удаления (визуально просто текст с командой)
        text += f"🔹 {row[0]} (от {row[1]}) \n↪️ Удалить: <code>/delmamont {row[0]}</code>\n\n"
        
    await message.answer(text, parse_mode="HTML")

async def main():
    await init_db()
    print("Бот HELLCASH запущен...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Exit")