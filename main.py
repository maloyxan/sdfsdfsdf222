import asyncio
import aiosqlite
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация
BOT_TOKEN = "8482913583:AAG66RQJo8cOOJE98aJ6Iwhmyiru4J0ysVk"  # Замените на токен вашего бота

# Настройки супергруппы
SUPERGROUP_ID = -1003650560814  # ID вашей супергруппы
GROUP_TOPICS = {
    "profits": 8,  # Тема для профитов
    "cash": 7,     # Тема для кассы
}

# ID администраторов (добавьте свои user_id)
ADMIN_IDS = [6731763080]  # Замените на ваш user_id

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Классы состояний
class AddMammothStates(StatesGroup):
    waiting_for_mammoth = State()

class CheckMammothStates(StatesGroup):
    waiting_for_mammoth = State()

class AddProfitStates(StatesGroup):
    waiting_for_worker = State()
    waiting_for_amount = State()
    waiting_for_percent = State()

class EditContactStates(StatesGroup):
    waiting_for_role = State()
    waiting_for_username = State()

# ========== ФУНКЦИИ ДЛЯ РАБОТЫ С БАЗОЙ ДАННЫХ ==========

async def init_db():
    """Инициализация базы данных"""
    async with aiosqlite.connect('workers.db') as db:
        # Таблица воркеров
        await db.execute('''
            CREATE TABLE IF NOT EXISTS workers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER UNIQUE,
                username TEXT,
                is_admin BOOLEAN DEFAULT 0,
                registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Таблица заявок
        await db.execute('''
            CREATE TABLE IF NOT EXISTS applications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER UNIQUE,
                username TEXT,
                application_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'pending'
            )
        ''')
        
        # Таблица мамонтов
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
        
        # Таблица профитов
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
        
        # Таблица контактов
        await db.execute('''
            CREATE TABLE IF NOT EXISTS contacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                role TEXT UNIQUE,
                username TEXT
            )
        ''')
        
        # Инициализация контактов
        await db.execute('''
            INSERT OR IGNORE INTO contacts (role, username) VALUES 
            ('owner', '@owner'),
            ('buyer', '@buyer'),
            ('curator', '@curator'),
            ('shop', '@shop')
        ''')
        
        # Добавляем админов
        for admin_id in ADMIN_IDS:
            await db.execute(
                'INSERT OR IGNORE INTO workers (user_id, username, is_admin) VALUES (?, ?, ?)',
                (admin_id, f'@admin_{admin_id}', 1)
            )
        
        await db.commit()

async def get_user_info(user_id: int):
    """Получение информации о пользователе"""
    async with aiosqlite.connect('workers.db') as db:
        cursor = await db.execute(
            'SELECT id, username, is_admin FROM workers WHERE user_id = ?',
            (user_id,)
        )
        return await cursor.fetchone()

async def get_application(user_id: int):
    """Получение заявки пользователя"""
    async with aiosqlite.connect('workers.db') as db:
        cursor = await db.execute(
            'SELECT id, status FROM applications WHERE user_id = ?',
            (user_id,)
        )
        return await cursor.fetchone()

async def add_worker(user_id: int, username: str, is_admin: bool = False):
    """Добавление воркера"""
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
    """Создание заявки"""
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
    """Принятие заявки"""
    async with aiosqlite.connect('workers.db') as db:
        try:
            cursor = await db.execute(
                'SELECT user_id, username FROM applications WHERE id = ?',
                (application_id,)
            )
            application = await cursor.fetchone()
            
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
    """Отклонение заявки"""
    async with aiosqlite.connect('workers.db') as db:
        try:
            cursor = await db.execute(
                'SELECT user_id FROM applications WHERE id = ?',
                (application_id,)
            )
            application = await cursor.fetchone()
            
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
    """Получение ожидающих заявок"""
    async with aiosqlite.connect('workers.db') as db:
        cursor = await db.execute(
            'SELECT id, user_id, username, application_date FROM applications ORDER BY application_date'
        )
        return await cursor.fetchall()

async def add_mammoth(mammoth_identifier: str, worker_id: int):
    """Добавление мамонта"""
    async with aiosqlite.connect('workers.db') as db:
        try:
            cursor = await db.execute(
                'SELECT worker_id FROM mammoths WHERE user_id = ? OR username = ?',
                (mammoth_identifier, mammoth_identifier)
            )
            existing = await cursor.fetchone()
            
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
            logger.error(f"Error adding mammoth: {e}")
            return False, None

async def check_mammoth(mammoth_identifier: str):
    """Проверка мамонта"""
    async with aiosqlite.connect('workers.db') as db:
        cursor = await db.execute('''
            SELECT m.user_id, m.username, w.username 
            FROM mammoths m
            LEFT JOIN workers w ON m.worker_id = w.id
            WHERE m.user_id = ? OR m.username = ?
        ''', (mammoth_identifier, mammoth_identifier))
        return await cursor.fetchone()

async def get_contacts():
    """Получение контактов"""
    async with aiosqlite.connect('workers.db') as db:
        cursor = await db.execute('SELECT role, username FROM contacts')
        rows = await cursor.fetchall()
        return {row[0]: row[1] for row in rows}

async def update_contact(role: str, username: str):
    """Обновление контакта"""
    async with aiosqlite.connect('workers.db') as db:
        await db.execute(
            'UPDATE contacts SET username = ? WHERE role = ?',
            (username, role)
        )
        await db.commit()

async def add_profit(worker_username: str, amount: float, percent: int):
    """Добавление профита"""
    async with aiosqlite.connect('workers.db') as db:
        try:
            if worker_username.startswith('@'):
                worker_username = worker_username[1:]
            
            cursor = await db.execute(
                'SELECT id FROM workers WHERE username LIKE ?',
                (f'%{worker_username}%',)
            )
            worker = await cursor.fetchone()
            
            if not worker and worker_username.isdigit():
                cursor = await db.execute(
                    'SELECT id FROM workers WHERE user_id = ?',
                    (int(worker_username),)
                )
                worker = await cursor.fetchone()
            
            if not worker:
                return False, "Воркер не найден"
            
            worker_id = worker[0]
            worker_amount = amount * (percent / 100)
            
            await db.execute(
                'INSERT INTO profits (worker_id, amount, percent, worker_amount) VALUES (?, ?, ?, ?)',
                (worker_id, amount, percent, worker_amount)
            )
            await db.commit()
            return True, worker_amount
        except Exception as e:
            logger.error(f"Error adding profit: {e}")
            return False, str(e)

async def get_cash_stats(period: str = None):
    """Получение статистики кассы"""
    async with aiosqlite.connect('workers.db') as db:
        now = datetime.now()
        
        if period == 'today':
            date_filter = now.strftime('%Y-%m-%d')
            query = "SELECT SUM(amount) FROM profits WHERE DATE(date) = ?"
            params = (date_filter,)
        elif period == 'week':
            week_ago = (now - timedelta(days=7)).strftime('%Y-%m-%d')
            query = "SELECT SUM(amount) FROM profits WHERE DATE(date) >= ?"
            params = (week_ago,)
        elif period == 'month':
            month_ago = (now - timedelta(days=30)).strftime('%Y-%m-%d')
            query = "SELECT SUM(amount) FROM profits WHERE DATE(date) >= ?"
            params = (month_ago,)
        else:
            query = "SELECT SUM(amount) FROM profits"
            params = ()
        
        cursor = await db.execute(query, params)
        result = await cursor.fetchone()
        return result[0] or 0

async def get_project_stats():
    """Получение статистики проекта"""
    async with aiosqlite.connect('workers.db') as db:
        cursor = await db.execute('SELECT COUNT(*) FROM mammoths')
        mammoth_count = (await cursor.fetchone())[0]
        
        cursor = await db.execute('SELECT COUNT(*) FROM workers WHERE is_admin = 0')
        worker_count = (await cursor.fetchone())[0]
        
        cursor = await db.execute('SELECT COUNT(*) FROM applications')
        pending_applications = (await cursor.fetchone())[0]
        
        cursor = await db.execute('SELECT SUM(amount) FROM profits')
        total_profits = (await cursor.fetchone())[0] or 0
        
        cursor = await db.execute('SELECT SUM(worker_amount) FROM profits')
        total_payouts = (await cursor.fetchone())[0] or 0
        
        return {
            'mammoths': mammoth_count,
            'workers': worker_count,
            'pending_applications': pending_applications,
            'total_profits': total_profits,
            'total_payouts': total_payouts,
            'project_income': total_profits - total_payouts
        }

async def get_all_workers():
    """Получение всех воркеров"""
    async with aiosqlite.connect('workers.db') as db:
        cursor = await db.execute('SELECT username, user_id FROM workers WHERE is_admin = 0')
        rows = await cursor.fetchall()
        return rows

async def get_all_admins():
    """Получение всех админов"""
    async with aiosqlite.connect('workers.db') as db:
        cursor = await db.execute('SELECT user_id FROM workers WHERE is_admin = 1')
        rows = await cursor.fetchall()
        return [row[0] for row in rows]

# ========== ФУНКЦИИ ДЛЯ РАБОТЫ С СУПЕРГРУППОЙ ==========

async def send_to_topic(thread_id: int, text: str, parse_mode: str = None):
    """Отправка сообщения в тему супергруппы"""
    try:
        await bot.send_message(
            chat_id=SUPERGROUP_ID,
            message_thread_id=thread_id,
            text=text,
            parse_mode=parse_mode
        )
        return True
    except Exception as e:
        logger.error(f"Error sending to topic {thread_id}: {e}")
        return False

async def add_profit_to_channel(profit_message: str):
    """Отправка профита в супергруппу"""
    return await send_to_topic(GROUP_TOPICS["profits"], profit_message, "Markdown")

async def send_cash_to_channel(cash_message: str):
    """Отправка кассы в супергруппу"""
    return await send_to_topic(GROUP_TOPICS["cash"], cash_message, "Markdown")

# ========== КЛАВИАТУРЫ ==========

def get_main_keyboard(is_admin: bool = False):
    """Основная клавиатура"""
    keyboard = [
        [InlineKeyboardButton(text="➕ Добавить мамонта", callback_data="add_mammoth")],
        [InlineKeyboardButton(text="🔍 Проверить мамонта", callback_data="check_mammoth")],
        [InlineKeyboardButton(text="💰 Касса", callback_data="cashbox")],
        [InlineKeyboardButton(text="📞 Контакты", callback_data="contacts")]
    ]
    
    if is_admin:
        keyboard.extend([
            [InlineKeyboardButton(text="💼 Добавить профит", callback_data="add_profit")],
            [InlineKeyboardButton(text="✏️ Изменить контакт", callback_data="edit_contact")],
            [InlineKeyboardButton(text="📊 Статистика проекта", callback_data="project_stats")],
            [InlineKeyboardButton(text="📝 Заявки", callback_data="view_applications")]
        ])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_application_keyboard(user_id: int):
    """Клавиатура для заявки"""
    keyboard = [
        [
            InlineKeyboardButton(text="✅ Принять", callback_data=f"accept_application_{user_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_application_{user_id}")
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_contacts_keyboard():
    """Клавиатура для контактов"""
    keyboard = [
        [InlineKeyboardButton(text="👑 Владелец", callback_data="edit_owner")],
        [InlineKeyboardButton(text="💰 Скуп", callback_data="edit_buyer")],
        [InlineKeyboardButton(text="👨‍💼 Куратор", callback_data="edit_curator")],
        [InlineKeyboardButton(text="🛒 Шоп", callback_data="edit_shop")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_application_decision_keyboard():
    """Клавиатура для решения по заявке"""
    keyboard = [
        [InlineKeyboardButton(text="📝 Подать заявку", callback_data="submit_application")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_workers_keyboard():
    """Клавиатура для списка воркеров"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")]
        ]
    )

# ========== ОБРАБОТЧИКИ КОМАНД ==========

@dp.message(Command("start"))
async def cmd_start(message: Message):
    """Обработчик команды /start"""
    user_id = message.from_user.id
    username = message.from_user.username or f"user_{user_id}"
    display_username = f"@{username}" if message.from_user.username else str(user_id)
    
    user_info = await get_user_info(user_id)
    
    if user_info:
        is_admin = bool(user_info[2])
        await message.answer(
            "Добро пожаловать в систему управления мамонтами!",
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
                "Вас нет в базе данных воркеров.\n"
                "Хотите подать заявку на присоединение к команде?",
                reply_markup=get_application_decision_keyboard()
            )

@dp.message(Command("касса", ignore_case=True))
async def cmd_cash_in_group(message: Message):
    """Обработчик команды /касса в супергруппе"""
    if message.chat.id == SUPERGROUP_ID:
        today = await get_cash_stats('today')
        week = await get_cash_stats('week')
        month = await get_cash_stats('month')
        all_time = await get_cash_stats('all')
        
        cash_message = (
            "💰 *КАССА ПРОЕКТА*\n\n"
            f"▪️ Сегодня: *${today:.2f}*\n"
            f"▪️ Неделя: *${week:.2f}*\n"
            f"▪️ Месяц: *${month:.2f}*\n"
            f"▪️ Всё время: *${all_time:.2f}*\n\n"
            f"📅 Обновлено: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
            "➖➖➖➖➖➖➖➖"
        )
        
        try:
            await send_cash_to_channel(cash_message)
            
            if message.message_thread_id != GROUP_TOPICS["cash"]:
                await message.reply("✅ Касса отправлена в соответствующую тему!")
        except Exception as e:
            logger.error(f"Error sending cash to channel: {e}")
            await message.reply("❌ Ошибка при отправке кассы. Проверьте права бота.")
    else:
        await message.answer("Эта команда работает только в супергруппе проекта")

@dp.message(Command("getid"))
async def cmd_get_id(message: Message):
    """Команда для получения ID чата и темы"""
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

# ========== ОБРАБОТЧИКИ ЗАЯВОК ==========

@dp.callback_query(F.data == "submit_application")
async def process_submit_application(callback: CallbackQuery):
    """Обработчик подачи заявки"""
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
                await bot.send_message(
                    admin_id,
                    f"📝 Новая заявка на воркера!\n\n"
                    f"👤 Пользователь: {display_username}\n"
                    f"🆔 ID: {user_id}\n"
                    f"📅 Дата: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
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
    """Обработчик принятия заявки"""
    user_info = await get_user_info(callback.from_user.id)
    if not user_info or not user_info[2]:
        await callback.answer("❌ Доступ запрещен")
        return
    
    user_id = int(callback.data.split("_")[2])
    
    async with aiosqlite.connect('workers.db') as db:
        cursor = await db.execute(
            'SELECT id, username FROM applications WHERE user_id = ?',
            (user_id,)
        )
        application = await cursor.fetchone()
    
    if not application:
        await callback.answer("Заявка не найдена")
        return
    
    application_id, username = application
    
    success, accepted_user_id, accepted_username = await accept_application(application_id)
    
    if success:
        try:
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
        
        admins = await get_all_admins()
        for admin_id in admins:
            if admin_id != callback.from_user.id:
                try:
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
    """Обработчик отклонения заявки"""
    user_info = await get_user_info(callback.from_user.id)
    if not user_info or not user_info[2]:
        await callback.answer("❌ Доступ запрещен")
        return
    
    user_id = int(callback.data.split("_")[2])
    
    async with aiosqlite.connect('workers.db') as db:
        cursor = await db.execute(
            'SELECT id, username FROM applications WHERE user_id = ?',
            (user_id,)
        )
        application = await cursor.fetchone()
    
    if not application:
        await callback.answer("Заявка не найдена")
        return
    
    application_id, username = application
    
    success, rejected_user_id = await reject_application(application_id)
    
    if success:
        try:
            await bot.send_message(
                rejected_user_id,
                "❌ К сожалению, ваша заявка была отклонена администратором.\n\n"
                "Вы можете попробовать подать заявку снова через /start."
            )
        except Exception as e:
            logger.error(f"Error notifying user {rejected_user_id}: {e}")
        
        await callback.message.edit_text(
            f"❌ Заявка от {username} отклонена."
        )
        
        admins = await get_all_admins()
        for admin_id in admins:
            if admin_id != callback.from_user.id:
                try:
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

# ========== ОСНОВНЫЕ ОБРАБОТЧИКИ ==========

@dp.callback_query(F.data == "add_mammoth")
async def process_add_mammoth(callback: CallbackQuery, state: FSMContext):
    """Обработчик добавления мамонта"""
    user_info = await get_user_info(callback.from_user.id)
    if not user_info:
        await callback.answer("❌ Вы не воркер!")
        return
    
    await callback.message.answer("Введите @username или ID мамонта:")
    await state.set_state(AddMammothStates.waiting_for_mammoth)
    await callback.answer()

@dp.message(AddMammothStates.waiting_for_mammoth)
async def process_mammoth_input(message: Message, state: FSMContext):
    """Обработчик ввода мамонта"""
    mammoth_identifier = message.text.strip()
    user_info = await get_user_info(message.from_user.id)
    
    if not user_info:
        await message.answer("Ошибка: пользователь не найден")
        await state.clear()
        return
    
    worker_id = user_info[0]
    success, existing_worker = await add_mammoth(mammoth_identifier, worker_id)
    
    if success:
        await message.answer(f"✅ Мамонт {mammoth_identifier} успешно добавлен!")
    else:
        await message.answer(f"❌ Мамонт уже привязан к другому воркеру")
    
    await state.clear()

@dp.callback_query(F.data == "check_mammoth")
async def process_check_mammoth(callback: CallbackQuery, state: FSMContext):
    """Обработчик проверки мамонта"""
    user_info = await get_user_info(callback.from_user.id)
    if not user_info:
        await callback.answer("❌ Вы не воркер!")
        return
    
    await callback.message.answer("Введите @username или ID мамонта для проверки:")
    await state.set_state(CheckMammothStates.waiting_for_mammoth)
    await callback.answer()

@dp.message(CheckMammothStates.waiting_for_mammoth)
async def process_check_mammoth_input(message: Message, state: FSMContext):
    """Обработчик ввода мамонта для проверки"""
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
        await message.answer(f"❌ Мамонт {mammoth_identifier} не найден в базе данных")
    
    await state.clear()

@dp.callback_query(F.data == "cashbox")
async def process_cashbox(callback: CallbackQuery):
    """Обработчик кнопки Касса"""
    today = await get_cash_stats('today')
    week = await get_cash_stats('week')
    month = await get_cash_stats('month')
    all_time = await get_cash_stats('all')
    
    cash_message = (
        "💰 *КАССА ПРОЕКТА*\n\n"
        f"▪️ Сегодня: *${today:.2f}*\n"
        f"▪️ Неделя: *${week:.2f}*\n"
        f"▪️ Месяц: *${month:.2f}*\n"
        f"▪️ Всё время: *${all_time:.2f}*\n\n"
        "➖➖➖➖➖➖➖➖"
    )
    await callback.message.answer(cash_message, parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(F.data == "contacts")
async def process_contacts(callback: CallbackQuery):
    """Обработчик кнопки Контакты"""
    contacts = await get_contacts()
    contacts_message = (
        "📞 *КОНТАКТЫ*\n\n"
        f"👑 Владелец: {contacts.get('owner', 'Не указан')}\n"
        f"💰 Скуп: {contacts.get('buyer', 'Не указан')}\n"
        f"👨‍💼 Куратор: {contacts.get('curator', 'Не указан')}\n"
        f"🛒 Шоп: {contacts.get('shop', 'Не указан')}\n\n"
        "➖➖➖➖➖➖➖➖"
    )
    await callback.message.answer(contacts_message, parse_mode="Markdown")
    await callback.answer()

# ========== АДМИНСКИЕ ФУНКЦИИ ==========

@dp.callback_query(F.data == "add_profit")
async def process_add_profit(callback: CallbackQuery, state: FSMContext):
    """Обработчик добавления профита"""
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
    """Обработчик ввода воркера для профита"""
    worker_username = message.text.strip()
    await state.update_data(worker_username=worker_username)
    await message.answer("Введите сумму профита (например: 1000):")
    await state.set_state(AddProfitStates.waiting_for_amount)

@dp.message(AddProfitStates.waiting_for_amount)
async def process_profit_amount(message: Message, state: FSMContext):
    """Обработчик ввода суммы профита"""
    try:
        amount = float(message.text.strip())
        if amount <= 0:
            await message.answer("❌ Сумма должна быть положительной. Введите сумму профита:")
            return
        
        await state.update_data(amount=amount)
        await message.answer("Введите процент воркера (например: 70):")
        await state.set_state(AddProfitStates.waiting_for_percent)
    except ValueError:
        await message.answer("❌ Пожалуйста, введите корректную сумму (число)")

@dp.message(AddProfitStates.waiting_for_percent)
async def process_profit_percent(message: Message, state: FSMContext):
    """Обработчик ввода процента профита"""
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
            worker_amount = result
            project_amount = amount - worker_amount
            
            profit_message = (
                "💰 *НОВЫЙ ПРОФИТ!*\n\n"
                f"▪️ Воркер: {worker_username}\n"
                f"▪️ Сумма: *${amount:.2f}*\n"
                f"▪️ Процент воркера: *{percent}%*\n"
                f"▪️ Выплата воркеру: *${worker_amount:.2f}*\n"
                f"▪️ Доход проекта: *${project_amount:.2f}*\n\n"
                f"📅 Дата: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
                "➖➖➖➖➖➖➖➖"
            )
            
            try:
                await add_profit_to_channel(profit_message)
                await message.answer("✅ Профит успешно добавлен и опубликован в супергруппе!")
            except Exception as e:
                logger.error(f"Error sending to channel: {e}")
                await message.answer("✅ Профит добавлен, но произошла ошибка при отправке в супергруппу.")
        else:
            error_msg = result if isinstance(result, str) else "Неизвестная ошибка"
            await message.answer(f"❌ Ошибка при добавлении профита: {error_msg}")
        
        await state.clear()
    except ValueError:
        await message.answer("❌ Пожалуйста, введите корректный процент (целое число)")

@dp.callback_query(F.data == "edit_contact")
async def process_edit_contact(callback: CallbackQuery):
    """Обработчик изменения контактов"""
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
    """Обработчик выбора контакта для изменения"""
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
        await callback.message.answer(f"Введите новый @username для {role}:")
        await state.set_state(EditContactStates.waiting_for_username)
    
    await callback.answer()

@dp.message(EditContactStates.waiting_for_username)
async def process_new_username(message: Message, state: FSMContext):
    """Обработчик ввода нового username для контакта"""
    data = await state.get_data()
    role = data['role']
    new_username = message.text.strip()
    
    if not new_username.startswith('@'):
        new_username = f"@{new_username}"
    
    await update_contact(role, new_username)
    
    contacts = await get_contacts()
    contacts_message = (
        "✅ Контакты обновлены!\n\n"
        "📞 *КОНТАКТЫ*\n\n"
        f"👑 Владелец: {contacts.get('owner', 'Не указан')}\n"
        f"💰 Скуп: {contacts.get('buyer', 'Не указан')}\n"
        f"👨‍💼 Куратор: {contacts.get('curator', 'Не указан')}\n"
        f"🛒 Шоп: {contacts.get('shop', 'Не указан')}\n\n"
        "➖➖➖➖➖➖➖➖"
    )
    await message.answer(contacts_message, parse_mode="Markdown")
    await state.clear()

@dp.callback_query(F.data == "project_stats")
async def process_project_stats(callback: CallbackQuery):
    """Обработчик статистики проекта"""
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
        f"▪️ Общая сумма профитов: *${stats['total_profits']:.2f}*\n"
        f"▪️ Выплачено воркерам: *${stats['total_payouts']:.2f}*\n"
        f"▪️ Доход проекта: *${stats['project_income']:.2f}*\n\n"
        "➖➖➖➖➖➖➖➖"
    )
    await callback.message.answer(stats_message, parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(F.data == "view_applications")
async def process_view_applications(callback: CallbackQuery):
    """Обработчик просмотра заявок"""
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
    """Обработчик возврата в главное меню"""
    user_info = await get_user_info(callback.from_user.id)
    is_admin = bool(user_info[2]) if user_info else False
    await callback.message.answer(
        "Главное меню:",
        reply_markup=get_main_keyboard(is_admin)
    )
    await callback.answer()

# ========== КОМАНДЫ ДЛЯ ТЕСТИРОВАНИЯ ==========

@dp.message(Command("test"))
async def cmd_test(message: Message):
    """Тестовая команда"""
    user_info = await get_user_info(message.from_user.id)
    if not user_info or not user_info[2]:
        return
    
    # Тест отправки в супергруппу
    try:
        # Тест в тему профитов
        await send_to_topic(
            GROUP_TOPICS["profits"],
            "✅ Тестовое сообщение в тему профитов",
            "Markdown"
        )
        
        # Тест в тему кассы
        await send_to_topic(
            GROUP_TOPICS["cash"],
            "✅ Тестовое сообщение в тему кассы",
            "Markdown"
        )
        
        await message.answer("✅ Тестовые сообщения отправлены в супергруппу!")
    except Exception as e:
        await message.answer(f"❌ Ошибка при отправке тестовых сообщений: {e}")

@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    """Команда для быстрой проверки статистики"""
    stats = await get_project_stats()
    stats_message = (
        "📊 *СТАТИСТИКА ПРОЕКТА*\n\n"
        f"▪️ Мамонты: {stats['mammoths']}\n"
        f"▪️ Воркеры: {stats['workers']}\n"
        f"▪️ Заявки: {stats['pending_applications']}\n"
        f"▪️ Профиты: ${stats['total_profits']:.2f}\n"
        f"▪️ Выплаты: ${stats['total_payouts']:.2f}\n"
        f"▪️ Доход: ${stats['project_income']:.2f}"
    )
    await message.answer(stats_message, parse_mode="Markdown")

# ========== ГЛАВНАЯ ФУНКЦИЯ ==========

async def main():
    """Главная функция запуска бота"""
    await init_db()
    print("=" * 50)
    print("🤖 Бот запущен!")
    print(f"🔗 Супергруппа ID: {SUPERGROUP_ID}")
    print(f"📊 Тема для профитов: {GROUP_TOPICS['profits']}")
    print(f"💰 Тема для кассы: {GROUP_TOPICS['cash']}")
    print(f"👑 Админы: {ADMIN_IDS}")
    print("=" * 50)
    
    # Запускаем бота
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())