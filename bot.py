import asyncio
import logging
import os
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
import aiohttp
from collections import defaultdict

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования с форматированием
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Инициализация бота
bot = Bot(token=os.getenv("BOT_TOKEN"))
dp = Dispatcher()


# Базовый URL для Bybit API v5
BYBIT_API_URL = "https://api.bybit.com"

# Словарь для хранения последних цен
last_prices = {}
# Словарь для хранения сигналов за последние 24 часа
signals_24h = defaultdict(list)

# Параметры для отслеживания
SMALL_PUMP_THRESHOLD = 5  # Небольшое движение цены (5% за 2 минуты)
BIG_PUMP_THRESHOLD = 10    # Сильное движение цены (10% за 2 минуты)
MONITORING_INTERVAL = 120  # интервал проверки в секундах
MIN_VOLUME_24H = 100000  # Минимальный объем торгов за 24 часа в USD

# Файл для хранения списка разрешенных пользователей
ALLOWED_USERS_FILE = "allowed_users.json"

def load_allowed_users():
    """Загрузка списка разрешенных пользователей из файла"""
    try:
        with open(ALLOWED_USERS_FILE, 'r') as f:
            a = json.load(f)
            return a
    except FileNotFoundError:
        return set()
    
allowed_users = load_allowed_users()    

def clean_old_signals():
    """Очистка старых сигналов (старше 24 часов)"""
    current_time = datetime.now()
    for symbol in list(signals_24h.keys()):
        signals_24h[symbol] = [signal for signal in signals_24h[symbol]
                             if (current_time - signal).total_seconds() < 86400]

async def get_all_symbols():
    """Получение списка всех торговых пар"""
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f"{BYBIT_API_URL}/v5/market/instruments-info", 
                                 params={"category": "spot"}) as response:
                data = await response.json()
                if data["retCode"] == 0:
                    return [item["symbol"] for item in data["result"]["list"]]
                return []
        except Exception as e:
            logging.error(f"Ошибка при получении списка символов: {e}")
            return []

async def get_market_data(symbol: str):
    """Получение рыночных данных с Bybit"""
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f"{BYBIT_API_URL}/v5/market/tickers", 
                                 params={"category": "spot", "symbol": symbol}) as response:
                data = await response.json()
                if data["retCode"] == 0 and data["result"]["list"]:
                    return data["result"]["list"][0]
                return None
        except Exception as e:
            logging.error(f"Ошибка при получении данных: {e}")
            return None

async def check_price_movement(symbol: str, current_price: float):
    """Проверка движения цены"""
    if symbol not in last_prices:
        last_prices[symbol] = current_price
        return False, 0
    
    price_change = ((current_price - last_prices[symbol]) / last_prices[symbol]) * 100
    last_prices[symbol] = current_price
    
    # Определяем тип движения цены
    if abs(price_change) >= BIG_PUMP_THRESHOLD:
        return True, price_change  # Сильное движение
    elif abs(price_change) >= SMALL_PUMP_THRESHOLD:
        return True, price_change  # Небольшое движение
    
    return False, 0

async def get_all_market_data():
    """Получение рыночных данных для всех монет одним запросом"""
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f"{BYBIT_API_URL}/v5/market/tickers", 
                                 params={"category": "spot"}) as response:
                data = await response.json()
                if data["retCode"] == 0 and data["result"]["list"]:
                    # Создаем словарь для быстрого доступа ко всем монетам
                    return {
                        item["symbol"]: item 
                        for item in data["result"]["list"]
                    }
                return {}
        except Exception as e:
            logging.error(f"Ошибка при получении данных всех монет: {e}")
            return {}

async def monitor_prices():
    """Мониторинг цен криптовалют"""
    iteration_counter = 0
    while True:
        iteration_counter += 1
        logging.info(f"🔄 Начало итерации мониторинга #{iteration_counter}")
        try:
            # Обновляем список разрешенных пользователей
            global allowed_users
            allowed_users = load_allowed_users()
            logging.info(f"👥 Загружен список разрешенных пользователей: {len(allowed_users)} пользователей")
            
            start_time = datetime.now()
            
            # Получаем данные всех монет одним запросом
            market_data_dict = await get_all_market_data()
            symbols = list(market_data_dict.keys())
            logging.info(f"📊 Получено {len(symbols)} торговых пар")
            
            signals_count = 0
            processed_count = 0
            skipped_low_volume = 0
            
            # Обрабатываем полученные данные
            for symbol in symbols:
                processed_count += 1
                market_data = market_data_dict[symbol]
                
                try:
                    current_price = float(market_data["lastPrice"])
                    price_change_24h = float(market_data["price24hPcnt"]) * 100
                    volume_24h = float(market_data["turnover24h"])
                    
                    # Пропускаем пары с низким объемом торгов
                    if volume_24h < MIN_VOLUME_24H:
                        skipped_low_volume += 1
                        continue
                        
                except (KeyError, ValueError) as e:
                    logging.warning(f"⚠️ Ошибка обработки данных для {symbol}: {e}")
                    continue
                
                has_signal, price_change = await check_price_movement(symbol, current_price)
                if has_signal:
                    signals_count += 1
                    # Добавляем сигнал в историю
                    signals_24h[symbol].append(datetime.now())
                    
                    # Очищаем старые сигналы
                    clean_old_signals()
                    
                    # Определяем тип движения для эмодзи
                    if abs(price_change) >= BIG_PUMP_THRESHOLD:
                        movement_emoji = "🚀" if price_change > 0 else "💥"
                    else:
                        movement_emoji = "📈" if price_change > 0 else "📉"
                    
                    # Формируем сообщение
                    message = (
                        f"{movement_emoji} Bybit - {symbol}\n"
                        f"💰 Объём торгов 24ч: ${volume_24h:,.2f}\n"
                        f"📊 Изменение цены за 2 мин: {price_change:.2f}%\n"
                        f"📈 Изменение цены 24ч: {price_change_24h:.2f}%\n"
                        f"💵 Текущая цена: ${current_price:,.8f}\n"
                        f"🔄 Количество сигналов за 24ч: {len(signals_24h[symbol])}"
                    )
                    
                    logging.info(f"🚨 Обнаружен сигнал для {symbol}: изменение цены {price_change:.2f}%")
                    
                    # Отправляем сигнал всем разрешенным пользователям
                    for user_id in allowed_users:
                        try:
                            asyncio.create_task(bot.send_message(chat_id=user_id, text=message))
                        except Exception as e:
                            logging.error(f"❌ Ошибка отправки сообщения пользователю {user_id}: {e}")
                
                if processed_count % 50 == 0:  # Логируем каждые 50 обработанных пар
                    logging.info(f"✅ Обработано {processed_count}/{len(symbols)} пар")
            
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            logging.info(
                f"📈 Итоги итерации #{iteration_counter}:\n"
                f"   - Всего пар: {len(symbols)}\n"
                f"   - Обработано пар: {processed_count}\n"
                f"   - Пропущено из-за низкого объема: {skipped_low_volume}\n"
                f"   - Найдено сигналов: {signals_count}\n"
                f"   - Время выполнения: {execution_time:.2f} сек\n"
                f"   - Среднее время на пару: {(execution_time/processed_count if processed_count else 0):.3f} сек"
            )
            
        except Exception as e:
            logging.error(f"❌ Ошибка в мониторинге: {e}")
        
        await asyncio.sleep(MONITORING_INTERVAL)

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    if message.from_user.id in allowed_users:
        await message.answer(
            "👋 Привет! Я бот для мониторинга цен на Bybit.\n"
            "Я буду отправлять уведомления о резких движениях цен и пампах."
        )
    else:
        await message.answer(
            "👋 Привет! К сожалению, у вас нет доступа к этому боту.\n"
            "Обратитесь к администратору для получения доступа."
        )

async def main():
    logging.info("🚀 Запуск бота для мониторинга цен Bybit")
    logging.info(f"⚙️ Настройки:\n"
                f"   - Порог небольшого движения: {SMALL_PUMP_THRESHOLD}%\n"
                f"   - Порог сильного движения: {BIG_PUMP_THRESHOLD}%\n"
                f"   - Минимальный объем торгов: ${MIN_VOLUME_24H:,}\n"
                f"   - Интервал проверки: {MONITORING_INTERVAL} сек")
    
    # Запускаем мониторинг цен в фоновом режиме
    asyncio.create_task(monitor_prices())
    # Запускаем бота
    logging.info("🤖 Бот запущен и готов к работе")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main()) 