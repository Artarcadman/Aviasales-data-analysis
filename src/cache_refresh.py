import time
import logging
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# === Настройки ===
AIRPORTS_FILE = "data/raw/russian_airports.json"  # Файл с аэропортами
BASE_URL = "https://www.aviasales.ru/search"
START_DATE = "2024-12-01"  # Начальная дата
END_DATE = "2025-11-01"    # Конечная дата
LOG_FILE = "data/logs/cache_refresh.log"

# === Логирование ===
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter("%(message)s"))
logging.getLogger().addHandler(console_handler)

# === Функция для загрузки списка кодов аэропортов ===
def load_airport_codes(file_path):
    import json
    with open(file_path, "r", encoding="utf-8") as f:
        airports = json.load(f)
    codes = [airport["code"] for airport in airports if airport["flightable"]]
    logging.info(f"Загружено {len(codes)} кодов аэропортов.")
    return codes

# === Функция для генерации всех дат ===
def generate_dates(start_date, end_date):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    dates = []
    while start <= end:
        dates.append(start.strftime("%d%m"))  # Формат "DDMM"
        start += timedelta(days=1)
    logging.info(f"Сгенерировано {len(dates)} дат.")
    return dates

# === Настройка браузера ===
def setup_browser():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  # Без отображения браузера
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    logging.info("Браузер успешно настроен.")
    return driver

# === Основная функция ===
def main():
    start_time = time.time()  # Засекаем время начала работы

    # Загружаем коды аэропортов
    airport_codes = load_airport_codes(AIRPORTS_FILE)

    # Подготавливаем браузер
    browser = setup_browser()

    # Генерируем даты
    all_dates = generate_dates(START_DATE, END_DATE)

    # Инициализация счётчиков
    total_routes = len(airport_codes) * (len(airport_codes) - 1) * len(all_dates)
    processed_routes = 0

    logging.info(f"Всего маршрутов для обработки: {total_routes}")

    # Перебор маршрутов
    for origin in airport_codes:
        for destination in airport_codes:
            if origin != destination:  # Исключаем маршруты в тот же аэропорт
                for date in all_dates:
                    url = f"{BASE_URL}/{origin}{date}{destination}1"
                    logging.info(f"Обработка маршрута: {origin} -> {destination}, дата: {date}")
                    logging.info(f"Открытие URL: {url}")

                    # Засекаем время открытия страницы
                    route_start_time = time.time()
                    try:
                        browser.get(url)
                        elapsed_time = time.time() - route_start_time
                        logging.info(f"Страница загрузилась за {elapsed_time:.2f} секунд.")
                    except Exception as e:
                        logging.error(f"Ошибка при обработке маршрута {origin} -> {destination}, дата: {date}: {e}")
                        continue

                    # Добавляем задержку, чтобы избежать блокировки
                    time.sleep(2)
                    processed_routes += 1

                    # Логируем прогресс
                    if processed_routes % 100 == 0:
                        logging.info(f"Обработано маршрутов: {processed_routes}/{total_routes}")

    # Закрываем браузер
    browser.quit()

    # Итоговая статистика
    end_time = time.time()
    elapsed_time = (end_time - start_time) / 60
    logging.info(f"Обработка завершена. Всего обработано маршрутов: {processed_routes}/{total_routes}")
    logging.info(f"Общее время выполнения: {elapsed_time:.2f} минут.")

if __name__ == "__main__":
    main()
