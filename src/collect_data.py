import time
import os
import json
import requests

# Параметры API
API_URL_PRICES = "https://api.travelpayouts.com/aviasales/v3/get_latest_prices"
API_URL_DETAILS = "https://api.travelpayouts.com/aviasales/v3/prices_for_dates"
API_TOKEN = "514e72fb5a73dac81132e6e4fb2b92bf"

# Директория для сохранения файлов
OUTPUT_DIR = "data/routes_archive/routes"

# Лимит запросов в минуту
API_REQUEST_LIMIT = 200

# Пути к файлам
AIRPORTS_FILE = "data/raw/russian_airports.json"
PROGRESS_FILE = "data\logs\progress.log"

def load_airports_data():
    """Загружает данные об аэропортах из файла и преобразует их в словарь."""
    with open(AIRPORTS_FILE, "r", encoding="utf-8") as f:
        airports_list = json.load(f)
    airports_dict = {airport["code"]: airport for airport in airports_list}
    return airports_dict

def fetch_prices_data(origin, destination):
    """Получает данные о ценах для указанного маршрута."""
    params = {
        "origin": origin,
        "destination": destination,
        "beginning_of_period": "2024-12",
        "period_type": "year",
        "group_by": "dates",
        "currency": "rub",
        "limit": 1000,
        "token": API_TOKEN,
    }
    try:
        response = requests.get(API_URL_PRICES, params=params)
        if response.status_code == 200:
            return response.json().get("data", [])
        elif response.status_code == 429:  # Превышен лимит запросов
            print("Превышен лимит запросов (get_latest_prices). Ожидание 60 секунд...")
            time.sleep(60)
            return None
        else:
            print(f"Ошибка API (get_latest_prices): {response.status_code} для {origin} -> {destination}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Ошибка запроса (get_latest_prices): {e}")
        return None

def fetch_flight_details(origin, destination):
    """Получает детальную информацию о рейсах для указанного маршрута."""
    params = {
        "origin": origin,
        "destination": destination,
        "beginning_of_period": "2024-12",
        "period_type": "year",
        "sorting": "price",
        "direct": "false",
        "currency": "rub",
        "limit": 1000,
        "one_way": "true",
        "token": API_TOKEN,
    }
    try:
        response = requests.get(API_URL_DETAILS, params=params)
        if response.status_code == 200:
            return response.json().get("data", [])
        elif response.status_code == 429:  # Превышен лимит запросов
            print("Превышен лимит запросов (prices_for_dates). Ожидание 60 секунд...")
            time.sleep(60)
            return None
        else:
            print(f"Ошибка API (prices_for_dates): {response.status_code} для {origin} -> {destination}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Ошибка запроса (prices_for_dates): {e}")
        return None

def combine_data(prices_data, flight_details):
    """Объединяет данные из двух запросов."""
    combined = []
    details_map = {flight["departure_at"]: flight for flight in flight_details}

    for price in prices_data:
        departure_time = price.get("depart_date")
        detail = details_map.get(departure_time, {})
        combined_entry = {
            **price,
            **detail,
            "price": price.get("value"),
            "flight_number": detail.get("flight_number"),
            "link": detail.get("link"),
            "duration": detail.get("duration"),
            "transfers": detail.get("transfers"),
        }
        combined.append(combined_entry)
    return combined

def enrich_data_with_airport_info(data, airport_to_info, origin, destination):
    """Обогащает данные рейсов информацией об аэропортах."""
    enriched_data = []
    for flight in data:
        origin_info = airport_to_info.get(origin, {})
        destination_info = airport_to_info.get(destination, {})
        flight["origin_airport_name"] = origin_info.get("name", "")
        flight["origin_city_code"] = origin_info.get("city_code", "")
        flight["origin_coordinates"] = origin_info.get("coordinates", {})
        flight["origin_time_zone"] = origin_info.get("time_zone", "")
        flight["destination_airport_name"] = destination_info.get("name", "")
        flight["destination_city_code"] = destination_info.get("city_code", "")
        flight["destination_coordinates"] = destination_info.get("coordinates", {})
        flight["destination_time_zone"] = destination_info.get("time_zone", "")
        enriched_data.append(flight)
    return enriched_data

def save_to_route_file(data, origin, destination):
    """Сохраняет данные для каждого маршрута в отдельный файл."""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    file_name = f"{origin}_TO_{destination}.json"
    file_path = os.path.join(OUTPUT_DIR, file_name)
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"Данные сохранены в {file_path}")

def load_progress_log():
    """Загружает список уже обработанных маршрутов."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            return {line.strip() for line in f}
    return set()

def update_progress_log(origin, destination):
    """Обновляет лог обработанных маршрутов."""
    with open(PROGRESS_FILE, "a", encoding="utf-8") as f:
        f.write(f"{origin} -> {destination}\n")

def main():
    # Засекаем время начала
    start_time = time.time()

    # Загрузка справочника аэропортов
    airport_to_info = load_airports_data()

    # Получение всех кодов аэропортов
    airport_codes = list(airport_to_info.keys())

    # Загрузка списка уже обработанных маршрутов
    processed_routes = load_progress_log()

    # Лимит запросов в рамках одного запуска
    request_count = 0
    processed_count = len(processed_routes)
    total_routes = len(airport_codes) * (len(airport_codes) - 1)

    print(f"Всего маршрутов для обработки: {total_routes}")
    print(f"Уже обработано: {processed_count}")

    interval_start_time = time.time()

    # Перебор всех пар аэропортов
    for origin in airport_codes:
        for destination in airport_codes:
            if origin != destination:
                route = f"{origin} -> {destination}"
                if route in processed_routes:
                    continue

                # Получение данных
                prices_data = fetch_prices_data(origin, destination)
                flight_details = fetch_flight_details(origin, destination)

                if prices_data and flight_details:
                    combined_data = combine_data(prices_data, flight_details)
                    enriched_data = enrich_data_with_airport_info(combined_data, airport_to_info, origin, destination)
                    save_to_route_file(enriched_data, origin, destination)

                update_progress_log(origin, destination)
                processed_count += 1
                print(f"Обработано маршрутов: {processed_count}/{total_routes}")

                request_count += 2  # Учитываем два запроса
                if request_count >= API_REQUEST_LIMIT:
                    elapsed_time = time.time() - interval_start_time
                    if elapsed_time < 60:
                        pause_time = 60 - elapsed_time
                        print(f"Достигнут лимит запросов. Ожидание {pause_time:.2f} секунд...")
                        time.sleep(pause_time)
                    request_count = 0
                    interval_start_time = time.time()

    end_time = time.time()
    print(f"Скрипт завершён. Обработано маршрутов: {processed_count}/{total_routes}")
    print(f"Общее время выполнения: {(end_time - start_time) / 60:.2f} минут.")

if __name__ == "__main__":
    main()
