import json
from pymongo import MongoClient, errors

# Настройки MongoDB
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "flights_database"
COLLECTION_NAME = "routes"

# Настройки файла данных
DATA_FILE = "data\processed/all_routes_combined.json"
BATCH_SIZE = 1000  # Количество записей для загрузки за один раз


def connect_to_mongo():
    """Подключение к MongoDB."""
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        print("Успешное подключение к MongoDB.")
        return collection
    except errors.ConnectionError as e:
        print(f"Ошибка подключения к MongoDB: {e}")
        return None


def load_data_in_batches(collection):
    """Загрузка данных в MongoDB."""
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as file:
            data = json.load(file)

        total_records = len(data)
        print(f"Всего записей в файле: {total_records}")

        for i in range(0, total_records, BATCH_SIZE):
            batch = data[i:i + BATCH_SIZE]
            collection.insert_many(batch, ordered=False)
            print(f"Загружено записей: {i + len(batch)} / {total_records}")

        print("Все данные успешно загружены в MongoDB.")
    except json.JSONDecodeError as e:
        print(f"Ошибка чтения JSON-файла: {e}")
    except errors.BulkWriteError as e:
        print(f"Ошибка записи данных в MongoDB: {e}")
    except Exception as e:
        print(f"Неизвестная ошибка: {e}")


def main():
    collection = connect_to_mongo()
    if collection is not None:
        load_data_in_batches(collection)


if __name__ == "__main__":
    main()
