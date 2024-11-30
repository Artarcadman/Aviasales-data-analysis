import os
import json

# Укажите папку с JSON-файлами
input_folder = "data/raw/russian_airports.json"
output_file = "data\processed/all_routes_combined.json"
log_file = "data\logs\progress_fetch.log"

# Проверка существования лог-файла
if os.path.exists(log_file):
    with open(log_file, "r", encoding="utf-8") as log:
        processed_files = set(log.read().splitlines())
else:
    processed_files = set()

# Сбор всех файлов в папке
files = [f for f in os.listdir(input_folder) if f.endswith(".json")]
total_files = len(files)

print(f"Найдено файлов для обработки: {total_files}")

# Открываем итоговый файл для потоковой записи
with open(output_file, "a", encoding="utf-8") as output:
    # Если файл пустой, начнем с открытия массива
    if os.stat(output_file).st_size == 0:
        output.write("[")
        first_record = True
    else:
        first_record = False

    # Обработка файлов
    for index, file_name in enumerate(files, start=1):
        if file_name in processed_files:
            print(f"Файл {file_name} уже обработан, пропускаем.")
            continue

        file_path = os.path.join(input_folder, file_name)
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                data = json.load(file)
                if isinstance(data, list):  # Проверяем, что файл содержит список
                    for record in data:
                        if not first_record:
                            output.write(",")  # Добавляем запятую перед новой записью
                        json.dump(record, output, ensure_ascii=False)
                        first_record = False

            # Логирование обработанного файла
            with open(log_file, "a", encoding="utf-8") as log:
                log.write(file_name + "\n")

            print(f"[{index}/{total_files}] Файл: {file_name} успешно обработан.")
        except json.JSONDecodeError as e:
            print(f"Ошибка чтения файла {file_name}: {e}")
        except Exception as e:
            print(f"Неожиданная ошибка при обработке файла {file_name}: {e}")

    # Завершаем JSON-массив
    if index == total_files:
        output.write("]")

print(f"Объединение данных завершено. Итоговый файл: {output_file}")
