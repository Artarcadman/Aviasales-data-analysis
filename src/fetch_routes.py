import os
import json
from tqdm import tqdm  # Для прогресс-бара (опционально)

# Конфигурация
input_folder = "D:\DataScience\data/routes_archive/filtered_routes"
output_file = "D:\DataScience\data/processed/all_routes_filtered/all_routrs_filtered.jsonl"
log_file = "D:\DataScience\data\logs\log_file_combine_routes"

# Создаем папки, если их нет
os.makedirs(os.path.dirname(output_file), exist_ok=True)
os.makedirs(os.path.dirname(log_file), exist_ok=True)

# Сбор списка файлов
files = [f for f in os.listdir(input_folder) if f.endswith(".json")]
total_files = len(files)

# Логирование в файл и терминал
def log(message, to_console=True):
    with open(log_file, "a", encoding="utf-8") as log:
        log.write(message + "\n")
    if to_console:
        print(message)

# Подсчет общего числа маршрутов
total_routes = 0
for file in files:
    with open(os.path.join(input_folder, file), "r", encoding="utf-8") as f:
        total_routes += sum(1 for _ in f)

# Инициализация счетчиков
processed_routes = 0
remaining_routes = total_routes

# Обработка данных
with open(output_file, "w", encoding="utf-8") as out:
    for file in tqdm(files, desc="Обработка файлов"):
        file_path = os.path.join(input_folder, file)
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    # Проверка валидности JSON
                    try:
                        record = json.loads(line.strip())
                    except json.JSONDecodeError:
                        log(f"Ошибка в файле {file}: некорректный JSON", to_console=False)
                        continue

                    # Запись в итоговый файл
                    out.write(line)
                    processed_routes += 1
                    remaining_routes -= 1

                    # Логирование
                    if processed_routes % 100 == 0:
                        log(f"Маршрут {processed_routes} занесен в общий файл")
                        log(f"Всего обработано записей: {processed_routes}")
                        log(f"Осталось внести в файл: {remaining_routes}")

        except Exception as e:
            log(f"Ошибка при обработке {file}: {str(e)}", to_console=False)

# Финальный вывод
log(f"Готово! Обработано маршрутов: {processed_routes}")