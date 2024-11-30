import json
import os

# Пути к файлам
input_file = 'D:/DataScience/data/russian_airports.json'  # Оригинальный файл
output_file = 'russian_airports_codes.json'  # Новый файл

# Открытие оригинального файла
with open(input_file, 'r', encoding='utf-8') as f:
    airports_data = json.load(f)

# Новый список для хранения отфильтрованных данных
restructured_data = []

# Цикл по аэропортам для отбора нужных полей
for airport in airports_data:
    restructured_airport = {
        "city_code": airport.get("city_code"),
        "code": airport.get("code"),
        "name": airport.get("name")
    }
    restructured_data.append(restructured_airport)

# Сохранение реструктурированных данных в новый файл
with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(restructured_data, f, ensure_ascii=False, indent=4)

print(f"Файл реструктурирован и сохранен как {output_file}")
