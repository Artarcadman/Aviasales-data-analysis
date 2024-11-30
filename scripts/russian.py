import pandas as pd

# Загружаем данные о аэропортах
airports = pd.read_json("D:/DataScience/data/russian_airports.json")
airports_code = airports["code"]

df_airports = pd.DataFrame(airports_code)

# Сохраняем в Excel файл в директорию проекта
output_file = "D:\DataScience\data/russian_airports.json"
df_airports.to_json(output_file, index=False)

print(f"Файл сохранен как {output_file}")
