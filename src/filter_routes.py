import pandas as pd
import os

folder = "D:\DataScience\data/routes_archive/routes"
output_folder = "D:\DataScience\data/routes_archive/filtered_routes"

columns_to_keep = [
    "depart_date",
    "gate",
    "found_at",
    "trip_class",
    "value",
    "number_of_changes",
    "duration",
    "distance",
    "origin_airport",
    "destination_airport",
    "airline",
    "origin_airport_name",
    "origin_city_code",
    "destination_airport_name",
    "destination_city_code"
]

broken_files = []
filtered_files = []

if not os.path.exists(output_folder):
    os.makedirs(output_folder)
for f in os.listdir(folder):
    if f.endswith(".json"):
        try:
            df = pd.read_json(folder + "/" + f)
                
            if set(columns_to_keep).issubset(df.columns):
                df_filtered = df[columns_to_keep]
                output_file = os.path.join(output_folder, f)
                df_filtered.to_json(output_file, orient="records", lines=True)
                print(f"Файл {f} успешно обработан и сохранен")
                filtered_files.append(f)
                    
            else:
                print(f"Файл {f} пропущен, так как отсутсвуют необходимые столбцы")
                broken_files.append(f)
                    
        except Exception as e:
            print(f"Ошибка приобработке файла {f}: {e}")

print("Файлы обработаны")
print(f"Файлов успешно отфильтровано: {len(filtered_files)}")
print(f"Сломанных файлов: {len(broken_files)}")
  

