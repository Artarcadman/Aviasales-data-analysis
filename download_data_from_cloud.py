import os
import requests

# URL файла в облаке
FILE_URL = "hhttps://drive.google.com/file/d/1OLuYjbuA3MEqNwyggz6HuuC6bdDjcTt7/view?usp=sharing"
LOCAL_PATH = "data/all_routes_combined.json"

def download_file(url, local_path):
    if not os.path.exists(local_path):
        print(f"Загрузка файла из {url}...")
        response = requests.get(url, stream=True)
        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print("Файл успешно загружен.")
    else:
        print("Файл уже существует. Загрузка не требуется.")

download_file(FILE_URL, LOCAL_PATH)
