import requests
import json
import pandas as pd
URL = 'https://api.travelpayouts.com/data/ru/airlines.json?_gl=1*i6anp0*_ga*MTg4NjYzNTgxNS4xNzM4NTkwMDQw*_ga_1WLL0NEBEH*MTczODY5NjEwNS4zLjEuMTczODY5NjEzOC4yNy4wLjA.'

response = requests.get(URL)

data = response.json()

df = pd.DataFrame(data)
df.to_json("airlines.json", orient="records", lines=True)




