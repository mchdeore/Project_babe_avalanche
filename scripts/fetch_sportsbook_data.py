#!/usr/bin/env python3

import requests
import os
import pandas
import yaml
from dotenv import load_dotenv

with open("config.yaml") as f:
    config = yaml.safe_load(f)

SPORTS = config["sports"]
MARKETS = config["markets"]
BOOKS = set(config["books"])
INTERVAL = config["polling"]["interval_seconds"]
DB_PATH = config["storage"]["database"]


load_dotenv()
API_KEY = os.getenv("ODDS_API_KEY")

url = "https://api.the-odds-api.com/v4/sports/basketball_nba/odds"

params = {
    "apiKey": API_KEY,
    "regions": "us",
    "markets": "h2h",
    "oddsFormat": "decimal"
}

response = requests.get(url, params=params)
response.raise_for_status()
games = response.json()

print(f"Successfully retrieved {len(games)} games")
print(games)