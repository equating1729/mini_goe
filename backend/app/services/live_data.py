"""Live-data helpers (prices, FX, weather), copied from ml/rag/rag.py.

Kept separate so the API does not need chromadb/ollama just for these.
Each function returns a string, or None if the external API fails.
"""
import re
import requests


def get_gold_price():
    try:
        r = requests.get("https://api.gold-api.com/price/XAU", timeout=10)
        if r.status_code == 200:
            price = r.json().get("price", 0)
            if price:
                return f"Current gold price: ${price:.2f} per ounce"
    except Exception as e:
        print(f"Gold price fetch failed: {e}")
    return None


def get_bitcoin_price():
    try:
        r = requests.get(
            "https://api.coingecko.com/api/v3/simple/price"
            "?ids=bitcoin&vs_currencies=usd", timeout=10)
        if r.status_code == 200:
            price = r.json().get("bitcoin", {}).get("usd", 0)
            if price:
                return f"Current Bitcoin price: ${price:,.2f} USD"
    except Exception as e:
        print(f"Bitcoin price fetch failed: {e}")
    return None


def get_exchange_rate():
    try:
        r = requests.get("https://api.exchangerate-api.com/v4/latest/USD", timeout=10)
        if r.status_code == 200:
            inr = r.json().get("rates", {}).get("INR", 0)
            if inr:
                return f"Current exchange rate: 1 USD = {inr:.2f} INR"
    except Exception as e:
        print(f"Exchange rate fetch failed: {e}")
    return None


WEATHER_CODES = {
    0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Foggy", 48: "Rime fog", 51: "Light drizzle", 53: "Moderate drizzle",
    55: "Dense drizzle", 61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
    71: "Slight snow", 73: "Moderate snow", 75: "Heavy snow",
    80: "Slight rain showers", 81: "Moderate rain showers", 82: "Violent rain showers",
}


def get_weather_delhi():
    """Delhi only (fixed coordinates)."""
    try:
        r = requests.get(
            "https://api.open-meteo.com/v1/forecast"
            "?latitude=28.61&longitude=77.23&current_weather=true", timeout=10)
        if r.status_code == 200:
            cw = r.json().get("current_weather", {})
            desc = WEATHER_CODES.get(cw.get("weathercode", 0), "Unknown")
            return f"Current weather in Delhi: {desc}, {cw.get('temperature', 0):.1f}°C"
    except Exception as e:
        print(f"Weather fetch failed: {e}")
    return None


def handle_live_query(question: str):
    """Return a live-data answer if the question is clearly one, else None."""
    q = question.lower()
    if "gold price" in q or "price of gold" in q:
        return get_gold_price()
    if re.search(r"\bbitcoin\b|\bbtc\b", q):
        return get_bitcoin_price()
    if "exchange rate" in q or "dollar to rupee" in q or "usd to inr" in q:
        return get_exchange_rate()
    if "weather" in q:
        return get_weather_delhi()
    return None