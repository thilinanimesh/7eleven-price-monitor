import requests

API_URL = "https://projectzerothree.info/api.php?format=json"

def fetch_prices():
    """Fetch all fuel prices from ProjectZeroThree API."""
    try:
        resp = requests.get(url=API_URL,timeout=10)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        print(f"Error fetching data: {e}")
        return None
    