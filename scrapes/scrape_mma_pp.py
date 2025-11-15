import requests
import pandas as pd

# Change this to the stat you want
DESIRED_STATS = ["Significant Strikes"]

def scrape_prizepicks_ufc():
    url = "https://api.prizepicks.com/projections"
    params = {"league_id": "12", "per_page": "250"}  
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://app.prizepicks.com/",
        "Origin": "https://app.prizepicks.com"
    }

    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        
        # Print debugging info
        print(f"\n[DEBUG PP] Status Code: {response.status_code}")
        print(f"[DEBUG PP] Response Headers: {dict(response.headers)}")
        print(f"[DEBUG PP] Response Text (first 500 chars): {response.text[:500]}")
        
        # Check if response is successful
        response.raise_for_status()
        
        # Check if response has content
        if not response.text or response.text.strip() == "":
            print("[ERROR] PrizePicks returned empty response")
            return pd.DataFrame(columns=["Player", "Stat", "Line", "Start Time", "Odds Type"])
        
        # Try to parse JSON
        try:
            data = response.json()
        except requests.exceptions.JSONDecodeError as e:
            print(f"[ERROR] Failed to parse JSON from PrizePicks: {e}")
            print(f"[ERROR] Response content: {response.text[:1000]}")
            return pd.DataFrame(columns=["Player", "Stat", "Line", "Start Time", "Odds Type"])
        
        print(f"[DEBUG PP] Successfully parsed JSON. Keys: {data.keys() if isinstance(data, dict) else 'Not a dict'}")
        
        # Check if data has expected structure
        if not isinstance(data, dict) or "data" not in data:
            print(f"[ERROR] Unexpected data structure from PrizePicks: {type(data)}")
            return pd.DataFrame(columns=["Player", "Stat", "Line", "Start Time", "Odds Type"])
        
        # Each projection is in data["data"]
        projections = []
        for item in data.get("data", []):
            attr = item.get("attributes", {})
            stat_type = attr.get("stat_type")

            if stat_type in DESIRED_STATS and attr.get("odds_type") == "standard":
                projections.append({
                    "Player": attr.get("description"),
                    "Stat": stat_type,
                    "Line": attr.get("line_score"),
                    "Start Time": attr.get("start_time"),
                    "Odds Type": attr.get("odds_type")
                })
        
        print(f"[DEBUG PP] Found {len(projections)} matching projections")
        return pd.DataFrame(projections)
    
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Request to PrizePicks failed: {e}")
        return pd.DataFrame(columns=["Player", "Stat", "Line", "Start Time", "Odds Type"])
    except Exception as e:
        print(f"[ERROR] Unexpected error scraping PrizePicks: {e}")
        return pd.DataFrame(columns=["Player", "Stat", "Line", "Start Time", "Odds Type"])

if __name__ == "__main__":
    df = scrape_prizepicks_ufc()
    print("\n--- PrizePicks UFC Projections ---")
    print(df)