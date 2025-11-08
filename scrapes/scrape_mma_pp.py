import requests
import pandas as pd

# Change this to the stat you want
DESIRED_STATS = ["Significant Strikes"]

def scrape_prizepicks_ufc():
    url = "https://api.prizepicks.com/projections"
    params = {"league_id": "12", "per_page": "250"}  
    headers = {"User-Agent": "Mozilla/5.0"}

    response = requests.get(url, params=params, headers=headers)
    data = response.json()

    # Each projection is in data["data"]
    projections = []
    for item in data["data"]:
        attr = item["attributes"]
        stat_type = attr.get("stat_type")

        if stat_type in DESIRED_STATS and attr.get("odds_type") == "standard":
            projections.append({
                "Player": attr.get("description"),
                "Stat": stat_type,
                "Line": attr.get("line_score"),
                "Start Time": attr.get("start_time"),
                "Odds Type": attr.get("odds_type")
            })

    return pd.DataFrame(projections)

if __name__ == "__main__":
    df = scrape_prizepicks_ufc()
    print(df)
