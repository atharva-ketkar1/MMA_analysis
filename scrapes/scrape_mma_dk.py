import requests
import pandas as pd


def get_dk_data(subcategory_id):
    """Fetch DraftKings UFC subcategory data dynamically."""
    url = (
        f"https://sportsbook-nash.draftkings.com/sites/US-OH-SB/api/sportscontent/controldata/"
        f"league/leagueSubcategory/v1/markets"
        f"?isBatchable=false&templateVars=9034%2C{subcategory_id}"
        f"&eventsQuery=%24filter%3DleagueId%20eq%20%279034%27%20AND%20clientMetadata%2FSubcategories%2Fany%28s%3A%20s%2FId%20eq%20%27{subcategory_id}%27%29"
        f"&marketsQuery=%24filter%3DclientMetadata%2FsubCategoryId%20eq%20%27{subcategory_id}%27%20AND%20tags%2Fall%28t%3A%20t%20ne%20%27SportcastBetBuilder%27%29"
        f"&include=Events&entity=events"
    )
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()


def scrape_draftkings_ufc_markets():
    """Scrape Significant Strikes O/U and Fight to Go Distance dynamically."""
    strikes_data = get_dk_data(16618)  # Significant Strikes O/U
    distance_data = get_dk_data(17644)  # Fight to Go Distance

    # --- Parse Significant Strikes ---
    def parse_strikes(data):
        markets = data.get("markets", [])
        selections = data.get("selections", [])
        strikes_ou = {m["id"]: m for m in markets if "Total Significant Strikes O/U" in m["name"]}
        rows = []
        for sel in selections:
            mid = sel.get("marketId")
            if mid not in strikes_ou:
                continue
            participants = sel.get("participants", [])
            
            # --- MODIFIED LOGIC START ---
            if participants:
                # Prefer the name from the participant list if available
                fighter = participants[0]["name"]
            else:
                # Fallback: Clean the market name to get the fighter name
                market_name = strikes_ou[mid]["name"]
                # Example: "Jack Hermansson Total Significant Strikes O/U" -> "Jack Hermansson"
                fighter = market_name.replace(" Total Significant Strikes O/U", "").strip()
            # --- MODIFIED LOGIC END ---
            
            label_str = sel.get("label", "")
            parts = label_str.split(' ')
            label_val = parts[0] if len(parts) > 0 else label_str
            line_val = float(parts[1]) if len(parts) > 1 else None
            rows.append({
                "fighter": fighter,
                "market_type": "Significant Strikes O/U",
                "label": label_val,
                "line": line_val,
                "odds": sel.get("displayOdds", {}).get("american"),
            })
        return pd.DataFrame(rows)

    # --- Parse Fight to Go Distance dynamically ---
    def parse_distance(data):
        events = data.get("events", [])
        markets = data.get("markets", [])
        selections = data.get("selections", [])

        # Map eventId -> fight name
        event_map = {e["id"]: e.get("name", "Unknown Fight") for e in events}

        # Map marketId -> eventId
        distance_markets = {}
        for m in markets:
            if "Fight to Go the Distance" in m.get("name", ""):
                distance_markets[m["id"]] = {"eventId": m.get("eventId"), "name": m.get("name")}

        rows = []
        for sel in selections:
            mid = sel.get("marketId")
            if mid not in distance_markets:
                continue
            market_info = distance_markets[mid]
            event_id = market_info.get("eventId")
            fight_name = event_map.get(event_id, "Unknown Fight")

            # Fallback: extract fighter names from market name dynamically
            if fight_name == "Unknown Fight":
                fight_name = market_info.get("name", "").replace("Fight to Go the Distance - ", "").strip()

            rows.append({
                "fight": fight_name,
                "market_type": "Fight to Go the Distance",
                "label": sel.get("label"),  # Yes / No
                "odds": sel.get("displayOdds", {}).get("american"),
            })

        return pd.DataFrame(rows)

    df_strikes = parse_strikes(strikes_data)
    df_distance = parse_distance(distance_data)

    return df_strikes, df_distance


if __name__ == "__main__":
    df_strikes, df_distance = scrape_draftkings_ufc_markets()
    print("\n--- Significant Strikes ---")
    print(df_strikes.to_string())
    print("\n--- Fight to Go the Distance ---")
    print(df_distance.to_string())