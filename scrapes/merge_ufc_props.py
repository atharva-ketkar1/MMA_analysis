import pandas as pd
import re
from datetime import date, timedelta
from .scrape_mma_dk import scrape_draftkings_ufc_markets
from .scrape_mma_pp import scrape_prizepicks_ufc
from thefuzz import process, fuzz


def slugify(text):
    """Basic function to clean text for a filename."""
    text = str(text).strip()
    text = text.replace(" vs. ", " vs ")
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[\s-]+', '_', text)
    if not text:
        return "unknown_event"
    return text


def clean_name(name):
    if not isinstance(name, str):
        return ""
    return (
        name.lower()
        .replace("jr.", "")
        .replace("jr", "")
        .replace("iii", "")
        .replace("ii", "")
        .replace("the", "")
        .replace("-", " ")
        .replace(".", "")
        .strip()
    )


def map_distance_odds(fighter_name, distance_df, name_cleaner):
    fighter_parts = set(name_cleaner(fighter_name).split())
    best_score = 0
    found_odds = None
    for _, row in distance_df.iterrows():
        fight_parts = set(name_cleaner(row['fight']).split())
        match_score = len(fighter_parts.intersection(fight_parts))
        if match_score > best_score:
            best_score = match_score
            found_odds = row.get('odds')
    return found_odds


def find_best_match(name, choices_list, score_cutoff=85):
    if choices_list is None or len(choices_list) == 0:
        return None
    best_match = process.extractOne(name, choices_list, scorer=fuzz.token_set_ratio)
    if best_match and best_match[1] >= score_cutoff:
        return best_match[0]
    return None


def merge_ufc_props():
    # Scrape data with error handling
    try:
        df_strikes, df_distance_data = scrape_draftkings_ufc_markets()
        print(f"\n[DEBUG] df_strikes shape: {df_strikes.shape}")
        print(f"[DEBUG] df_strikes columns: {df_strikes.columns.tolist()}")
        print(f"[DEBUG] df_distance_data shape: {df_distance_data.shape}")
        print(f"[DEBUG] df_distance_data columns: {df_distance_data.columns.tolist()}")
    except Exception as e:
        print(f"[ERROR] Failed to scrape DraftKings data: {e}")
        raise
    
    try:
        df_pp = scrape_prizepicks_ufc()
        print(f"\n[DEBUG] df_pp shape: {df_pp.shape}")
        print(f"[DEBUG] df_pp columns: {df_pp.columns.tolist()}")
        
        # Check if PrizePicks returned empty data
        if df_pp.empty:
            print("[WARN] PrizePicks returned no data. Continuing with DraftKings data only.")
            # Return a minimal DataFrame with DK data
            return pd.DataFrame(columns=[
                "fighter", "PP Line", "DK Line", "Difference (PP−DK)",
                "Best Bet (DK)", "Going Distance", "Actual", "Between Lines",
                "PP Bet Correct", "DK Bet Correct"
            ])
    except Exception as e:
        print(f"[ERROR] Failed to scrape PrizePicks data: {e}")
        print("[WARN] Continuing without PrizePicks data...")
        df_pp = pd.DataFrame(columns=["Player", "Stat", "Line", "Start Time", "Odds Type"])

    df_dk = pd.concat([df_strikes, df_distance_data], ignore_index=True)
    print(f"\n[DEBUG] df_dk shape after concat: {df_dk.shape}")
    print(f"[DEBUG] df_dk columns: {df_dk.columns.tolist()}")

    df_dk["fighter_clean"] = df_dk.get("fighter", df_dk.get("fight", "")).apply(clean_name)
    df_pp["fighter_clean"] = df_pp["Player"].apply(clean_name) if not df_pp.empty else pd.Series(dtype=str)

    # Filter DK data for significant strikes (Over)
    df_dk_filtered = df_dk[
        (df_dk["market_type"] == "Significant Strikes O/U") &
        (df_dk["label"].str.lower() == "over")
    ].copy()
    print(f"\n[DEBUG] df_dk_filtered shape: {df_dk_filtered.shape}")
    print(f"[DEBUG] df_dk_filtered columns: {df_dk_filtered.columns.tolist()}")

    # Store the original DK fighter order
    ordered_dk_fighters = df_dk_filtered['fighter_clean'].drop_duplicates().tolist()

    # Filter DK data for Going Distance (Yes)
    df_distance_yes = df_dk[
        (df_dk["market_type"] == "Fight to Go the Distance") &
        (df_dk["label"].str.lower() == "yes")
    ].copy().reset_index(drop=True)
    print(f"\n[DEBUG] df_distance_yes shape before drop_duplicates: {df_distance_yes.shape}")

    df_distance_yes = df_distance_yes.drop_duplicates(subset=["fight"]).reset_index(drop=True)
    print(f"\n[DEBUG] df_distance_yes shape after drop_duplicates: {df_distance_yes.shape}")

    # Handle empty PrizePicks data
    if df_pp.empty:
        print("\n[WARN] No PrizePicks data available. Cannot create merged dataset.")
        return pd.DataFrame(columns=[
            "fighter", "PP Line", "DK Line", "Difference (PP−DK)",
            "Best Bet (DK)", "Going Distance", "Actual", "Between Lines",
            "PP Bet Correct", "DK Bet Correct"
        ])

    # Fuzzy match DK fighters to PP fighters
    dk_name_choices = df_dk_filtered['fighter_clean'].unique()
    print(f"\n[DEBUG] Number of unique DK fighters: {len(dk_name_choices)}")
    
    df_pp['dk_match_name'] = df_pp['fighter_clean'].apply(
        find_best_match, args=(dk_name_choices,)
    )
    print(f"\n[DEBUG] df_pp shape after adding dk_match_name: {df_pp.shape}")
    print(f"[DEBUG] df_pp columns before merge: {df_pp.columns.tolist()}")

    merged = pd.merge(
        df_pp,
        df_dk_filtered,
        left_on="dk_match_name",
        right_on="fighter_clean",
        how="left"
    )
    print(f"\n[DEBUG] merged shape after initial merge: {merged.shape}")
    print(f"[DEBUG] merged columns after merge: {merged.columns.tolist()}")

    merged = merged.rename(columns={"Player": "fighter"})
    print(f"\n[DEBUG] merged shape after rename: {merged.shape}")
    print(f"[DEBUG] merged columns after rename: {merged.columns.tolist()}")

    # Drop duplicates AFTER renaming and reset index
    merged = merged.drop_duplicates(subset=['fighter_clean_x']).reset_index(drop=True)
    print(f"\n[DEBUG] merged shape after drop_duplicates: {merged.shape}")

    # Calculated columns
    merged["PP Line"] = pd.to_numeric(merged["Line"], errors='coerce')
    merged["DK Line"] = pd.to_numeric(merged["line"], errors='coerce')
    merged["Difference (PP−DK)"] = merged["PP Line"] - merged["DK Line"]
    merged["Best Bet (DK)"] = merged["Difference (PP−DK)"].apply(
        lambda x: "over" if pd.notna(x) and x > 0 else "under" if pd.notna(x) else ""
    )

    # Map Going Distance dynamically
    print(f"\n[DEBUG] About to create going_distance_values list for {len(merged)} fighters")
    
    # Get the fighter column safely
    fighter_series = merged['fighter'].iloc[:, 0] if isinstance(merged['fighter'], pd.DataFrame) else merged['fighter']
    
    going_distance_values = [
        map_distance_odds(f, df_distance_yes, clean_name) 
        for f in fighter_series
    ]
    print(f"\n[DEBUG] going_distance_values length: {len(going_distance_values)}")
    
    merged["Going Distance"] = going_distance_values

    # Add extra columns
    for col in ["Actual", "Between Lines", "PP Bet Correct", "DK Bet Correct"]:
        merged[col] = ""

    # Re-sort the DataFrame based on the original DK fighter order
    try:
        merged['fighter_clean_y'] = pd.Categorical(
            merged['fighter_clean_y'],
            categories=ordered_dk_fighters,
            ordered=True
        )
        merged = merged.sort_values('fighter_clean_y').reset_index(drop=True)
        print("\n[DEBUG] Re-sorted DataFrame based on DK fighter order.")
    except KeyError:
        print("\n[DEBUG] Could not find 'fighter_clean_y' for sorting. Skipping re-sort.")
    except Exception as e:
        print(f"\n[DEBUG] Error during re-sorting: {e}. Skipping re-sort.")

    # Final column order
    final_cols = [
        "fighter", "PP Line", "DK Line", "Difference (PP−DK)",
        "Best Bet (DK)", "Going Distance", "Actual", "Between Lines",
        "PP Bet Correct", "DK Bet Correct",
    ]
    final_df = merged[[col for col in final_cols if col in merged.columns]]
    
    # Filter out rows where DK Line is NaN
    final_df = final_df[final_df["DK Line"].notna()].reset_index(drop=True)
    final_df = final_df.loc[:, ~final_df.columns.duplicated(keep='last')]

    return final_df


if __name__ == "__main__":
    try:
        df_final = merge_ufc_props()
        
        if df_final.empty:
            print("\n[WARN] No data to process. Exiting.")
            exit(1)
        
        print("\n--- Fighters missed by fuzzy match (NaN on DK Line) ---")
        print(df_final[df_final['DK Line'].isna()]['fighter'])

        print("\n--- FINAL MERGED TABLE ---")
        print(df_final.to_string())

        main_event_name = "unknown_event"
        try:
            if len(df_final) >= 2:
                last_fighter_full = df_final['fighter'].iloc[-1]
                second_last_fighter_full = df_final['fighter'].iloc[-2]
                
                last_name_1 = last_fighter_full.split(' ')[-1].capitalize()
                last_name_2 = second_last_fighter_full.split(' ')[-1].capitalize()

                main_event_name = f"{last_name_1} vs {last_name_2}"
                
            elif len(df_final) == 1:
                last_name_1 = df_final['fighter'].iloc[0].split(' ')[-1].capitalize()
                main_event_name = f"{last_name_1}_fight"
                
        except Exception as e:
            print(f"[WARN] Could not determine main event name. Defaulting to 'unknown_event'. Error: {e}")
            main_event_name = "unknown_event"

        # Get the date
        today = date.today()
        days_until_saturday = (5 - today.weekday() + 7) % 7
        saturday_date = today + timedelta(days=days_until_saturday)
        date_str = saturday_date.strftime("%m-%d-%Y")

        safe_event_name = slugify(main_event_name)
        output_filename = f"UFC_Fight_Lines/{date_str}_{safe_event_name}.csv"
        
        df_final.to_csv(output_filename, index=False)
        print(f"\nSuccessfully saved to {output_filename}")
        
    except Exception as e:
        print(f"\n[ERROR] Script failed: {e}")
        import traceback
        traceback.print_exc()
        exit(1)