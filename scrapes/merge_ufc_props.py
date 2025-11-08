import pandas as pd
from .scrape_mma_dk import scrape_draftkings_ufc_markets
from .scrape_mma_pp import scrape_prizepicks_ufc
from thefuzz import process, fuzz


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
    best_match = process.extractOne(name, choices_list, scorer=fuzz.token_sort_ratio)
    if best_match and best_match[1] >= score_cutoff:
        return best_match[0]
    return None


def merge_ufc_props():
    df_strikes, df_distance_data = scrape_draftkings_ufc_markets()
    df_pp = scrape_prizepicks_ufc()

    print(f"\n[DEBUG] df_strikes shape: {df_strikes.shape}")
    print(f"[DEBUG] df_strikes columns: {df_strikes.columns.tolist()}")
    print(f"[DEBUG] df_distance_data shape: {df_distance_data.shape}")
    print(f"[DEBUG] df_distance_data columns: {df_distance_data.columns.tolist()}")
    print(f"[DEBUG] df_pp shape: {df_pp.shape}")
    print(f"[DEBUG] df_pp columns: {df_pp.columns.tolist()}")

    df_dk = pd.concat([df_strikes, df_distance_data], ignore_index=True)
    print(f"[DEBUG] df_dk shape after concat: {df_dk.shape}")
    print(f"[DEBUG] df_dk columns: {df_dk.columns.tolist()}")

    df_dk["fighter_clean"] = df_dk.get("fighter", df_dk.get("fight", "")).apply(clean_name)
    df_pp["fighter_clean"] = df_pp["Player"].apply(clean_name)

    # Filter DK data for significant strikes (Over)
    df_dk_filtered = df_dk[
        (df_dk["market_type"] == "Significant Strikes O/U") &
        (df_dk["label"].str.lower() == "over")
    ].copy()
    print(f"[DEBUG] df_dk_filtered shape: {df_dk_filtered.shape}")
    print(f"[DEBUG] df_dk_filtered columns: {df_dk_filtered.columns.tolist()}")

    # Filter DK data for Going Distance (Yes)
    df_distance_yes = df_dk[
        (df_dk["market_type"] == "Fight to Go the Distance") &
        (df_dk["label"].str.lower() == "yes")
    ].copy().reset_index(drop=True)
    print(f"[DEBUG] df_distance_yes shape before drop_duplicates: {df_distance_yes.shape}")

    # Drop duplicate fights dynamically
    df_distance_yes = df_distance_yes.drop_duplicates(subset=["fight"]).reset_index(drop=True)
    print(f"[DEBUG] df_distance_yes shape after drop_duplicates: {df_distance_yes.shape}")

    # Fuzzy match DK fighters to PP fighters
    dk_name_choices = df_dk_filtered['fighter_clean'].unique()
    print(f"[DEBUG] Number of unique DK fighters: {len(dk_name_choices)}")
    
    df_pp['dk_match_name'] = df_pp['fighter_clean'].apply(
        find_best_match, args=(dk_name_choices,)
    )
    print(f"[DEBUG] df_pp shape after adding dk_match_name: {df_pp.shape}")
    print(f"[DEBUG] df_pp columns before merge: {df_pp.columns.tolist()}")

    merged = pd.merge(
        df_pp,
        df_dk_filtered,
        left_on="dk_match_name",
        right_on="fighter_clean",
        how="left"
    )
    print(f"[DEBUG] merged shape after initial merge: {merged.shape}")
    print(f"[DEBUG] merged columns after merge: {merged.columns.tolist()}")

    merged = merged.rename(columns={"Player": "fighter"})
    print(f"[DEBUG] merged shape after rename: {merged.shape}")
    print(f"[DEBUG] merged columns after rename: {merged.columns.tolist()}")

    # Drop duplicates AFTER renaming and reset index
    merged = merged.drop_duplicates(subset=['fighter_clean_x']).reset_index(drop=True)
    print(f"[DEBUG] merged shape after drop_duplicates: {merged.shape}")

    # Calculated columns
    merged["PP Line"] = merged["Line"].astype(float)
    merged["DK Line"] = merged["line"].astype(float)
    merged["Difference (PP−DK)"] = merged["PP Line"] - merged["DK Line"]
    merged["Best Bet (DK)"] = merged["Difference (PP−DK)"].apply(lambda x: "over" if x > 0 else "under")

    # Map Going Distance dynamically - create as a list to avoid index issues
    print(f"[DEBUG] About to create going_distance_values list for {len(merged)} fighters")
    
    # FIX: Get the first 'fighter' column explicitly
    fighter_series = merged['fighter'].iloc[:, 0] if isinstance(merged['fighter'], pd.DataFrame) else merged['fighter']
    
    going_distance_values = [
        map_distance_odds(f, df_distance_yes, clean_name) 
        for f in fighter_series
    ]
    print(f"[DEBUG] going_distance_values length: {len(going_distance_values)}")
    
    merged["Going Distance"] = going_distance_values

    # Add extra columns
    for col in ["Actual", "Between Lines", "PP Bet Correct", "DK Bet Correct"]:
        merged[col] = ""

    # Final column order
    final_cols = [
        "fighter", "PP Line", "DK Line", "Difference (PP−DK)",
        "Best Bet (DK)", "Going Distance", "Actual", "Between Lines",
        "PP Bet Correct", "DK Bet Correct",
    ]
    final_df = merged[[col for col in final_cols if col in merged.columns]]
    # Filter out rows where DK Line is NaN
    final_df = final_df[final_df["DK Line"].notna()].reset_index(drop=True)


    return final_df


if __name__ == "__main__":
    df_final = merge_ufc_props()
    print("\n--- Fighters missed by fuzzy match (NaN on DK Line) ---")
    print(df_final[df_final['DK Line'].isna()]['fighter'])

    print("\n--- FINAL MERGED TABLE ---")
    print(df_final.to_string())

    df_final.to_csv("merged_ufc_props.csv", index=False)
    print("\nSuccessfully saved to merged_ufc_props.csv")
