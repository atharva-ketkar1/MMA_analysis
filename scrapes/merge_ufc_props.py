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
    """Clean fighter name by removing common suffixes and normalizing."""
    if not isinstance(name, str):
        return ""
    
    cleaned = (
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
    
    # Remove extra spaces
    cleaned = ' '.join(cleaned.split())
    
    return cleaned


def get_name_variants(name):
    """
    Generate common variants of a fighter name for better matching.
    Handles nicknames, middle names, and common variations.
    """
    if not isinstance(name, str):
        return [name]
    
    cleaned = clean_name(name)
    variants = [cleaned]
    
    parts = cleaned.split()
    
    if len(parts) >= 2:
        # First and last name only (removes middle names)
        variants.append(f"{parts[0]} {parts[-1]}")
        
        # Last name only (for very unique last names)
        if len(parts[-1]) > 4:  # Only if last name is reasonably unique
            variants.append(parts[-1])
    
    # Common nickname mappings (can be expanded)
    nickname_map = {
        'daniel': 'dan',
        'dan': 'daniel',
        'anthony': 'tony',
        'tony': 'anthony',
        'william': 'will',
        'will': 'william',
        'william': 'billy',
        'robert': 'rob',
        'rob': 'robert',
        'robert': 'bobby',
        'james': 'jim',
        'jim': 'james',
        'james': 'jimmy',
        'joseph': 'joe',
        'joe': 'joseph',
        'michael': 'mike',
        'mike': 'michael',
        'christopher': 'chris',
        'chris': 'christopher',
        'jonathan': 'jon',
        'jon': 'jonathan',
    }
    
    # Apply nickname transformations
    if len(parts) >= 1:
        first_name = parts[0]
        if first_name in nickname_map:
            nickname = nickname_map[first_name]
            # Create variant with nickname + rest of name
            variant = ' '.join([nickname] + parts[1:])
            variants.append(variant)
            
            # Also add nickname + last name only
            if len(parts) >= 2:
                variants.append(f"{nickname} {parts[-1]}")
    
    # Remove duplicates while preserving order
    seen = set()
    unique_variants = []
    for v in variants:
        if v not in seen:
            seen.add(v)
            unique_variants.append(v)
    
    return unique_variants


def find_best_match(name, choices_list, score_cutoff=70):
    """
    Find best match using multiple fuzzy matching strategies.
    Returns the best match if above cutoff, otherwise None.
    """
    if choices_list is None or len(choices_list) == 0:
        return None
    
    # Generate variants for the input name
    name_variants = get_name_variants(name)
    
    best_match = None
    best_score = 0
    
    # Try each variant with different scoring algorithms
    scorers = [fuzz.token_sort_ratio, fuzz.token_set_ratio, fuzz.ratio]
    
    for variant in name_variants:
        for scorer in scorers:
            try:
                match = process.extractOne(variant, choices_list, scorer=scorer)
                if match and match[1] > best_score:
                    best_score = match[1]
                    best_match = match[0]
            except:
                continue
    
    if best_match and best_score >= score_cutoff:
        return best_match
    
    return None


def map_distance_odds(fighter_name, distance_df, name_cleaner):
    """Map distance odds using token overlap scoring."""
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
    print(f"[DEBUG] DK fighters: {list(dk_name_choices)}")
    
    # Add debug output for matching
    df_pp['dk_match_name'] = None
    match_scores = []
    
    for idx, row in df_pp.iterrows():
        pp_clean = row['fighter_clean']
        match = find_best_match(pp_clean, dk_name_choices)
        df_pp.at[idx, 'dk_match_name'] = match
        
        # Debug output for each match attempt
        if match:
            # Get the best score for reporting
            best_score = 0
            for variant in get_name_variants(pp_clean):
                for scorer in [fuzz.token_sort_ratio, fuzz.token_set_ratio, fuzz.ratio]:
                    try:
                        score = scorer(variant, match)
                        best_score = max(best_score, score)
                    except:
                        pass
            
            match_scores.append(best_score)
            print(f"[DEBUG] ✓ Matched '{row['Player']}' ({pp_clean}) -> '{match}' (score: {best_score})")
        else:
            match_scores.append(0)
            print(f"[DEBUG] ✗ NO MATCH for '{row['Player']}' ({pp_clean})")
            print(f"         Tried variants: {get_name_variants(pp_clean)}")
    
    print(f"\n[DEBUG] df_pp shape after adding dk_match_name: {df_pp.shape}")
    print(f"[DEBUG] Successful matches: {df_pp['dk_match_name'].notna().sum()}/{len(df_pp)}")
    if match_scores:
        avg_score = sum(s for s in match_scores if s > 0) / max(1, sum(1 for s in match_scores if s > 0))
        print(f"[DEBUG] Average match score: {avg_score:.1f}")

    merged = pd.merge(
        df_pp,
        df_dk_filtered,
        left_on="dk_match_name",
        right_on="fighter_clean",
        how="left"
    )
    print(f"\n[DEBUG] merged shape after initial merge: {merged.shape}")
    print(f"[DEBUG] merged columns after merge: {merged.columns.tolist()}")

    # Drop the 'fighter' column from DK before renaming to avoid duplicates
    if 'fighter' in merged.columns:
        merged = merged.drop(columns=['fighter'])
    
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
    print(f"\n[DEBUG] Rows before filtering NaN DK Lines: {len(final_df)}")
    print(f"[DEBUG] Rows with NaN DK Lines: {final_df['DK Line'].isna().sum()}")
    
    # Show which fighters are being filtered out
    missing_dk_mask = final_df['DK Line'].isna()
    if missing_dk_mask.any():
        # Handle potential DataFrame column issue
        fighter_col = final_df.loc[missing_dk_mask, 'fighter']
        if isinstance(fighter_col, pd.DataFrame):
            missing_dk = fighter_col.iloc[:, 0].tolist()
        else:
            missing_dk = fighter_col.tolist()
        print(f"[DEBUG] Fighters without DK lines (will be filtered): {missing_dk}")
    
    final_df = final_df[final_df["DK Line"].notna()].reset_index(drop=True)
    print(f"[DEBUG] Rows after filtering NaN DK Lines: {len(final_df)}")
    
    final_df = final_df.loc[:, ~final_df.columns.duplicated(keep='last')]

    return final_df


if __name__ == "__main__":
    try:
        df_final = merge_ufc_props()
        
        if df_final.empty:
            print("\n[WARN] No data to process. Exiting.")
            exit(1)
        
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