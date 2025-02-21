#pylint:disable=E1137
import requests
import json
import pandas as pd
from datetime import datetime

# -------------------------------------- 1 (Indent: 0 tabs) -------------------------------------- #
API_KEY = "YOUR_API_KEY_HERE"         # Replace with your API key.
FACTION_ID = "YOUR_FACTION_ID_HERE"     # Replace with your faction ID.
OPPOSING_FACTION = "The S.C.P foundation"  # This will be updated dynamically

CHAIN_CSV = "chains.csv"
ATTACKS_CSV = "attacks.csv"
FINAL_SUMMARY_CSV = "attack_summary.csv"
ERROR_LOG_CSV = "errors.csv"

START_TIME = 1738969204  # Will be updated dynamically
END_TIME = 1739210884    # Will be updated dynamically

# Spacing mapping for chaining hits (base points for hits after warmup)
spacing_mapping = {
    "<1 minute": 1,
    "1-2 minutes": 2,
    "2-3 minutes": 3,
    "3-4 minutes": 5,
    "4-5 minutes": 8
}

# -------------------------------------- 2 (Indent: 0 tabs) -------------------------------------- #
def fetch_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"API Error: {response.status_code}")
        return None

def categorize_time_difference(seconds):
    if seconds < 60:
        return "<1 minute"
    elif seconds < 120:
        return "1-2 minutes"
    elif seconds < 180:
        return "2-3 minutes"
    elif seconds < 240:
        return "3-4 minutes"
    elif seconds < 300:
        return "4-5 minutes"
    else:
        return "ERROR: Time out of range"

# -------------------------------------- 2.1 (Indent: 0 tabs) -------------------------------------- #
def update_war_info():
    """
    Calls the rankedwars endpoint to get the last ranked war,
    extracts the war start/end times and opposing faction, and updates
    the global START_TIME, END_TIME, and OPPOSING_FACTION.
    """
    global OPPOSING_FACTION, START_TIME, END_TIME
    rankedwars_url = f"https://api.torn.com/faction/{FACTION_ID}?key={API_KEY}&selections=rankedwars"
    rankedwars_data = fetch_data(rankedwars_url)
    if not rankedwars_data or "rankedwars" not in rankedwars_data:
        print("No ranked wars data found.")
        return
    
    ranked_wars = rankedwars_data["rankedwars"]
    if not ranked_wars:
        print("No ranked wars available.")
        return
    
    # Find the ranked war with the most recent end time.
    latest_war_id, latest_war = max(ranked_wars.items(), key=lambda item: int(item[1]["war"]["end"]))
    war_info = latest_war["war"]
    new_start = int(war_info["start"])
    new_end = int(war_info["end"])
    START_TIME = new_start
    END_TIME = new_end
    
    # In the ranked war, "factions" contains our faction and the opponent.
    factions = latest_war["factions"]
    for faction_id, faction_data in factions.items():
        if str(faction_id) != FACTION_ID:
            OPPOSING_FACTION = faction_data["name"]
            break
    
    print(f"Updated war info: START_TIME = {START_TIME}, END_TIME = {END_TIME}, OPPOSING_FACTION = {OPPOSING_FACTION}")

# -------------------------------------- 3 (Indent: 0 tabs) -------------------------------------- #
def process_torn_attacks(start_time, end_time):
    chain_url = f"https://api.torn.com/faction/{FACTION_ID}?key={API_KEY}&from={start_time}&to={end_time}&comment=TornAPI&selections=chains"
    chain_data = fetch_data(chain_url)

    if not chain_data or "chains" not in chain_data:
        print("No chain data found.")
        return None

    chain_records = [{"Chain ID": k, "Start": int(v["start"]), "End": int(v["end"])} 
                      for k, v in chain_data["chains"].items()]
    df_chains = pd.DataFrame(chain_records)
    df_chains.to_csv(CHAIN_CSV, index=False)
    print(f"✅ Saved chain data to {CHAIN_CSV}")
    
    attack_data = []
    for _, row in df_chains.iterrows():
        chain_start = row["Start"]
        chain_end = row["End"] + 60
        current_chain_id = row["Chain ID"]
        attack_url = f"https://api.torn.com/faction/{FACTION_ID}?key={API_KEY}&from={chain_start}&to={chain_end}&comment=TornAPI&selections=attacks"
        attack_response = fetch_data(attack_url)

        if attack_response and "attacks" in attack_response:
            for attack in attack_response["attacks"].values():
                # Add the chain id so we can group attacks by chain later.
                attack["chain_id"] = current_chain_id
                attack_data.append(attack)
            
    if not attack_data:
        print("No attack data found.")
        return None

    attack_records = []
    for attack in attack_data:
        if attack["chain"] > 0 and attack["defender_factionname"] != "Untitled":
            attack_records.append({
                "Chain ID": attack["chain_id"],
                "Attacker Name": attack["attacker_name"],
                "Defender Faction": attack["defender_factionname"],
                "Time Ended (UTC)": datetime.utcfromtimestamp(int(attack["timestamp_ended"])).strftime('%H:%M:%S'),
                "Chain Value": attack["chain"],
                "Timestamp Ended": int(attack["timestamp_ended"]),
                "Time Since Last Attack": None  # to be computed below
            })

    df_attacks = pd.DataFrame(attack_records)
    error_logs = []  # Initialize error logs

    if df_attacks.empty:
        print("No valid attacks found.")
        df_attacks = pd.DataFrame(columns=["Chain ID", "Attacker Name", "Defender Faction",
                                             "Time Ended (UTC)", "Chain Value", "Timestamp Ended",
                                             "Time Since Last Attack"])
    else:
        # Sort by Chain ID then by Timestamp Ended
        df_attacks = df_attacks.sort_values(by=["Chain ID", "Timestamp Ended"]).reset_index(drop=True)
        df_attacks["Time Since Last Attack"] = None

        for i in range(1, len(df_attacks)):
            # Only compare consecutive hits if they belong to the same chain.
            if df_attacks.loc[i, "Chain ID"] == df_attacks.loc[i - 1, "Chain ID"]:
                time_diff = df_attacks.loc[i, "Timestamp Ended"] - df_attacks.loc[i - 1, "Timestamp Ended"]
                category = categorize_time_difference(time_diff)
                if category == "ERROR: Time out of range":
                    error_logs.append(f"⚠️ Error: Attack at {df_attacks.loc[i, 'Time Ended (UTC)']} categorized incorrectly!")
                df_attacks.at[i, "Time Since Last Attack"] = category

    df_attacks.to_csv(ATTACKS_CSV, index=False)
    print(f"✅ Saved attack data to {ATTACKS_CSV}")
    
    # -------------------------------------- 7 (Indent: 1 tab) -------------------------------------- #
    df_filtered = df_attacks[df_attacks["Chain Value"] > 10].copy()
    df_filtered = df_filtered[df_filtered["Time Since Last Attack"].notnull()]

    def count_attacks(df, time_category):
        total_counts = df[df["Time Since Last Attack"] == time_category].groupby("Attacker Name").size()
        scp_counts = df[(df["Time Since Last Attack"] == time_category) &
                        (df["Defender Faction"] == OPPOSING_FACTION)].groupby("Attacker Name").size()
        return total_counts, scp_counts

    categories = [cat for cat in df_filtered["Time Since Last Attack"].unique() if cat is not None]
    category_counts = {}

    for category in categories:
        total, scp = count_attacks(df_filtered, category)
        merged = total.to_frame(name=category).join(scp.rename(f"{category}_scp"), how="outer").fillna(0)
        merged[f"{category}_formatted"] = merged.apply(lambda row: f"{int(row[category])}.{int(row[f'{category}_scp']):03}", axis=1)
        category_counts[category] = merged[f"{category}_formatted"]

    total_1_10 = df_attacks[df_attacks["Chain Value"] <= 10].groupby("Attacker Name").size()
    scp_1_10 = df_attacks[(df_attacks["Chain Value"] <= 10) &
                          (df_attacks["Defender Faction"] == OPPOSING_FACTION)].groupby("Attacker Name").size()

    merged_1_10 = total_1_10.to_frame(name="Attacks #1-10").join(
        scp_1_10.rename("Attacks #1-10_scp"), how="outer"
    ).fillna(0)

    merged_1_10["Attacks #1-10_formatted"] = merged_1_10.apply(
        lambda row: f"{int(row['Attacks #1-10'])}.{int(row['Attacks #1-10_scp']):03}", axis=1
    )
    all_possible_categories = ["<1 minute", "1-2 minutes", "2-3 minutes", "3-4 minutes", "4-5 minutes"]
    for cat in all_possible_categories:
        if cat not in category_counts:
            category_counts[cat] = pd.Series(dtype="float")
    final_summary = pd.DataFrame({key: pd.Series(value) for key, value in category_counts.items()}).fillna("0.000")
    merged_1_10 = merged_1_10.reindex(final_summary.index, fill_value="0.000")
    final_summary["Attacks #1-10"] = merged_1_10["Attacks #1-10_formatted"]

    # -------------------------------------- New Block 8 (Indent: 1 tab) -------------------------------------- #
    # Calculate per-attacker chain points.
    # For each chain (grouped by Attacker Name and Chain ID):
    #  • For each hit:
    #      - If hit number ≤ 10, base = 2; otherwise, base = spacing_mapping value (using "Time Since Last Attack").
    #      - Every hit gets an enemy bonus of +0.5 if on the enemy.
    #      - Additionally, if the overall hit number is 10, 25, or 50 and the hit is NOT on the enemy,
    #        record a -10 penalty (separately).
    #  • Then add a chain bonus: +2 per full 10 minutes of chain duration plus +5 for chain participation.
    # We accumulate per-attacker:
    #  - Base Points (from all hits including enemy bonus),
    #  - Penalty Points (sum of -10 penalties),
    #  - Chain Bonus,
    #  - and Total Chain Points = Base Points + Penalty Points + Chain Bonus.
    attacker_base_points = {}
    attacker_penalty_points = {}
    attacker_chain_bonus = {}
    attacker_chain_points = {}

    # Group by Attacker and Chain ID.
    for (attacker, chain_id), group in df_attacks.groupby(["Attacker Name", "Chain ID"]):
        group = group.sort_values("Timestamp Ended")
        chain_base = 0.0
        chain_penalty = 0.0
        for i, (_, hit) in enumerate(group.iterrows()):
            overall_hit = i + 1  # hit number within this chain
            if overall_hit <= 10:
                base = 2
            else:
                spacing_cat = hit["Time Since Last Attack"]
                base = spacing_mapping.get(spacing_cat, 0)
            # Every hit gets enemy bonus if on enemy.
            enemy_bonus = 0.5 if hit["Defender Faction"] == OPPOSING_FACTION else 0.0
            # For bonus hits (10th, 25th, 50th), if hit is NOT on enemy, record a -10 penalty.
            bonus_pen = -10 if (overall_hit in [10, 25, 50] and hit["Defender Faction"] != OPPOSING_FACTION) else 0
            chain_base += (base + enemy_bonus)
            chain_penalty += bonus_pen
        # Chain bonus: +2 points for every full 10 minutes in the chain plus +5 for participation.
        duration = group["Timestamp Ended"].max() - group["Timestamp Ended"].min()
        bonus_time = 2 * (duration // 600)
        participation_bonus = 5
        chain_bonus = bonus_time + participation_bonus
        chain_total = chain_base + chain_penalty + chain_bonus
        # Aggregate per attacker.
        attacker_base_points[attacker] = attacker_base_points.get(attacker, 0) + chain_base
        attacker_penalty_points[attacker] = attacker_penalty_points.get(attacker, 0) + chain_penalty
        attacker_chain_bonus[attacker] = attacker_chain_bonus.get(attacker, 0) + chain_bonus
        attacker_chain_points[attacker] = attacker_chain_points.get(attacker, 0) + chain_total

    # Total hits per attacker.
    total_hits = df_attacks.groupby("Attacker Name").size()
    points_df = pd.DataFrame({
        "Total Hits": total_hits,
        "Base Points": pd.Series(attacker_base_points),
        "Penalty Points": pd.Series(attacker_penalty_points),
        "Chain Bonus": pd.Series(attacker_chain_bonus),
        "Chain Points": pd.Series(attacker_chain_points)
    }).fillna(0)
    # Total Points here equal the sum of base, penalty, and bonus.
    points_df["Total Points"] = points_df["Chain Points"]

    # Merge the points info into final_summary.
    final_summary = final_summary.merge(points_df, left_index=True, right_index=True, how="left")
    
    # -------------------------------------- 9 (Indent: 1 tab) -------------------------------------- #
    # Reassign the Attacks #1-10 column if needed.
    final_summary["Attacks #1-10"] = merged_1_10["Attacks #1-10_formatted"]

    if error_logs:
        error_df = pd.DataFrame({"Errors": [" | ".join(error_logs)]})
        error_df.to_csv(ERROR_LOG_CSV, index=False)
        print(f"⚠️ Errors detected! Check '{ERROR_LOG_CSV}' for details.")

    final_summary.to_csv(FINAL_SUMMARY_CSV, index=True)
    print(f"✅ Saved final summary to {FINAL_SUMMARY_CSV}")

# -------------------------------------- 10 (Indent: 0 tabs) -------------------------------------- #
if __name__ == "__main__":
    update_war_info()  # Update START_TIME, END_TIME, and OPPOSING_FACTION dynamically
    process_torn_attacks(START_TIME, END_TIME)