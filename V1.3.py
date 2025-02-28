#pylint:disable=E1137
import requests
import json
import pandas as pd
from datetime import datetime
import logging

# -------------------------------------- Logging Configuration -------------------------------------- #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("torn_analysis.log"),
        logging.StreamHandler()
    ]
)

# -------------------------------------- Global Variables -------------------------------------- #
API_KEY = "5F6V3NWgfkhZb1cw"         # Replace with your API key.
FACTION_ID = "53228"                 # Replace with your faction ID.
OPPOSING_FACTION = "The S.C.P foundation"  # This will be updated dynamically

CHAIN_CSV = "chains.csv"
ATTACKS_CSV = "attacks.csv"
FINAL_SUMMARY_CSV = "attack_summary.csv"
ERROR_LOG_CSV = "errors.csv"
PENALTY_HITS_CSV = "penalty_hits.csv"

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

# -------------------------------------- Helper Functions -------------------------------------- #
def fetch_data(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            logging.info(f"Successfully fetched data from {url}")
            return response.json()
        else:
            logging.error(f"API Error {response.status_code} for URL: {url}")
            return None
    except Exception as e:
        logging.exception(f"Exception during API call to {url}: {e}")
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

# -------------------------------------- Update War Info -------------------------------------- #
def update_war_info():
    """
    Retrieves the most recent war information based on start time,
    extracts the war start/end times, and selects the opposing faction.
    Only attacks from this war (the most recent) will be considered.
    """
    global OPPOSING_FACTION, START_TIME, END_TIME
    rankedwars_url = f"https://api.torn.com/faction/{FACTION_ID}?key={API_KEY}&selections=rankedwars"
    rankedwars_data = fetch_data(rankedwars_url)
    if not rankedwars_data or "rankedwars" not in rankedwars_data:
        logging.error("No ranked wars data found.")
        return
    
    ranked_wars = rankedwars_data["rankedwars"]
    if not ranked_wars:
        logging.error("No ranked wars available.")
        return
    
    try:
        # Select the war with the most recent start time.
        latest_war_id, latest_war = max(ranked_wars.items(), key=lambda item: int(item[1]["war"]["start"]))
        war_info = latest_war["war"]
        new_start = int(war_info["start"])
        new_end = int(war_info["end"])
        START_TIME = new_start
        END_TIME = new_end

        # Choose the opposing faction (the one that is not our FACTION_ID).
        factions = latest_war["factions"]
        for faction_id, faction_data in factions.items():
            if str(faction_id) != FACTION_ID:
                OPPOSING_FACTION = faction_data["name"]
                break

        logging.info(f"Updated war info: START_TIME = {START_TIME}, END_TIME = {END_TIME}, OPPOSING_FACTION = {OPPOSING_FACTION}")
    except Exception as e:
        logging.exception(f"Error updating war info: {e}")

# -------------------------------------- Process Attacks -------------------------------------- #
def process_torn_attacks(start_time, end_time):
    """
    Retrieves chain and attack data for the selected war,
    processes only attacks that are part of chains, and computes per-attacker chain points.
    Negative points are applied only to bonus hits—hits where the "Chain Value" is equal
    to 10, 25, or 50—when the hit is not against the opposing faction.
    Additionally, a "Cut" column is added along with special rows for FACTION CUT and TOTALS.
    """
    try:
        # Get chain data.
        chain_url = f"https://api.torn.com/faction/{FACTION_ID}?key={API_KEY}&from={start_time}&to={end_time}&comment=TornAPI&selections=chains"
        chain_data = fetch_data(chain_url)
        if not chain_data or "chains" not in chain_data:
            logging.error("No chain data found.")
            return None

        chain_records = [{"Chain ID": k, "Start": int(v["start"]), "End": int(v["end"])} 
                         for k, v in chain_data["chains"].items()]
        df_chains = pd.DataFrame(chain_records)
        df_chains.to_csv(CHAIN_CSV, index=False)
        logging.info(f"Saved chain data to {CHAIN_CSV}")

        # Retrieve attack data for each chain.
        attack_data = []
        for _, row in df_chains.iterrows():
            chain_start = row["Start"]
            chain_end = row["End"] + 60  # 60-second buffer.
            current_chain_id = row["Chain ID"]
            attack_url = f"https://api.torn.com/faction/{FACTION_ID}?key={API_KEY}&from={chain_start}&to={chain_end}&comment=TornAPI&selections=attacks"
            attack_response = fetch_data(attack_url)

            if attack_response and "attacks" in attack_response:
                for attack in attack_response["attacks"].values():
                    if attack["chain"] > 0 and attack["defender_factionname"] != "Untitled":
                        attack["chain_id"] = current_chain_id
                        attack_data.append(attack)

        if not attack_data:
            logging.error("No attack data found.")
            return None

        attack_records = []
        for attack in attack_data:
            attack_records.append({
                "Chain ID": attack["chain_id"],
                "Attacker Name": attack["attacker_name"],
                "Defender Faction": attack["defender_factionname"],
                "Time Ended (UTC)": datetime.utcfromtimestamp(int(attack["timestamp_ended"])).strftime('%H:%M:%S'),
                "Chain Value": attack["chain"],
                "Timestamp Ended": int(attack["timestamp_ended"]),
                "Time Since Last Attack": None  # To be computed.
            })

        df_attacks = pd.DataFrame(attack_records)
        error_logs = []

        if df_attacks.empty:
            logging.error("No valid attacks found.")
            df_attacks = pd.DataFrame(columns=["Chain ID", "Attacker Name", "Defender Faction",
                                                 "Time Ended (UTC)", "Chain Value", "Timestamp Ended",
                                                 "Time Since Last Attack"])
        else:
            df_attacks = df_attacks.sort_values(by=["Chain ID", "Timestamp Ended"]).reset_index(drop=True)
            df_attacks["Time Since Last Attack"] = None

            for i in range(1, len(df_attacks)):
                if df_attacks.loc[i, "Chain ID"] == df_attacks.loc[i - 1, "Chain ID"]:
                    time_diff = df_attacks.loc[i, "Timestamp Ended"] - df_attacks.loc[i - 1, "Timestamp Ended"]
                    category = categorize_time_difference(time_diff)
                    if category == "ERROR: Time out of range":
                        error_logs.append(f"Error: Attack at {df_attacks.loc[i, 'Time Ended (UTC)']} categorized incorrectly!")
                    df_attacks.at[i, "Time Since Last Attack"] = category

        df_attacks.to_csv(ATTACKS_CSV, index=False)
        logging.info(f"Saved attack data to {ATTACKS_CSV}")

        # Process attack summary.
        df_filtered = df_attacks[(df_attacks["Chain Value"] > 10) & (df_attacks["Time Since Last Attack"].notnull())].copy()

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

        # ------------------------------ Calculate Chain Points ------------------------------ #
        attacker_base_points = {}
        attacker_penalty_points = {}
        attacker_chain_bonus = {}
        attacker_chain_points = {}

        valid_chain_ids = set(df_chains["Chain ID"])
        penalty_hits_details = []

        for (attacker, chain_id), group in df_attacks.groupby(["Attacker Name", "Chain ID"]):
            if chain_id not in valid_chain_ids:
                continue
            group = group.sort_values("Timestamp Ended")
            chain_base = 0.0
            chain_penalty = 0.0
            for i, (_, hit) in enumerate(group.iterrows()):
                overall_hit = i + 1
                if overall_hit <= 10:
                    base = 2
                else:
                    spacing_cat = hit["Time Since Last Attack"]
                    base = spacing_mapping.get(spacing_cat, 0)
                enemy_bonus = 0.5 if hit["Defender Faction"] == OPPOSING_FACTION else 0.0
                chain_val = int(hit["Chain Value"])
                if chain_val in [10, 25, 50] and hit["Defender Faction"] != OPPOSING_FACTION:
                    bonus_pen = -10
                    penalty_hit = {
                        "Chain ID": chain_id,
                        "Attacker Name": attacker,
                        "Chain Value": chain_val,
                        "Hit Number": overall_hit,
                        "Timestamp Epoch": hit["Timestamp Ended"],
                        "Time Ended (UTC)": datetime.utcfromtimestamp(int(hit["Timestamp Ended"])).strftime('%Y-%m-%d %H:%M:%S')
                    }
                    penalty_hits_details.append(penalty_hit)
                else:
                    bonus_pen = 0
                chain_base += (base + enemy_bonus)
                chain_penalty += bonus_pen
            duration = group["Timestamp Ended"].max() - group["Timestamp Ended"].min()
            bonus_time = 2 * (duration // 600)
            participation_bonus = 5
            chain_bonus = bonus_time + participation_bonus
            chain_total = chain_base + chain_penalty + chain_bonus

            attacker_base_points[attacker] = attacker_base_points.get(attacker, 0) + chain_base
            attacker_penalty_points[attacker] = attacker_penalty_points.get(attacker, 0) + chain_penalty
            attacker_chain_bonus[attacker] = attacker_chain_bonus.get(attacker, 0) + chain_bonus
            attacker_chain_points[attacker] = attacker_chain_points.get(attacker, 0) + chain_total

        if penalty_hits_details:
            penalty_df = pd.DataFrame(penalty_hits_details)
            penalty_df.to_csv(PENALTY_HITS_CSV, index=False)
            logging.info(f"Saved penalty hits details to {PENALTY_HITS_CSV}")

        total_hits = df_attacks.groupby("Attacker Name").size()
        points_df = pd.DataFrame({
            "Total Hits": total_hits,
            "Base Points": pd.Series(attacker_base_points),
            "Penalty Points": pd.Series(attacker_penalty_points),
            "Chain Bonus": pd.Series(attacker_chain_bonus),
            "Chain Points": pd.Series(attacker_chain_points)
        }).fillna(0)
        points_df["Total Points"] = points_df["Chain Points"]

        final_summary = final_summary.merge(points_df, left_index=True, right_index=True, how="left")
        final_summary["Attacks #1-10"] = merged_1_10["Attacks #1-10_formatted"]

        if error_logs:
            error_df = pd.DataFrame({"Errors": [" | ".join(error_logs)]})
            error_df.to_csv(ERROR_LOG_CSV, index=False)
            logging.warning(f"Errors detected! Check '{ERROR_LOG_CSV}' for details.")

        # ------------------- New Features: "Cut", FACTION CUT, and TOTALS ------------------- #
        players_total_points = final_summary["Total Points"].sum()
        players_total_hits = final_summary["Total Hits"].sum()
        faction_cut_points = players_total_hits / 2.0
        denominator = players_total_points + faction_cut_points

        final_summary["Cut"] = final_summary["Total Points"] / denominator

        # Create the FACTION CUT row and drop columns that are all NA.
        faction_row = {col: None for col in final_summary.columns}
        faction_row["Total Points"] = faction_cut_points
        faction_row["Cut"] = faction_cut_points / denominator
        faction_row_df = pd.DataFrame([faction_row], index=["FACTION CUT"]).reindex(columns=final_summary.columns)
        faction_row_df = faction_row_df.dropna(axis=1, how='all')
        final_summary = pd.concat([final_summary, faction_row_df], sort=False)

        # Create the TOTALS row.
        totals = {col: pd.to_numeric(final_summary[col], errors='coerce').sum() for col in final_summary.columns}
        if abs(totals.get("Cut", 0) - 1.0) > 1e-6:
            raise Exception(f"Total Cut is {totals.get('Cut', 0)} instead of 1")
        totals_row_df = pd.DataFrame([totals], index=["TOTALS"]).reindex(columns=final_summary.columns)
        totals_row_df = totals_row_df.dropna(axis=1, how='all')
        final_summary = pd.concat([final_summary, totals_row_df], sort=False)

        final_summary.to_csv(FINAL_SUMMARY_CSV, index=True)
        logging.info(f"Saved final summary to {FINAL_SUMMARY_CSV}")

    except Exception as e:
        logging.exception(f"Exception in process_torn_attacks: {e}")

# -------------------------------------- Main -------------------------------------- #
if __name__ == "__main__":
    try:
        update_war_info()  # Update war info.
        process_torn_attacks(START_TIME, END_TIME)
    except Exception as e:
        logging.exception(f"Fatal error in main: {e}")