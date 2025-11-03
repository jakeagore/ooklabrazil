# -*- coding: utf-8 -*-
"""
Created on Mon Nov  3 08:56:23 2025

@author: jakea
"""

import pandas as pd
import pathlib
import glob

# --------------------- #
# --- CONFIGURATION --- #
# --------------------- #
DOWNLOADS = pathlib.Path(r"C:\Users\jakea\Downloads")
OUTPUT_FILE = DOWNLOADS / "brazil_combined_connectivity_2021_2025.csv"

# Patterns for the two file families
MUNI_PATTERN = DOWNLOADS / "brazil_municipality_connectivity_*.csv"
STATE_PATTERN = DOWNLOADS / "brazil_state_connectivity_*.csv"

# --------------- #
# --- HELPERS --- #
# --------------- #
def read_and_tag(pattern: str, level: str) -> pd.DataFrame:
    """
    Read all CSVs matching *pattern*, add geographical_level column,
    and return a concatenated DataFrame.
    """
    dfs = []
    for f in glob.glob(str(pattern)):
        df = pd.read_csv(f, dtype=str)          # read everything as string first
        # Convert numeric columns back to float (except codes/ids)
        numeric_cols = ["avg_d_Mbps", "avg_u_Mbps", "avg_lat_ms", "tests",
                        "year", "quarter"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        df["geographical_level"] = level
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

# ------------ #
# --- MAIN --- #
# ------------ #
muni_df = read_and_tag(MUNI_PATTERN, "municipality")
state_df = read_and_tag(STATE_PATTERN, "state")
combined = pd.concat([muni_df, state_df], ignore_index=True)

# Re-order columns so geographical_level appears early (optional)
cols = list(combined.columns)
cols.remove("geographical_level")
new_order = ["geographical_level"] + cols
combined = combined[new_order]

# WRITE MASTER CSV
combined.to_csv(OUTPUT_FILE, index=False, float_format="%.6f")