# -*- coding: utf-8 -*-
"""
Created on Mon Nov  3 12:47:10 2025

@author: jakea
"""

import pandas as pd
import glob
import os

# Set Path
os.chdir(r"C:\Users\jakea\Downloads")

# ------------------ #
# --- FIND FILES --- #
# ------------------ #
muni_files = sorted(glob.glob("Brazil_Municipality_Connectivity_*.csv"))
state_files = sorted(glob.glob("Brazil_State_Connectivity_*.csv"))

# ---------------------- #
# --- READ & PROCESS --- #
# ---------------------- #
dfs = []

for file in muni_files + state_files:    
    df = pd.read_csv(file)

    # Add geography level
    if 'Municipality' in file:
        df['geography_level'] = 'Municipality'
    else:
        df['geography_level'] = 'State'
        df['municipality_code'] = pd.NA
        df['municipality_name'] = pd.NA

    # Use 'year' column or extract from filename
    if 'year' not in df.columns:
        year = int(file.split('_')[-1].replace('.csv', ''))
        df['year'] = year

    dfs.append(df)

# --------------- #
# --- COMBINE --- #
# --------------- #
full_df = pd.concat(dfs, ignore_index=True)

# Standardize columns
cols = [
    'municipality_code', 'municipality_name', 'state_code', 'state_name',
    'service_type', 'year', 'quarter', 'geography_level',
    'avg_lat_ms', 'tests', 'avg_d_Mbps', 'avg_u_Mbps'
]
full_df = full_df.reindex(columns=cols)

# ------------ #
# --- SAVE --- #
# ------------ #
output_file = "brazil_connectivity_long.csv"
full_df.to_csv(output_file, index=False)