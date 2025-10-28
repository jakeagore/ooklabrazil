# -*- coding: utf-8 -*-
"""
Created on Wed Sep 24 06:56:37 2025

@author: jakea
"""

from datetime import datetime
import geopandas as gp
import pandas as pd
import numpy as np
import os
from shapely.geometry import box

# ----------------------------- #
# --- Ookla Tile Aquisition --- #
# ----------------------------- #
# Ookla URL generator
def quarter_start(year: int, q: int) -> datetime:
    if not 1 <= q <= 4:
        raise ValueError("Quarter must be within [1, 2, 3, 4]")
    month = [1, 4, 7, 10]
    return datetime(year, month[q - 1], 1)

def get_tile_url(service_type: str, year: int, q: int) -> str:
    dt = quarter_start(year, q)
    base_url = "https://ookla-open-data.s3-us-west-2.amazonaws.com/shapefiles/performance"
    url = f"{base_url}/type%3D{service_type}/year%3D{dt:%Y}/quarter%3D{q}/{dt:%Y-%m-%d}_performance_{service_type}_tiles.zip"
    return url

# Uncomment and modify these for desired service type and time period
# service type: "mobile" (mobile cellular) or "fixed" (global fixed broadband)
# service_type = "fixed"
# year = 2025
# quarter = 1

# Print URL for downloading from the web
# tile_url = get_tile_url(service_type, year, quarter)
# print(tile_url)

# --------------------------------- #
# --- Load Brazilian Shapefiles --- #
# --------------------------------- #
# Load Brazilian States
# Download from: https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2024/Brasil/BR_UF_2024.zip
state_shapefile_path = "D:\\BR_UF_2024\\BR_UF_2024.shp"
br_states = gp.read_file(state_shapefile_path).to_crs(4326)

# Load Brazilian Municipalities
# Download from: https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2024/Brasil/BR_Municipios_2024.zip
municipality_shapefile_path = "D:\\BR_Municipios_2024\\BR_Municipios_2024.shp"
br_municipalities = gp.read_file(municipality_shapefile_path).to_crs(4326)

# Define field names
STATE_ID_FIELD = 'CD_UF'
STATE_NAME_FIELD = 'NM_UF'
MUN_ID_FIELD = 'CD_MUN'
MUN_NAME_FIELD = 'NM_MUN'

# ------------------------------------------------- #
# --- Join Ookla Tiles and Brazilian Shapefiles --- #
# ------------------------------------------------- #

# Pre-Processing: Filter tiles by Brazil's bounding box
min_lon, max_lon = -74, -34
min_lat, max_lat = -34, 5
brazil_bbox = box(min_lon, min_lat, max_lon, max_lat)

# Ensure spatial indices are built (may be redundent, GeoPandas uses spatial indices already, but explicitly building them could improve runtime)
br_states.sindex
br_municipalities.sindex

# Lists to collect stats
all_state_stats = []
all_municipality_stats = []

# Loop over service types and quarters
# Modify for desired service type and time period
service_types = ["fixed", "mobile"]
quarters = [1, 2, 3] # Modified to 3 quarters for 2025; append a 4th quarter for a fully accessible year
year = 2025

for service_type in service_types:
    for quarter in quarters:
        
        # Load tiles
        dt = quarter_start(year, quarter)
        tile_shapefile_path = f"D:\\{dt:%Y-%m-%d}_performance_{service_type}_tiles\\gps_{service_type}_tiles.shp"
        tiles = gp.read_file(tile_shapefile_path)
        
        # Filter tiles
        tiles_filtered = tiles[tiles.geometry.intersects(brazil_bbox)].copy()
        tiles_filtered.sindex

        # Spatial Joins
        # Metrics: 
        # - avg download speed
        # - avg upload speed
        # - average latency
        
        # TODO avg_lat_up_ms & avg_lat_down_ms parquet only, causing problems, resolve later
        # - average latency under load of all tests performed in the tile as measured during the download phase of the test: Use Case: Streaming, web browsing
        # - average latency under load of all tests performed in the tile as measured during the upload phase of the test: Use Case: Video calls, uploads
        
        # States
        tiles_in_br_states = gp.sjoin(tiles_filtered, br_states, how="inner", predicate='intersects')
        
        tiles_in_br_states['avg_d_mbps'] = tiles_in_br_states['avg_d_kbps'] / 1000 # converted kbps -> mbps
        tiles_in_br_states['avg_u_mbps'] = tiles_in_br_states['avg_u_kbps'] / 1000
        tiles_in_br_states['avg_lat_ms'] = tiles_in_br_states['avg_lat_ms']
        
        # Municipalities
        tiles_in_br_municipalities = gp.sjoin(tiles_filtered, br_municipalities, how="inner", predicate='intersects')
        
        tiles_in_br_municipalities['avg_d_mbps'] = tiles_in_br_municipalities['avg_d_kbps'] / 1000
        tiles_in_br_municipalities['avg_u_mbps'] = tiles_in_br_municipalities['avg_u_kbps'] / 1000
        tiles_in_br_municipalities['avg_lat_ms'] = tiles_in_br_municipalities['avg_lat_ms']
        
        # Aggregate All Metrics with Weighted Averages
        # States
        state_stats = (
            tiles_in_br_states.groupby([STATE_ID_FIELD, STATE_NAME_FIELD])
            .apply(
                lambda x: pd.Series({
                    # Speeds
                    "avg_d_mbps_wt": np.average(x["avg_d_mbps"], weights=x["tests"]),
                    "avg_u_mbps_wt": np.average(x["avg_u_mbps"], weights=x["tests"]),
                    # Latency
                    "avg_lat_ms_wt": np.average(x["avg_lat_ms"], weights=x["tests"]),
                }),
                include_groups=False
            )
            .reset_index()
            .merge(
                tiles_in_br_states.groupby([STATE_ID_FIELD, STATE_NAME_FIELD]).agg(tests=("tests", "sum")).reset_index(),
                on=[STATE_ID_FIELD, STATE_NAME_FIELD],
            )
        )
        state_stats['service_type'] = service_type
        state_stats['year'] = year
        state_stats['quarter'] = quarter
        all_state_stats.append(state_stats)
        
        # Municipalities
        municipality_stats = (
            tiles_in_br_municipalities.groupby([MUN_ID_FIELD, MUN_NAME_FIELD])
            .apply(
                lambda x: pd.Series({
                    # Speeds
                    "avg_d_mbps_wt": np.average(x["avg_d_mbps"], weights=x["tests"]),
                    "avg_u_mbps_wt": np.average(x["avg_u_mbps"], weights=x["tests"]),
                    # Latency
                    "avg_lat_ms_wt": np.average(x["avg_lat_ms"], weights=x["tests"]),
                }),
                include_groups=False
            )
            .reset_index()
            .merge(
                tiles_in_br_municipalities.groupby([MUN_ID_FIELD, MUN_NAME_FIELD]).agg(tests=("tests", "sum")).reset_index(),
                on=[MUN_ID_FIELD, MUN_NAME_FIELD],
            )
        )
        municipality_stats['service_type'] = service_type
        municipality_stats['year'] = year
        municipality_stats['quarter'] = quarter
        all_municipality_stats.append(municipality_stats)

# Concatenate all stats
all_state_df = pd.concat(all_state_stats, ignore_index=True) if all_state_stats else pd.DataFrame()
all_municipality_df = pd.concat(all_municipality_stats, ignore_index=True) if all_municipality_stats else pd.DataFrame()

# -------------------------- #
# --- EXPORT FOR TABLEAU --- #
# -------------------------- #
# Set directory where the csv files will go; Change to your preferred location
downloads_path = r"C:\Users\jakea\Downloads"

# Export States
state_export = all_state_df[[
    'CD_UF', 'NM_UF', 'service_type', 'year', 'quarter',
    'avg_d_mbps_wt', 'avg_u_mbps_wt', 'avg_lat_ms_wt', 'tests'
]].copy()
state_export.columns = [
    'state_code', 'state_name', 'service_type', 'year', 'quarter',
    'avg_d_Mbps', 'avg_u_Mbps', 'avg_lat_ms', 'tests'
]
state_export.to_csv(os.path.join(downloads_path, f'brazil_state_connectivity_{year}.csv'), index=False)

# Add state names to municipality data; for establishing a relationship between connections in Tableau
state_mapping = dict(zip(br_states['CD_UF'], br_states['NM_UF']))
all_municipality_df['NM_UF'] = all_municipality_df['CD_MUN'].astype(str).str[:2].map(state_mapping)

# Export Municipalities
mun_export = all_municipality_df[[
    'CD_MUN', 'NM_MUN', 'NM_UF', 'service_type', 'year', 'quarter',
    'avg_d_mbps_wt', 'avg_u_mbps_wt', 'avg_lat_ms_wt', 'tests'
]].copy()
mun_export.columns = [
    'municipality_code', 'municipality_name', 'state_name', 'service_type', 'year', 'quarter',
    'avg_d_Mbps', 'avg_u_Mbps', 'avg_lat_ms', 'tests'
]
mun_export.to_csv(os.path.join(downloads_path, f'brazil_municipality_connectivity_{year}.csv'), index=False)