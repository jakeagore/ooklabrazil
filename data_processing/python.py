# -*- coding: utf-8 -*-
"""
Created on Wed Sep 24 06:56:37 2025

@author: jakea
"""

# TODO Optimization Ideas:
# - select which year and quarter from Tableau board
# - select desired metric, download speed, upload speed, and/or latency
# - select states and/or municipalities

from datetime import datetime
import geopandas as gp
import pandas as pd
import numpy as np
import os
from shapely.geometry import box

# ------------------------------------ #
# --- Load Ookla Performance Tiles --- #
# ------------------------------------ #
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

# service type: "mobile" or "fixed", year, quarter
# Modify these for desired service type and time period
service_type = "fixed"
year = 2019
quarter = 1

# Print URL for downloading
tile_url = get_tile_url(service_type, year, quarter)
print(tile_url)

# Load tiles
# Download from: generated ookla-open-data URL
dt = quarter_start(year, quarter)
tile_shapefile_path = f"D:\\{dt:%Y-%m-%d}_performance_{service_type}_tiles\\gps_{service_type}_tiles.shp"
tiles = gp.read_file(tile_shapefile_path)

# Pre-Processing: Filter tiles by Brazil's bounding box
min_lon, max_lon = -74, -34
min_lat, max_lat = -34, 5
# Create Brazil bounding box polygon
brazil_bbox = box(min_lon, min_lat, max_lon, max_lat)
# Filter
tiles_filtered = tiles[tiles.geometry.intersects(brazil_bbox)].copy()

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

# Ensure spatial indices are built (may be redundent, GeoPandas uses spatial indices already, but explicitly building them could improve runtime)
tiles_filtered.sindex
br_states.sindex
br_municipalities.sindex

# --------------------- #
# --- Spatial Joins --- #
# --------------------- #
# Metrics: 
# - avg download speed
# - avg upload speed
# - average latency: Use Case: Overall network quality

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

# ---------------------------------------------------- #
# --- Aggregate All Metrics with Weighted Averages --- #
# ---------------------------------------------------- #
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

# -------------------------- #
# --- EXPORT FOR TABLEAU --- #
# -------------------------- #
# Set directory where the csv files will go; Change to your prefered location
downloads_path = r"C:\Users\jakea\Downloads"

# Add state names to municipality data for better geocoding
state_mapping = dict(zip(state_stats['CD_UF'], state_stats['NM_UF']))
municipality_stats['NM_UF'] = municipality_stats['CD_MUN'].astype(str).str[:2].map(state_mapping)

# Export States with All metrics
state_export = state_stats[[
    'CD_UF', 'NM_UF', 'service_type', 'year', 'quarter',
    'avg_d_mbps_wt', 'avg_u_mbps_wt',
    'avg_lat_ms_wt',
    'tests'
]].copy()

state_export.columns = [
    'state_code', 'state_name', 'service_type', 'year', 'quarter',
    'avg_d_Mbps', 'avg_u_Mbps',
    'avg_lat_ms',
    'tests'
]
state_export.to_csv(os.path.join(downloads_path, f'brazil_state_connectivity_{year}.csv'), index=False)

# Export Municipalities with All metrics
mun_export = municipality_stats[[
    'CD_MUN', 'NM_MUN', 'NM_UF', 'service_type', 'year', 'quarter',
    'avg_d_mbps_wt', 'avg_u_mbps_wt',
    'avg_lat_ms_wt',
    'tests'
]].copy()

mun_export.columns = [
    'municipality_code', 'municipality_name', 'state_name', 'service_type', 'year', 'quarter',
    'avg_d_Mbps', 'avg_u_Mbps',
    'avg_lat_ms',
    'tests'
]
mun_export.to_csv(os.path.join(downloads_path, f'brazil_municipality_connectivity_{year}.csv'), index=False)