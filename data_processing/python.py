# -*- coding: utf-8 -*-
"""
Created on Wed Sep 24 06:56:37 2025

@author: jakea
"""

from datetime import datetime
import geopandas as gp
import pandas as pd
import numpy as np

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

tile_url = get_tile_url("fixed", 2024, 4)
print(tile_url)

# Download from: generated ookla-open-data URL
tile_shapefile_path = "C:/Users/jakea/OneDrive/Documentos/capstone/spyder_working_dir/2024-10-01_performance_fixed_tiles/gps_fixed_tiles.shp"
tiles = gp.read_file(tile_shapefile_path)

# Pre-Processing: Filter tiles by Brazil's bounding box
min_lon, max_lon = -74, -34
min_lat, max_lat = -34, 5
tiles['centroid'] = tiles.geometry.centroid
tiles_filtered = tiles[
    (tiles['centroid'].x >= min_lon) & (tiles['centroid'].x <= max_lon) &
    (tiles['centroid'].y >= min_lat) & (tiles['centroid'].y <= max_lat)
].copy()
tiles_filtered = tiles_filtered.drop(columns=['centroid'])

# --------------------------------- #
# --- Load Brazilian Shapefiles --- #
# --------------------------------- #
# Load Brazilian States
# Download from: https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2024/Brasil/BR_UF_2024.zip
state_shapefile_path = "C:/Users/jakea/OneDrive/Documentos/capstone/spyder_working_dir/BR_UF_2024/BR_UF_2024.shp"
br_states = gp.read_file(state_shapefile_path)
br_states = br_states.to_crs(4326)

# Load Brazilian Municipalities
# Download from: https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2024/Brasil/BR_Municipios_2024.zip
municipality_shapefile_path = "C:/Users/jakea/OneDrive/Documentos/capstone/spyder_working_dir/BR_Municipios_2024/BR_Municipios_2024.shp"
br_municipalities = gp.read_file(municipality_shapefile_path)
br_municipalities = br_municipalities.to_crs(4326)

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
# States
tiles_in_br_states = gp.sjoin(tiles_filtered, br_states, how="inner", predicate='intersects')

# Metrics: 
# - avg download speed, avg upload speed
# - average latency: Use Case: Overall network quality
# - average latency under load of all tests performed in the tile as measured during the download phase of the test: Use Case: Streaming, web browsing
# - average latency under load of all tests performed in the tile as measured during the upload phase of the test: Use Case: Video calls, uploads
tiles_in_br_states['avg_d_mbps'] = tiles_in_br_states['avg_d_kbps'] / 1000 # converted kbps -> mbps
tiles_in_br_states['avg_u_mbps'] = tiles_in_br_states['avg_u_kbps'] / 1000
tiles_in_br_states['avg_lat_ms'] = tiles_in_br_states['avg_lat_ms']
tiles_in_br_states['avg_lat_down_ms'] = tiles_in_br_states['avg_lat_down_ms']
tiles_in_br_states['avg_lat_up_ms'] = tiles_in_br_states['avg_lat_up_ms']

# Municipalities
tiles_in_br_municipalities = gp.sjoin(tiles_filtered, br_municipalities, how="inner", predicate='intersects')

tiles_in_br_municipalities['avg_d_mbps'] = tiles_in_br_municipalities['avg_d_kbps'] / 1000
tiles_in_br_municipalities['avg_u_mbps'] = tiles_in_br_municipalities['avg_u_kbps'] / 1000
tiles_in_br_municipalities['avg_lat_ms'] = tiles_in_br_municipalities['avg_lat_ms']
tiles_in_br_municipalities['avg_lat_down_ms'] = tiles_in_br_municipalities['avg_lat_down_ms']
tiles_in_br_municipalities['avg_lat_up_ms'] = tiles_in_br_municipalities['avg_lat_up_ms']

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
            "avg_lat_down_ms_wt": np.average(x["avg_lat_down_ms"], weights=x["tests"]),
            "avg_lat_up_ms_wt": np.average(x["avg_lat_up_ms"], weights=x["tests"]),
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
            "avg_lat_down_ms_wt": np.average(x["avg_lat_down_ms"], weights=x["tests"]),
            "avg_lat_up_ms_wt": np.average(x["avg_lat_up_ms"], weights=x["tests"]),
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
# Add state names to municipality data for better geocoding
state_mapping = dict(zip(state_stats['CD_UF'], state_stats['NM_UF']))
municipality_stats['NM_UF'] = municipality_stats['CD_MUN'].astype(str).str[:2].map(state_mapping)

# Export States with All metrics
state_export = state_stats[[
    'CD_UF', 'NM_UF', 
    'avg_d_mbps_wt', 'avg_u_mbps_wt',
    'avg_lat_ms_wt', 'avg_lat_down_ms_wt', 'avg_lat_up_ms_wt',
    'tests'
]].copy()

state_export.columns = [
    'State_Code', 'State_Name',
    'Avg_Download_Mbps', 'Avg_Upload_Mbps',
    'Avg_Latency_ms', 'Avg_Latency_Down_ms', 'Avg_Latency_Up_ms',
    'Total_Tests'
]
state_export.to_csv('Brazil_State_Connectivity_Full.csv', index=False)

# Export Municipalities with All metrics
mun_export = municipality_stats[[
    'CD_MUN', 'NM_MUN', 'NM_UF',
    'avg_d_mbps_wt', 'avg_u_mbps_wt',
    'avg_lat_ms_wt', 'avg_lat_down_ms_wt', 'avg_lat_up_ms_wt',
    'tests'
]].copy()

mun_export.columns = [
    'Municipality_Code', 'Municipality_Name', 'State_Name',
    'Avg_Download_Mbps', 'Avg_Upload_Mbps',
    'Avg_Latency_ms', 'Avg_Latency_Down_ms', 'Avg_Latency_Up_ms',
    'Total_Tests'
]
mun_export.to_csv('Brazil_Municipality_Connectivity_Full.csv', index=False)