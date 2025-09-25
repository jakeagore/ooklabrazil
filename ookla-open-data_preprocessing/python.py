# -*- coding: utf-8 -*-
"""
Created on Wed Sep 24 06:56:37 2025

@author: jakea
"""

%matplotlib inline

import os
from datetime import datetime
import geopandas as gp
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from shapely.geometry import Point
from adjustText import adjust_text

# Troubleshooting
print(os.getcwd()) # Get current working directory
print(os.path.exists("filename")) # Check if a path exists. Returns T/F

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

<<<<<<< Updated upstream
tile_url = get_tile_url("fixed", 2020, 2)
=======
tile_url = get_tile_url("fixed", 2024, 4)
>>>>>>> Stashed changes
print(tile_url)

# Load tiles
# Download from: generated ookla-open-data URL
<<<<<<< Updated upstream
tile_shapefile_path = "C:/Users/jakea/OneDrive/Documentos/capstone/spyder_working_dir/2020-04-01_performance_fixed_tiles/gps_fixed_tiles.shp"
=======
tile_shapefile_path = "C:/Users/jakea/OneDrive/Documentos/capstone/spyder_working_dir/2024-10-01_performance_fixed_tiles/gps_fixed_tiles.shp" # make sure year and quarter are intended ones
>>>>>>> Stashed changes
tiles = gp.read_file(tile_shapefile_path)
# print(tiles.head())

# Load Brazilian states
# Download from: https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2024/Brasil/BR_UF_2024.zip
state_shapefile_path = "C:/Users/jakea/OneDrive/Documentos/capstone/spyder_working_dir/BR_UF_2024/BR_UF_2024.shp"
br_states = gp.read_file(state_shapefile_path)
br_states = br_states.to_crs(4326)
# print("State columns:", br_states.columns.tolist()) # For troubleshooting. Check fields like 'CD_UF' and 'NM_UF'

# Load Brazilian municipalities
# Download from: https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2024/Brasil/BR_Municipios_2024.zip
municipality_shapefile_path = "C:/Users/jakea/OneDrive/Documentos/capstone/spyder_working_dir/BR_Municipios_2024/BR_Municipios_2024.shp"
br_municipalities = gp.read_file(municipality_shapefile_path)
br_municipalities = br_municipalities.to_crs(4326)
# print("Municipality columns:", br_municipalities.columns.tolist()) # For troubleshooting. Check fields like 'CD_MUN' and 'NM_MUN'

# Define field names
STATE_ID_FIELD = 'CD_UF'
STATE_NAME_FIELD = 'NM_UF'
MUN_ID_FIELD = 'CD_MUN'
MUN_NAME_FIELD = 'NM_MUN'

# Spatial join and convert speeds for states
tiles_in_br_states = gp.sjoin(tiles, br_states, how="inner", predicate='intersects')
tiles_in_br_states['avg_d_mbps'] = tiles_in_br_states['avg_d_kbps'] / 1000
tiles_in_br_states['avg_u_mbps'] = tiles_in_br_states['avg_u_kbps'] / 1000
# print(tiles_in_br_states.head())

# Spatial join and convert speeds for municipalities
tiles_in_br_municipalities = gp.sjoin(tiles, br_municipalities, how="inner", predicate='intersects')
tiles_in_br_municipalities['avg_d_mbps'] = tiles_in_br_municipalities['avg_d_kbps'] / 1000
tiles_in_br_municipalities['avg_u_mbps'] = tiles_in_br_municipalities['avg_u_kbps'] / 1000
# print(tiles_in_br_municipalities.head())

# Aggregate weighted average download speeds for states
state_stats = (
    tiles_in_br_states.groupby([STATE_ID_FIELD, STATE_NAME_FIELD])
    .apply(
        lambda x: pd.Series({"avg_d_mbps_wt": np.average(x["avg_d_mbps"], weights=x["tests"])}),
        include_groups=False
    )
    .reset_index()
    .merge(
        tiles_in_br_states.groupby([STATE_ID_FIELD, STATE_NAME_FIELD]).agg(tests=("tests", "sum")).reset_index(),
        on=[STATE_ID_FIELD, STATE_NAME_FIELD],
    )
)

# Aggregate weighted average download speeds for municipalities
municipality_stats = (
    tiles_in_br_municipalities.groupby([MUN_ID_FIELD, MUN_NAME_FIELD])
    .apply(
        lambda x: pd.Series({"avg_d_mbps_wt": np.average(x["avg_d_mbps"], weights=x["tests"])}),
        include_groups=False
    )
    .reset_index()
    .merge(
        tiles_in_br_municipalities.groupby([MUN_ID_FIELD, MUN_NAME_FIELD]).agg(tests=("tests", "sum")).reset_index(),
        on=[MUN_ID_FIELD, MUN_NAME_FIELD],
    )
)

# Top/Bottom Tables
# Create top/bottom 5 table for states
state_table_stats = (
    state_stats.loc[state_stats["tests"] >= 50]
    .nlargest(5, "avg_d_mbps_wt")
    .pipe(lambda x: pd.concat([x, state_stats.loc[state_stats["tests"] >= 50].nsmallest(5, "avg_d_mbps_wt")]))
    .sort_values("avg_d_mbps_wt", ascending=False)
    .round(2)
)
state_header = [STATE_ID_FIELD, "State", "Avg download speed (Mbps)", "Tests"]
state_table_stats = state_table_stats.rename(columns=dict(zip(state_table_stats.columns, state_header)))
print("Top and Bottom 5 States by Download Speed (tests >= 50):")
print(state_table_stats)

# Create top/bottom 20 table for municipalities
mun_table_stats = (
    municipality_stats.loc[municipality_stats["tests"] >= 50]
    .nlargest(20, "avg_d_mbps_wt")
    .pipe(lambda x: pd.concat([x, municipality_stats.loc[municipality_stats["tests"] >= 50].nsmallest(20, "avg_d_mbps_wt")]))
    .sort_values("avg_d_mbps_wt", ascending=False)
    .round(2)
)
mun_header = [MUN_ID_FIELD, "Municipality", "Avg download speed (Mbps)", "Tests"]
mun_table_stats = mun_table_stats.rename(columns=dict(zip(mun_table_stats.columns, mun_header)))
print("Top and Bottom 20 Municipalities by Download Speed (tests >= 50):")
print(mun_table_stats)

# Mapping
# Common plotting setup
labels = ["0 to 25 Mbps", "25 to 50 Mbps", "50 to 100 Mbps", "100 to 150 Mbps", "150+ Mbps"]
bins = [0, 25, 50, 100, 150, np.inf]
capitals = [
    'Rio Branco', 'Maceió', 'Macapá', 'Manaus', 'Salvador', 'Fortaleza', 'Brasília', 
    'Vitória', 'Goiânia', 'São Luís', 'Cuiabá', 'Campo Grande', 'Belo Horizonte', 
    'Belém', 'João Pessoa', 'Curitiba', 'Recife', 'Teresina', 'Rio de Janeiro', 
    'Natal', 'Porto Alegre', 'Porto Velho', 'Boa Vista', 'Florianópolis', 
    'São Paulo', 'Aracaju', 'Palmas'
]
br_capitals = br_municipalities[br_municipalities[MUN_NAME_FIELD].str.strip().isin([c.strip() for c in capitals])].to_crs(5880)
if not br_capitals.empty:
    br_capitals["centroid"] = br_capitals["geometry"].centroid
    br_capitals = br_capitals.set_geometry("centroid")

# State map
state_data = br_states[[STATE_ID_FIELD, 'geometry']].merge(state_stats, on=STATE_ID_FIELD, how='left')
state_data['avg_d_mbps_wt'] = state_data['avg_d_mbps_wt'].fillna(0)
state_data = state_data.to_crs(5880)
state_data['group'] = pd.cut(state_data['avg_d_mbps_wt'], bins=bins, right=False, labels=labels)

fig, ax = plt.subplots(1, figsize=(12, 12))
state_data.plot(
    column="group", cmap="BuPu", linewidth=0.4, ax=ax, edgecolor="black", legend=True, missing_kwds={"color": "lightgrey"}
)
ax.set_axis_off()
leg = ax.get_legend()
if leg:
    leg.set_bbox_to_anchor((1.05, 0.5))
    leg.set_title("Mean download speed (Mbps)\nin Brazilian States")
if not br_capitals.empty:
    texts = []
    for x, y, label in zip(br_capitals.geometry.x, br_capitals.geometry.y, br_capitals[MUN_NAME_FIELD]):
        texts.append(ax.text(x, y, label, fontsize=8, fontweight="bold", ha="center", va="center", transform=ax.transData))
    adjust_text(
        texts,
        force_points=0.2,
        force_text=0.6,
        expand_points=(1.2, 1.2),
        expand_text=(1.2, 1.2),
        arrowprops=dict(arrowstyle="-|>", color="black", lw=0.3, shrinkA=30, shrinkB=30)
    )
plt.tight_layout()
plt.show()

# Municipality map
municipality_data = br_municipalities[[MUN_ID_FIELD, 'geometry']].merge(municipality_stats, on=MUN_ID_FIELD, how='left')
municipality_data['avg_d_mbps_wt'] = municipality_data['avg_d_mbps_wt'].fillna(0)
municipality_data = municipality_data.to_crs(5880)
municipality_data['group'] = pd.cut(municipality_data['avg_d_mbps_wt'], bins=bins, right=False, labels=labels)

fig, ax = plt.subplots(1, figsize=(12, 12))
municipality_data.plot(
    column="group", cmap="BuPu", linewidth=0.1, ax=ax, edgecolor="black", legend=True, missing_kwds={"color": "lightgrey"}
)
ax.set_axis_off()
leg = ax.get_legend()
if leg:
    leg.set_bbox_to_anchor((1.05, 0.5))
    leg.set_title("Mean download speed (Mbps)\nin Brazilian Municipalities")
if not br_capitals.empty:
    texts = []
    for x, y, label in zip(br_capitals.geometry.x, br_capitals.geometry.y, br_capitals[MUN_NAME_FIELD]):
        texts.append(ax.text(x, y, label, fontsize=6, fontweight="bold", ha="center", va="center", transform=ax.transData))
    adjust_text(
        texts,
        force_points=0.2,
        force_text=0.6,
        expand_points=(1.2, 1.2),
        expand_text=(1.2, 1.2),
        arrowprops=dict(arrowstyle="-|>", color="black", lw=0.3, shrinkA=30, shrinkB=30)
    )
plt.tight_layout()
<<<<<<< Updated upstream
plt.show()
=======
plt.show()
>>>>>>> Stashed changes
