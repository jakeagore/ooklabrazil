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
# Check current dir
print(os.getcwd())

# Check if path exists: returns T or F
os.path.exists("filename")

# Files you'll need
# tile_shapefile: Ookla-open-data based on service type ("fixed" for global fixed broadband "mobile" for cellular), year, and quarter
# county_shapefile: County boundaries from the U.S. Census Bureau ftp site.
# place_shapefile

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

tile_url = get_tile_url("fixed", 2020, 2)
tile_url

# Load tiles: Download the zip file, extract it to the current working directory, and verify it's there
tile_shapefile_path = "C:/Users/jakea/OneDrive/Documentos/capstone/spyder_working_dir/2020-04-01_performance_fixed_tiles/gps_fixed_tiles.shp"
tiles = gp.read_file(tile_shapefile_path)
print(tiles.head())

# Load counties
# zipfile of region's county boundaries
county_shapefile_path = "C:/Users/jakea/OneDrive/Documentos/capstone/spyder_working_dir/tl_2019_us_county/tl_2019_us_county.shp"
counties = gp.read_file(county_shapefile_path)
# filter out the fips code and reproject to match the tiles
ky_counties = counties.loc[counties['STATEFP'] == '21'].to_crs(4326)
print(ky_counties.head())

# Spatial join and convert speeds
tiles_in_ky_counties = gp.sjoin(tiles, ky_counties, how="inner", predicate='intersects')
tiles_in_ky_counties['avg_d_mbps'] = tiles_in_ky_counties['avg_d_kbps'] / 1000
tiles_in_ky_counties['avg_u_mbps'] = tiles_in_ky_counties['avg_u_kbps'] / 1000
print(tiles_in_ky_counties.head())

# Aggregate weighted average download speeds
county_stats = (
    tiles_in_ky_counties.groupby(["GEOID", "NAMELSAD"])
    .apply(
        lambda x: pd.Series({"avg_d_mbps_wt": np.average(x["avg_d_mbps"], weights=x["tests"])}),
        include_groups=False
    )
    .reset_index()
    .merge(
        tiles_in_ky_counties.groupby(["GEOID", "NAMELSAD"]).agg(tests=("tests", "sum")).reset_index(),
        on=["GEOID", "NAMELSAD"],
    )
)

# Create top/bottom 20 table
table_stats = (
    county_stats.loc[county_stats["tests"] >= 50]
    .nlargest(20, "avg_d_mbps_wt")
    .pipe(lambda x: pd.concat([x, county_stats.loc[county_stats["tests"] >= 50].nsmallest(20, "avg_d_mbps_wt")]))
    .sort_values("avg_d_mbps_wt", ascending=False)
    .round(2)
)
header = ["GEOID", "County", "Avg download speed (Mbps)", "Tests"]
table_stats = table_stats.rename(columns=dict(zip(table_stats.columns, header)))
print("Top and Bottom 20 Counties by Download Speed (tests >= 50):")
print(table_stats)

# Mapping steps
county_data = ky_counties[['GEOID', 'geometry']].merge(county_stats, on='GEOID').to_crs(26916)
labels = ["0 to 25 Mbps", "25 to 50 Mbps", "50 to 100 Mbps", "100 to 150 Mbps", "150 to 200 Mbps"]
county_data['group'] = pd.cut(
    county_data.avg_d_mbps_wt, 
    (0, 25, 50, 100, 150, 200), 
    right=False, 
    labels=labels
)

place_shapefile_path = "C:/Users/jakea/OneDrive/Documentos/capstone/spyder_working_dir/tl_2019_21_place/tl_2019_21_place.shp"
ky_places = gp.read_file(place_shapefile_path)
ky_places = ky_places.loc[ky_places['PCICBSA'] >= "Y"].sample(15, random_state=1).to_crs(26916)
ky_places["centroid"] = ky_places["geometry"].centroid
ky_places = ky_places.set_geometry("centroid")
fig, ax = plt.subplots(1, figsize=(16, 6))
county_data.plot(
    column="group", cmap="BuPu", linewidth=0.4, ax=ax, edgecolor="0.1", legend=True
)
ax.axis("off")
leg = ax.get_legend()
leg.set_bbox_to_anchor((1.15, 0.3))
leg.set_title("Mean download speed (Mbps)\nin Kentucky Counties")
texts = []
for x, y, label in zip(ky_places.geometry.x, ky_places.geometry.y, ky_places["NAME"]):
    texts.append(plt.text(x, y, label, fontsize=10, fontweight="bold", ha="left"))
adjust_text(
    texts,
    force_points=0.3,
    force_text=0.8,
    expand_points=(1, 1),
    expand_text=(1, 1),
    arrowprops=dict(arrowstyle="-", color="black", lw=0.5, shrinkA=10, shrinkB=10)
)
plt.show()