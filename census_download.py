import requests
import pandas as pd
import geopandas as gpd
import time
from urllib.parse import quote

# ================================
# 1. Configuration
# ================================
# Your ACS API key (load securely in production)
api_key = "UhcVU80jpNgM7iSEacdehocqxjWzzVIbqnhPPmZc"

# Specify the ACS dataset and year (adjust if needed)
acs_base_url = "https://api.census.gov/data/2021/acs/acs5"
variables_url = f"{acs_base_url}/variables.json"

# Define NYC geography:
# New York state (FIPS "36") and NYC counties:
nyc_state = "36"
nyc_counties = ["005", "047", "061", "081", "085"]

# ================================
# 2. Retrieve the list of all ACS variables available
# ================================
print("Retrieving list of ACS variables...")
resp_vars = requests.get(variables_url)
if resp_vars.status_code != 200:
    raise Exception(f"Error retrieving variables: {resp_vars.status_code} {resp_vars.text}")

vars_json = resp_vars.json()
# Extract all variable keys
all_variable_keys = list(vars_json["variables"].keys())

# Ensure that the human-readable NAME is included.
if "NAME" not in all_variable_keys:
    all_variable_keys = ["NAME"] + all_variable_keys

# Join variables into a comma-separated string.
get_vars_str = ",".join(all_variable_keys)
print(f"Total number of variables to request: {len(all_variable_keys)}")

# ================================
# 3. Download ACS tract-level data for NYC
# ================================
print("Downloading ACS tract-level data for NYC...")
acs_data_rows = []
header = None  # to capture header from first valid query

# For tract-level queries, we specify:
#   for=tract:* 
#   in=state:36 county:{county_code}
for county in nyc_counties:
    params = {
        "get": get_vars_str,
        "for": "tract:*",
        "in": f"state:{nyc_state} county:{county}",
        "key": api_key
    }
    print(f"Requesting data for county FIPS {county}...", end=" ")
    resp = requests.get(acs_base_url, params=params)
    if resp.status_code != 200:
        print(f"Error: {resp.status_code}")
        continue
    json_data = resp.json()
    if not header:
        header = json_data[0]  # set header from first response
    # Append all data rows (skip header)
    acs_data_rows.extend(json_data[1:])
    print(f"got {len(json_data) - 1} rows.")
    time.sleep(0.5)  # be polite to the API

if not acs_data_rows:
    raise Exception("No ACS data was retrieved for NYC.")

# Create a DataFrame from the ACS data.
acs_df = pd.DataFrame(acs_data_rows, columns=header)
print(f"Total ACS tract records downloaded: {len(acs_df)}")

# -------------------------------
# 3a. Create a GEOID for joining
# -------------------------------
# ACS data contains state, county, and tract codes.
# Build the standard GEOID: state (2-digit) + county (3-digit) + tract (6-digit)
acs_df["state"] = acs_df["state"].str.zfill(2)
acs_df["county"] = acs_df["county"].str.zfill(3)
acs_df["tract"] = acs_df["tract"].str.zfill(6)
acs_df["GEOID"] = acs_df["state"] + acs_df["county"] + acs_df["tract"]

# ================================
# 4. Download NYC tract polygons from TIGERweb
# ================================
print("Downloading TIGER tract polygons for NYC...")

# TIGERweb endpoint for tract polygons (layer 3)
tiger_base_url = "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/Tracts_Blocks/MapServer/3/query"

# Build a "where" clause to filter for NYC.
# Note: The query string must be URL encoded.
# The clause checks that STATEFP equals "36" and COUNTYFP is one of the NYC county codes.
county_conditions = " OR ".join([f"COUNTYFP='{c}'" for c in nyc_counties])
where_clause = f"STATEFP='36' AND ({county_conditions})"

# TIGERweb API parameters.
tiger_params = {
    "where": where_clause,
    "outFields": "*",
    "f": "geojson",
    "resultOffset": 0,
    "resultRecordCount": 1000  # adjust if needed
}

tiger_features = []
while True:
    tiger_resp = requests.get(tiger_base_url, params=tiger_params)
    if tiger_resp.status_code != 200:
        raise Exception(f"Error retrieving TIGER data: {tiger_resp.status_code} {tiger_resp.text}")
    tiger_geojson = tiger_resp.json()
    feats = tiger_geojson.get("features", [])
    if not feats:
        break
    tiger_features.extend(feats)
    print(f"Retrieved {len(feats)} features (offset {tiger_params['resultOffset']})")
    if len(feats) < tiger_params["resultRecordCount"]:
        break  # Last page reached
    tiger_params["resultOffset"] += tiger_params["resultRecordCount"]
    time.sleep(0.5)

print(f"Total TIGER tract features downloaded: {len(tiger_features)}")

# Create a GeoDataFrame from the TIGER features.
tiger_geojson_all = {"type": "FeatureCollection", "features": tiger_features}
tiger_gdf = gpd.GeoDataFrame.from_features(tiger_geojson_all["features"])

# -------------------------------
# 4a. Prepare TIGER GEOID for joining
# -------------------------------
# Typically, TIGER tract features include the following fields:
#   STATEFP (state, 2-digit), COUNTYFP (county, 3-digit), TRACTCE (tract, 6-digit)
if "GEOID" not in tiger_gdf.columns:
    tiger_gdf["STATEFP"] = tiger_gdf["STATEFP"].astype(str).str.zfill(2)
    tiger_gdf["COUNTYFP"] = tiger_gdf["COUNTYFP"].astype(str).str.zfill(3)
    tiger_gdf["TRACTCE"] = tiger_gdf["TRACTCE"].astype(str).str.zfill(6)
    tiger_gdf["GEOID"] = tiger_gdf["STATEFP"] + tiger_gdf["COUNTYFP"] + tiger_gdf["TRACTCE"]

# Ensure a coordinate reference system is set (TIGER data is typically NAD83)
if tiger_gdf.crs is None:
    tiger_gdf.set_crs(epsg=4269, inplace=True)

# ================================
# 5. Merge ACS data with TIGER tract polygons
# ================================
print("Merging ACS data with NYC tract polygons...")
merged_gdf = tiger_gdf.merge(acs_df, on="GEOID", how="left")

# ================================
# 6. Export the merged data as GeoJSON
# ================================
output_filename = "nyc_census_acs_with_tracts.geojson"
print(f"Writing the merged data to {output_filename} ...")
merged_gdf.to_file(output_filename, driver="GeoJSON")

print("Finished! GeoJSON with ACS data joined to NYC tract polygons has been created.")