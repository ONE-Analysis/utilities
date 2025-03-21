import requests
import pandas as pd
import geopandas as gpd
import json
import time

def download_nyc_block_groups():
    """
    Download block group boundaries for all NYC boroughs.
    """
    url = "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_Current/MapServer/12/query"

    # All NYC counties
    nyc_counties = {
        '005': 'Bronx',
        '047': 'Brooklyn',
        '061': 'Manhattan',
        '081': 'Queens',
        '085': 'Staten Island'
    }

    where_clause = "STATE='36' AND COUNTY IN ('005','047','061','081','085')"

    params = {
        "where": where_clause,
        "outFields": "*",
        "returnGeometry": "true",
        "f": "geojson",
        "geometryPrecision": 5,
        "spatialRel": "esriSpatialRelIntersects"
    }

    print("Downloading NYC block group boundaries...")

    try:
        response = requests.get(url, params=params)
        if response.status_code != 200:
            raise Exception(f"Error retrieving TIGER data: {response.status_code}")

        geojson_data = response.json()
        feature_count = len(geojson_data.get("features", []))
        print(f"Retrieved {feature_count} block groups")

        # Convert to GeoDataFrame
        gdf = gpd.GeoDataFrame.from_features(geojson_data["features"])

        # Create GEOID for joining with ACS data
        gdf["GEOID"] = gdf["STATE"].astype(str).str.zfill(2) + \
                      gdf["COUNTY"].astype(str).str.zfill(3) + \
                      gdf["TRACT"].astype(str).str.zfill(6) + \
                      gdf["BLKGRP"].astype(str)

        # Add borough names
        gdf["borough"] = gdf["COUNTY"].map(nyc_counties)

        return gdf

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return None

def get_acs_variables(api_key, year="2021"):
    """
    Retrieve list of all available ACS variables.
    """
    variables_url = f"https://api.census.gov/data/{year}/acs/acs5/variables.json"
    print("Retrieving list of ACS variables...")

    try:
        resp = requests.get(variables_url)
        if resp.status_code != 200:
            raise Exception(f"Error retrieving variables: {resp.status_code}")

        vars_json = resp.json()
        all_vars = list(vars_json["variables"].keys())

        # Filter out non-estimate variables and special fields
        estimate_vars = [var for var in all_vars 
                        if var.endswith('E') and 
                        var not in {"state", "county", "tract", "block group", "for", "in"}]

        return estimate_vars
    except Exception as e:
        print(f"Error getting variables: {str(e)}")
        return None

def fetch_acs_batch(api_key, variables, state, county, year="2021", max_retries=5):
    """
    Fetch a batch of ACS variables for specified geography.
    """
    acs_base_url = f"https://api.census.gov/data/{year}/acs/acs5"

    params = {
        "get": f"NAME,{','.join(variables)}",
        "for": "block group:*",
        "in": f"state:{state} county:{county} tract:*",
        "key": api_key
    }

    for retry in range(max_retries):
        try:
            response = requests.get(acs_base_url, params=params)
            if response.status_code == 200:
                return response.json()
            time.sleep(2 ** retry)  # Exponential backoff
        except Exception as e:
            print(f"Attempt {retry + 1} failed: {str(e)}")
            time.sleep(2 ** retry)

    return None

def fetch_acs_data_for_county(api_key, county, variables, state="36", year="2021", batch_size=100):
    """
    Fetch ACS data for a county in batches.
    """
    print(f"Fetching ACS data for county {county}...")

    # Split variables into batches
    var_batches = [variables[i:i + batch_size] for i in range(0, len(variables), batch_size)]

    all_data = []
    for i, batch in enumerate(var_batches):
        print(f"Processing batch {i+1}/{len(var_batches)} for county {county}")
        batch_data = fetch_acs_batch(api_key, batch, state, county, year)

        if batch_data:
            if not all_data:
                all_data = batch_data  # First batch includes headers
            else:
                # Merge data columns from subsequent batches
                for row_idx in range(1, len(batch_data)):
                    all_data[row_idx].extend(batch_data[row_idx][1:-4])  # Exclude NAME and geo columns

        time.sleep(1)  # Rate limiting

    if all_data:
        df = pd.DataFrame(all_data[1:], columns=all_data[0])
        df["GEOID"] = df["state"].str.zfill(2) + \
                      df["county"].str.zfill(3) + \
                      df["tract"].str.zfill(6) + \
                      df["block group"]
        return df

    return None

def save_geojson(data, filename):
    """Save GeoDataFrame to GeoJSON file."""
    try:
        data.to_file(filename, driver="GeoJSON")
        print(f"Successfully saved data to {filename}")
        return True
    except Exception as e:
        print(f"Error saving file: {str(e)}")
        return False

if __name__ == "__main__":
    # Your Census API key
    api_key = "a3ebdf1648b7fb21df55df7246d9642f040c0ee0"

    # NYC counties
    nyc_counties = ["005", "047", "061", "081", "085"]

    # Download block group boundaries
    gdf_boundaries = download_nyc_block_groups()

    if gdf_boundaries is not None:
        # Get list of ACS variables
        variables = get_acs_variables(api_key)

        if variables:
            # Fetch ACS data for each county
            all_acs_data = []
            for county in nyc_counties:
                county_data = fetch_acs_data_for_county(api_key, county, variables)
                if county_data is not None:
                    all_acs_data.append(county_data)

            if all_acs_data:
                # Combine all county data
                df_acs = pd.concat(all_acs_data, ignore_index=True)

                # Merge boundaries with ACS data
                print("Merging spatial and ACS data...")
                merged_gdf = gdf_boundaries.merge(df_acs, on="GEOID", how="left")

                # Save the result
                output_file = "nyc_block_groups_complete.geojson"
                save_geojson(merged_gdf, output_file)

                # Print summary
                print(f"\nSummary:")
                print(f"Number of block groups: {len(merged_gdf)}")
                print(f"Number of variables: {len(merged_gdf.columns)}")
                print(f"Memory usage: {merged_gdf.memory_usage().sum() / 1024 / 1024:.2f} MB")
