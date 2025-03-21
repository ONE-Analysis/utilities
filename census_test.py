#!/usr/bin/env python
import requests
import time
import json

def download_nyc_census_tracts():
    """
    Download NYC census tract polygons from the TIGERweb/Tracts_Blocks service.
    
    This script queries layer 10 (Census Tracts (10)) using an attribute-based query:
       STATEFP = '36' AND COUNTYFP IN ('005','047','061','081','085')
    
    No API key is required.
    
    Returns:
        A Python dictionary representing a GeoJSON FeatureCollection.
    """
    # Use layer 10 (Census Tracts (10)); change this number if needed.
    url = "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/Tracts_Blocks/MapServer/10/query"
    
    # Build the WHERE clause in SQL syntax.
    where_clause = "STATEFP='36' AND COUNTYFP IN ('005','047','061','081','085')"
    
    # Set up query parameters per ArcGIS REST API guidelines.
    params = {
        "where": where_clause,
        "outFields": "*",           # Return all fields
        "returnGeometry": "true",   # Return the geometry with the features
        "f": "geojson",             # Return format is GeoJSON
        "resultOffset": 0,          # Paging: start at offset 0
        "resultRecordCount": 1000   # Maximum number of records per page (adjust if needed)
    }
    
    features = []
    while True:
        response = requests.get(url, params=params)
        if response.status_code != 200:
            raise Exception(f"Error retrieving TIGER data: {response.status_code} {response.text}")
        data = response.json()
        batch = data.get("features", [])
        if not batch:
            break
        features.extend(batch)
        print(f"Retrieved {len(batch)} features (offset {params['resultOffset']})")
        # If the number of returned features is less than requested, we've reached the last page.
        if len(batch) < params["resultRecordCount"]:
            break
        params["resultOffset"] += params["resultRecordCount"]
        time.sleep(0.5)  # Pause to be polite to the server.
    
    print(f"Total TIGER tract features downloaded: {len(features)}")
    # Assemble the features into a GeoJSON FeatureCollection.
    geojson = {
        "type": "FeatureCollection",
        "features": features
    }
    return geojson

if __name__ == '__main__':
    try:
        geojson_data = download_nyc_census_tracts()
        output_filename = "nyc_census_tracts.geojson"
        with open(output_filename, "w") as f:
            json.dump(geojson_data, f)
        print(f"Saved TIGER tract polygons to {output_filename}")
    except Exception as e:
        print("An error occurred:", e)