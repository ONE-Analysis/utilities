#!/usr/bin/env python
import requests
import time
import json

def download_nyc_census_tracts():
    """
    Download NYC census tract boundaries and demographic data.
    Returns a GeoJSON FeatureCollection with both geometry and census data.
    """
    # Base URL for TIGER web service
    url = "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_Current/MapServer/10/query"

    # NYC counties (boroughs)
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

    print("Downloading NYC census tract boundaries...")

    try:
        response = requests.get(url, params=params)
        if response.status_code != 200:
            raise Exception(f"Error retrieving TIGER data: {response.status_code}")

        geojson_data = response.json()
        feature_count = len(geojson_data.get("features", []))
        print(f"Retrieved {feature_count} census tracts")

        # Add borough names to properties
        for feature in geojson_data["features"]:
            props = feature["properties"]
            county_fips = props.get("COUNTY")
            if county_fips:
                props["borough"] = nyc_counties.get(county_fips, "Unknown")

        return geojson_data

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return None

def save_geojson(data, filename):
    """Save GeoJSON data to file."""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f)
        print(f"Successfully saved data to {filename}")
        return True
    except Exception as e:
        print(f"Error saving file: {str(e)}")
        return False

if __name__ == "__main__":
    output_file = "nyc_census_tracts_complete.geojson"

    # Download and save the data
    geojson_data = download_nyc_census_tracts()
    if geojson_data:
        save_geojson(geojson_data, output_file)
