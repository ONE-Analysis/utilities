import geopandas as gpd
from pathlib import Path

def convert_geojsons_to_shapefiles(input_folder):
    # Create input folder path object
    input_path = Path(input_folder)
    
    # Create shapefiles subfolder if it doesn't exist
    output_path = input_path / 'shapefiles'
    output_path.mkdir(exist_ok=True)
    
    # Get all geojson files in the input folder
    geojson_files = list(input_path.glob('*.geojson'))
    
    for geojson_file in geojson_files:
        # Read the geojson
        gdf = gpd.read_file(geojson_file)
        
        # Create output shapefile path with same name as geojson
        shapefile_path = output_path / f"{geojson_file.stem}.shp"
        
        # Save as shapefile
        gdf.to_file(shapefile_path)
        print(f"Converted {geojson_file.name} to {shapefile_path.name}")

# Usage example
input_folder = '/Users/oliveratwood/One Architecture Dropbox/_ONE LABS/[ Side Projects ]/ONE-Labs-Github/ONE-Labs/OUTPUTS'