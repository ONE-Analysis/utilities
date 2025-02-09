import geopandas as gpd
from pathlib import Path

def convert_geojsons_to_shapefiles(input_folder):
    # Create input folder path object
    input_path = Path(input_folder)

    # Verify input path exists
    if not input_path.exists():
        print(f"Input folder does not exist: {input_path}")
        return

    # Create shapefiles subfolder if it doesn't exist
    output_path = input_path / 'shapefiles'
    output_path.mkdir(exist_ok=True)

    # Get all geojson files in the input folder
    geojson_files = list(input_path.glob('*.geojson'))

    # Print number of files found
    print(f"Found {len(geojson_files)} .geojson files in {input_path}")

    # Print list of found files
    if geojson_files:
        print("Files found:")
        for file in geojson_files:
            print(f"- {file.name}")

    for geojson_file in geojson_files:
        try:
            # Read the geojson
            gdf = gpd.read_file(geojson_file)

            # Create output shapefile path with same name as geojson
            shapefile_path = output_path / f"{geojson_file.stem}.shp"

            # Save as shapefile
            gdf.to_file(shapefile_path)
            print(f"Converted {geojson_file.name} to {shapefile_path.name}")
        except Exception as e:
            print(f"Error processing {geojson_file.name}: {str(e)}")

# Usage example
input_folder = '/Users/oliveratwood/One Architecture Dropbox/_ONE LABS/[ Side Projects ]/ONE-Labs-Github/OUTPUTS'
convert_geojsons_to_shapefiles(input_folder)
