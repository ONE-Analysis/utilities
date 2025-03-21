import os
import sys
import shutil
import subprocess
from tqdm import tqdm

def extract_lpkx(lpkx_path, extract_dir):
    """
    Attempts to extract the contents of the LPKX file using shutil.unpack_archive.
    If extraction fails, prints an error message and exits.
    """
    if not os.path.exists(lpkx_path):
        print("Error: LPKX file does not exist:", lpkx_path)
        sys.exit(1)
    if not os.path.exists(extract_dir):
        os.makedirs(extract_dir)
    
    try:
        # shutil.unpack_archive will try to detect the archive format
        shutil.unpack_archive(lpkx_path, extract_dir)
        print("Extraction complete. Files extracted to:", extract_dir)
    except Exception as e:
        print("Error during extraction:", e)
        print("The file may not be a valid archive or may require ArcGIS for extraction.")
        sys.exit(1)

def find_geodatabase(extracted_dir):
    """
    Recursively searches for a file geodatabase (folder ending in .gdb) in the extracted directory.
    """
    for root, dirs, files in os.walk(extracted_dir):
        for d in dirs:
            if d.lower().endswith('.gdb'):
                gdb_path = os.path.join(root, d)
                print("Found file geodatabase:", gdb_path)
                return gdb_path
    return None

def list_gdb_layers(gdb_path):
    """
    Lists layers in the geodatabase using ogrinfo.
    """
    try:
        result = subprocess.check_output(["ogrinfo", gdb_path], universal_newlines=True)
    except subprocess.CalledProcessError as e:
        print("Error running ogrinfo:", e)
        sys.exit(1)
    
    layers = []
    for line in result.splitlines():
        # Use a simple heuristic to capture layer names from the output.
        if ": " in line:
            parts = line.split(":")
            if len(parts) >= 2:
                layer_name = parts[1].split("(")[0].strip()
                if layer_name and layer_name not in layers:
                    layers.append(layer_name)
    return layers

def convert_layer_to_geojson(gdb_path, layer_name, output_geojson):
    """
    Converts a specified layer from the geodatabase to a GeoJSON file using ogr2ogr.
    """
    print(f"Converting layer '{layer_name}' to GeoJSON...")
    try:
        subprocess.check_call(["ogr2ogr", "-f", "GeoJSON", output_geojson, gdb_path, layer_name])
        print("GeoJSON created at:", output_geojson)
    except subprocess.CalledProcessError as e:
        print("Error during conversion:", e)
        sys.exit(1)

def process_lpkx(lpkx_file, input_dir, output_dir, temp_base_dir):
    """
    Processes a single .lpkx file:
      - Extracts it into a temporary folder
      - Finds the contained file geodatabase
      - Lists available layers and selects the first one
      - Converts that layer to GeoJSON with a matching name
      - Prints the head (first 10 lines) of the GeoJSON to verify the export
    """
    full_lpkx_path = os.path.join(input_dir, lpkx_file)
    dataset_name = os.path.splitext(lpkx_file)[0]
    
    # Create a dedicated temporary extraction directory for this file.
    extract_dir = os.path.join(temp_base_dir, dataset_name)
    if os.path.exists(extract_dir):
        shutil.rmtree(extract_dir)
    os.makedirs(extract_dir)
    
    # Extract the LPKX contents.
    extract_lpkx(full_lpkx_path, extract_dir)
    
    # Find the file geodatabase.
    gdb_path = find_geodatabase(extract_dir)
    if not gdb_path:
        print(f"No file geodatabase found in {lpkx_file}")
        return
    
    # List available layers in the geodatabase.
    layers = list_gdb_layers(gdb_path)
    if not layers:
        print(f"No layers found in the geodatabase of {lpkx_file}")
        return
    print("Layers found:", layers)
    layer_to_convert = layers[0]
    
    # Build the output file path with the same name as the input (but with .geojson extension).
    output_geojson = os.path.join(output_dir, f"{dataset_name}.geojson")
    convert_layer_to_geojson(gdb_path, layer_to_convert, output_geojson)
    
    # Print the head of the GeoJSON file to ensure export is successful.
    try:
        with open(output_geojson, 'r') as geojson_file:
            head = ''.join([geojson_file.readline() for _ in range(10)])
        print(f"Head of {output_geojson}:")
        print(head)
    except Exception as e:
        print("Error reading the GeoJSON file:", e)
    
    # Clean up the temporary extraction folder for this dataset.
    shutil.rmtree(extract_dir)

def main():
    # Define the input and output directories.
    input_dir = '/Users/oliveratwood/One Architecture Dropbox/_ONE LABS/[ Side Projects ]/ONE-Labs-Github/utilities/input'
    output_dir = '/Users/oliveratwood/One Architecture Dropbox/_ONE LABS/[ Side Projects ]/ONE-Labs-Github/utilities/output'
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Create a temporary base directory for extraction.
    temp_base_dir = os.path.join(output_dir, "temp_extraction")
    if not os.path.exists(temp_base_dir):
        os.makedirs(temp_base_dir)
    
    # Get a list of all .lpkx files in the input directory.
    lpkx_files = [f for f in os.listdir(input_dir) if f.lower().endswith('.lpkx')]
    if not lpkx_files:
        print("No .lpkx files found in the input directory.")
        return
    
    # Process each .lpkx file using a progress bar.
    for lpkx_file in tqdm(lpkx_files, desc="Processing files", unit="file"):
        try:
            process_lpkx(lpkx_file, input_dir, output_dir, temp_base_dir)
        except Exception as e:
            print(f"Error processing {lpkx_file}: {e}")
    
    # Remove the temporary base extraction directory after processing.
    if os.path.exists(temp_base_dir):
        shutil.rmtree(temp_base_dir)
    print("\nAll files processed.")

if __name__ == "__main__":
    main()