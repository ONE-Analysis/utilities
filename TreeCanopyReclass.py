import rasterio
import numpy as np
from tqdm import tqdm
import os
import shutil

def estimate_output_size(width, height, compression_factor=0.3):
    """Estimate output size in GB, assuming 30% of uncompressed size with LZW"""
    uncompressed_size = width * height  # 1 byte per pixel for uint8
    compressed_size = uncompressed_size * compression_factor
    return compressed_size / (1024**3)  # Convert to GB

# Read the input raster
input_path = "/Users/oliveratwood/One Architecture Dropbox/_ONE LABS/[ Side Projects ]/ONE-Labs-Github/streets/input/landcover_nyc_2021_6in.tif"
output_path = "/Users/oliveratwood/One Architecture Dropbox/_ONE LABS/[ Side Projects ]/ONE-Labs-Github/streets/input/NYC_TreeCanopy.tif"

# Check available disk space
output_dir = os.path.dirname(output_path)
if not os.path.exists(output_dir):
    os.makedirs(output_dir)  # Create the directory if it doesn't exist
free_space = shutil.disk_usage(output_dir).free / (1024**3)  # Convert to GB

print(f"Available disk space: {free_space:.1f} GB")

# Use a large chunk size but not the entire raster
CHUNK_SIZE = 25000  # This should use about 25GB of RAM per chunk

with rasterio.open(input_path) as src:
    estimated_size = estimate_output_size(src.width, src.height)
    print(f"Estimated output size: {estimated_size:.1f} GB")
    
    if estimated_size > free_space * 0.8:  # Leave 20% buffer
        print(f"WARNING: This operation might require more disk space than available!")
        response = input("Do you want to continue anyway? (y/n): ")
        if response.lower() != 'y':
            print("Operation cancelled")
            exit()
    
    # Modify the profile for GeoTIFF output with maximum compression
    profile = src.profile.copy()
    profile.update({
        'driver': 'GTiff',
        'compress': 'lzw',
        'tiled': True,
        'blockxsize': 256,
        'blockysize': 256,
        'zlevel': 9,  # Maximum compression
        'dtype': 'uint8'  # Using uint8 since we only need 0 and 1
    })
    
    # Calculate total number of chunks
    total_chunks = ((src.height + CHUNK_SIZE - 1) // CHUNK_SIZE) * \
                   ((src.width + CHUNK_SIZE - 1) // CHUNK_SIZE)
    
    print(f"\nProcessing raster in {total_chunks} chunks...")
    
    with rasterio.open(output_path, 'w', **profile) as dst:
        with tqdm(total=total_chunks, desc="Processing") as pbar:
            for j in range(0, src.height, CHUNK_SIZE):
                for i in range(0, src.width, CHUNK_SIZE):
                    # Calculate the size of this chunk
                    win_width = min(CHUNK_SIZE, src.width - i)
                    win_height = min(CHUNK_SIZE, src.height - j)
                    window = rasterio.windows.Window(i, j, win_width, win_height)
                    
                    # Read and process chunk
                    data = src.read(1, window=window)
                    reclass = np.where(data == 1, 1, 0).astype('uint8')
                    
                    # Write chunk
                    dst.write(reclass, 1, window=window)
                    
                    pbar.update(1)

print(f"\nReclassified raster saved to: {output_path}")

# Print raster info
with rasterio.open(output_path) as dst:
    print(f"Output raster dimensions: {dst.width}x{dst.height}")
    print(f"Output raster data type: {dst.dtypes[0]}")
    actual_size = os.path.getsize(output_path) / (1024**3)
    print(f"Actual output file size: {actual_size:.1f} GB")