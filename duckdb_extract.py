import duckdb
import os
import boxsdk
import geopandas as gpd
from shapely.geometry import box
import json # Import the json module

# Define the bounding box for Disney Animal Kingdom and create a shapely box geometry
bbox_coords = [-81.61303476507055, 28.34441345722594, -81.56830935354668, 28.3768179430727]
bbox_geom = box(bbox_coords[0], bbox_coords[1], bbox_coords[2], bbox_coords[3])

# Construct the DuckDB query
# Note: You may need to adjust the layer name if the geodatabase has multiple layers
query = f"""
LOAD spatial;
LOAD httpfs;
LOAD zipfs;

SELECT * -- Select all columns, including geometry
FROM duckdb_spatial.st_read('zip://https://documentst.ecosphere.fws.gov/wetlands/data/State-Downloads/FL_geodatabase_wetlands.zip!FL_geodatabase_wetlands.gdb',
 bbox=[{bbox_coords[0]}, {bbox_coords[1]}, {bbox_coords[2]}, {bbox_coords[3]}]);
"""

# Connect to DuckDB and execute the query
con = duckdb.connect(database=':memory:', read_only=False)
# Fetch the results into a DataFrame
df = con.fetchdf()

# Convert to GeoDataFrame
# Assuming the geometry column from DuckDB is named 'geom' and is in WKB format
# You might need to adjust the column name based on the actual geodatabase structure
# Assuming the geometry column from DuckDB is named 'geom' and is in WKB format
# You might need to adjust the column name based on the actual geodatabase structure
try:
    from shapely import wkb
    gdf = gpd.GeoDataFrame(df, geometry=df['geom'].apply(wkb.loads))
except Exception as e:
    print(f"Error converting to GeoDataFrame. Check the geometry column name and format: {e}")
    gdf = None


if gdf is not None and not gdf.empty:
    # Save as GeoJSON
    geojson_output_path = "animal_kingdom_wetlands.geojson"
    try:
        gdf.to_file(geojson_output_path, driver="GeoJSON")
        print(f"Data saved to {geojson_output_path}")
    except Exception as e:
        print(f"Error saving to GeoJSON: {e}")

    # Save as FileGDB
    # Note: Writing to FileGDB requires the 'FileGDB' driver to be available to GDAL.
    # This might require installing the ESRI File Geodatabase API or using a GDAL build
    # that includes FileGDB support. Conda installations of geopandas often include this.
    filegdb_output_path = "animal_kingdom_wetlands.gdb"
    try:
        # Create a directory for the FileGDB if it doesn't exist
        if not os.path.exists(filegdb_output_path):
            os.makedirs(filegdb_output_path)
        gdf.to_file(filegdb_output_path, driver="FileGDB", layer="wetlands") # Specify a layer name
        print(f"Data saved to {filegdb_output_path}")
    except Exception as e:
        print(f"Error saving to FileGDB. Ensure the 'FileGDB' driver is available: {e}")

else:
    print("No data extracted or conversion to GeoDataFrame failed.")


# Box.com Upload
# Read credentials from environment variables
BOX_CLIENT_ID = os.environ.get("BOX_CLIENT_ID")
BOX_CLIENT_SECRET = os.environ.get("BOX_CLIENT_SECRET")
BOX_ACCESS_TOKEN = os.environ.get("BOX_ACCESS_TOKEN") # Or other authentication method

# Replace with the ID of the Box folder where you want to upload the file
BOX_FOLDER_ID = os.environ.get("BOX_FOLDER_ID")

# Get metadata from environment variable
# The BOX_METADATA environment variable should contain a JSON string, e.g.,
# '{"project_name": "Animal Kingdom Wetlands", "data_source": "FWS", "extraction_date": "YYYY-MM-DD"}'
# You also need a corresponding metadata template defined in your Box account.
# Replace 'YOUR_METADATA_TEMPLATE_KEY' with the actual key of your Box metadata template.
BOX_METADATA_JSON = os.environ.get("BOX_METADATA")
metadata = {}
if BOX_METADATA_JSON:
    try:
        metadata = json.loads(BOX_METADATA_JSON)
        print("Metadata loaded from environment variable.")
    except json.JSONDecodeError as e:
        print(f"Error decoding BOX_METADATA JSON: {e}")
        metadata = {} # Reset metadata if decoding fails



if not all([BOX_CLIENT_ID, BOX_CLIENT_SECRET, BOX_ACCESS_TOKEN, BOX_FOLDER_ID]):
    print("Box.com credentials or folder ID not found in environment variables. Skipping upload.")
else:
    try:
        # Configure the Box SDK with your credentials
        oauth2 = boxsdk.OAuth2(
            client_id=BOX_CLIENT_ID,
            client_secret=BOX_CLIENT_SECRET,
            access_token=BOX_ACCESS_TOKEN,
        )
        client = boxsdk.Client(oauth2)

        # Upload GeoJSON
        geojson_file_path = "animal_kingdom_wetlands.geojson"
        if os.path.exists(geojson_file_path):
            geojson_file_name = os.path.basename(geojson_file_path)
            try:
                new_geojson_file = client.folder(BOX_FOLDER_ID).upload(geojson_file_path, geojson_file_name)
                print(f"File '{geojson_file_name}' uploaded to Box folder ID '{BOX_FOLDER_ID}'")

                # Apply metadata (replace 'YOUR_METADATA_TEMPLATE_KEY' with your actual template key)
                if metadata:
                    try:
                        new_geojson_file.metadata.create('enterprise', 'YOUR_METADATA_TEMPLATE_KEY', metadata)
                        print(f"Metadata applied to '{geojson_file_name}'")
                    except Exception as e:
                        print(f"Error applying metadata to GeoJSON file: {e}")
            except Exception as e:
                print(f"Error uploading GeoJSON file to Box: {e}")

        # Upload FileGDB (as a zip file, as Box might not handle GDB directories directly)
        # This requires zipping the .gdb directory before uploading
        import shutil
        filegdb_output_path = "animal_kingdom_wetlands.gdb" # The directory created for the GDB
        filegdb_zip_path = "animal_kingdom_wetlands.gdb.zip"
        if os.path.exists(filegdb_output_path):
            try:
                shutil.make_archive("animal_kingdom_wetlands.gdb", 'zip', filegdb_output_path) # Create the zip
                filegdb_zip_name = os.path.basename(filegdb_zip_path)
                new_filegdb_file = client.folder(BOX_FOLDER_ID).upload(filegdb_zip_path, filegdb_zip_name)
                print(f"File '{filegdb_zip_name}' uploaded to Box folder ID '{BOX_FOLDER_ID}'")
            except Exception as e:
                # Apply metadata (replace 'YOUR_METADATA_TEMPLATE_KEY' with your actual template key)
                if metadata:
                    try:
                        new_filegdb_file.metadata.create('enterprise', 'YOUR_METADATA_TEMPLATE_KEY', metadata)
                        print(f"Metadata applied to '{filegdb_zip_name}'")
                    except Exception as e:
                        print(f"Error applying metadata to FileGDB zip file: {e}")
                print(f"Error uploading FileGDB zip file to Box: {e}")
            finally:
                # Clean up the created zip file
                if os.path.exists(filegdb_zip_path):
                    os.remove(filegdb_zip_path)

    except Exception as e:
        print(f"Error during Box upload process: {e}")
