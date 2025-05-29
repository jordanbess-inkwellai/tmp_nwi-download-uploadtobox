import duckdb
import os
import boxsdk

# Define the bounding box for Disney Animal Kingdom
bbox = [-81.61303476507055, 28.34441345722594, -81.56830935354668, 28.3768179430727]

# Construct the DuckDB query
# Note: You may need to adjust the layer name if the geodatabase has multiple layers
query = f"""
LOAD spatial;
LOAD httpfs;
LOAD zipfs;

COPY (
    SELECT *
    FROM duckdb_spatial.st_read('zip://https://documentst.ecosphere.fws.gov/wetlands/data/State-Downloads/FL_geodatabase_wetlands.zip!FL_geodatabase_wetlands.gdb',
                                bbox=[{bbox[0]}, {bbox[1]}, {bbox[2]}, {bbox[3]}])
) TO 'animal_kingdom_wetlands.csv' (HEADER, DELIMITER ',');
"""

# Connect to DuckDB and execute the query to save to CSV
con = duckdb.connect(database=':memory:', read_only=False)
con.execute(query)
con.close()

print("Data extracted and saved to animal_kingdom_wetlands.csv")

# Box.com Upload
# Read credentials from environment variables
BOX_CLIENT_ID = os.environ.get("BOX_CLIENT_ID")
BOX_CLIENT_SECRET = os.environ.get("BOX_CLIENT_SECRET")
BOX_ACCESS_TOKEN = os.environ.get("BOX_ACCESS_TOKEN") # Or other authentication method

# Replace with the ID of the Box folder where you want to upload the file
BOX_FOLDER_ID = os.environ.get("BOX_FOLDER_ID")


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

 file_path = "animal_kingdom_wetlands.csv"
 file_name = os.path.basename(file_path)

 new_file = client.folder(BOX_FOLDER_ID).upload(file_path, file_name)
 print(f"File '{file_name}' uploaded to Box folder ID '{BOX_FOLDER_ID}'")
 except Exception as e:
 print(f"Error uploading file to Box: {e}")

# Kestra Flow (YAML format)
# Save the following as a .yaml file
"""
id: duckdb-box-upload
namespace: dev

tasks:
  - id: run_python_script
    type: io.kestra.plugin.scripts.python.Commands
    commands:
      - pip install duckdb boxsdk  # Install necessary libraries
      - python your_script_name.py # Replace with the name of your Python script
"""