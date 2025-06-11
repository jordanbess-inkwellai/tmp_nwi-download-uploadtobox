# DuckDB Spatial Data Extractor with Box Upload
this code has been migrated to https://github.com/aiinkwell/ecco-geospatialdata_integration_review_analysis-services_fastapi/tree/main/app/services/vector_geospatial_data_downloader
A production-ready Python application for extracting spatial data using DuckDB and uploading results to Box.com with metadata support.

## Features

- **Flexible Data Sources**: Support for various spatial data formats (GeoDatabase, Shapefile, GeoJSON, etc.)
- **API Integration**: Download from REST APIs, WFS, OGC API Features using DuckDB HTTP client
- **Cloud Storage Support**: Access data from S3, GCS, Azure, FTP via fsspec
- **DuckDB-powered**: Fast spatial queries with bounding box filtering
- **Multiple Export Formats**: GeoJSON, FileGDB, Shapefile, GeoPackage, CSV, Parquet
- **Box.com Integration**: Automatic upload with metadata support
- **Bounding Box Calculator**: Calculate bbox from polygon features (GeoJSON, GPKG, etc.)
- **Configuration-driven**: JSON configuration files or command-line arguments
- **Production-ready**: Comprehensive error handling, logging, and retry mechanisms
- **Extensible**: Easy to add new data sources and locations

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd duckdb-spatial-extractor

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## New Features

### API Downloader Module
Download spatial data from various APIs and cloud storage:
- **DuckDB HTTP Client**: Native HTTP requests within DuckDB
- **Fsspec Integration**: Support for S3, GCS, Azure, FTP, SSH, HDFS
- **Spatial APIs**: Pre-configured support for ArcGIS REST, WFS, OGC API Features

See [API Downloader Documentation](docs/API_DOWNLOADER_README.md) for details.

### Bounding Box Calculator
Calculate bounding boxes from polygon features:
```bash
# From GeoJSON
python bbox_calculator.py input.geojson

# From GeoPackage with filtering
python bbox_calculator.py data.gpkg --layer buildings --where "height > 50"

# With buffer and JSON output
python bbox_calculator.py area.geojson --buffer 0.01 --output-format json
```stall -r requirements.txt
```

## Configuration

### Environment Variables

Copy `.env.example` to `.env` and fill in your Box.com credentials:

```bash
cp .env.example .env
```

### Data Sources and Locations

Edit `config.py` to add custom data sources and predefined locations:

```python
DATA_SOURCES = {
    "your_source": DataSource(
        name="Your Data Source",
        url="https://example.com/data.zip",
        geometry_column="geometry"
    )
}

LOCATIONS = {
    "your_location": BoundingBox(
        min_lon=-180.0,
        min_lat=-90.0,
        max_lon=180.0,
        max_lat=90.0
    )
}
```

## Usage

### Command Line Interface

```bash
# Using predefined source and location
python main.py --source fws_wetlands_fl --location disney_animal_kingdom

# Using custom bounding box
python main.py --source fws_wetlands_fl --bbox -81.61 28.34 -81.56 28.37

# Using configuration file
python main.py --config example_config.json

# With Box upload
python main.py --config example_config.json --upload-to-box

# Specify output formats
python main.py --source fws_wetlands_fl --location disney_animal_kingdom \
    --formats geojson gpkg --output-dir ./output

# Dry run to preview configuration
python main.py --config example_config.json --dry-run
```

### Configuration File

Create a JSON configuration file:

```json
{
  "job_name": "wetlands_extraction",
  "data_source": "fws_wetlands_fl",
  "bounding_box": {
    "min_lon": -81.61,
    "min_lat": 28.34,
    "max_lon": -81.56,
    "max_lat": 28.37
  },
  "output_formats": ["geojson", "filegdb"],
  "output_prefix": "wetlands",
  "metadata": {
    "project": "Environmental Assessment",
    "date": "2024-01-01"
  }
}
```

### Python API

```python
from config import ExtractionConfig, BoundingBox, DataSource
from data_extractor import DuckDBSpatialExtractor, DataExporter
from box_uploader import BoxUploader, BoxConfig

# Create configuration
config = ExtractionConfig(
    job_name="my_extraction",
    data_source=DataSource(
        name="My Source",
        url="https://example.com/data.zip",
        geometry_column="geom"
    ),
    bounding_box=BoundingBox(-81.61, 28.34, -81.56, 28.37),
    output_formats=["geojson", "gpkg"]
)

# Extract data
with DuckDBSpatialExtractor(config) as extractor:
    gdf = extractor.extract_to_geodataframe()
    
    # Export to files
    exporter = DataExporter(gdf)
    results = exporter.export_multiple("./output", "my_data", ["geojson", "gpkg"])
    
    # Upload to Box
    box_config = BoxConfig.from_env()
    if box_config:
        with BoxUploader(box_config) as uploader:
            upload_results = uploader.upload_multiple(results)
```

## Kestra Integration

Use the provided `duckdb_box_upload.yaml` for Kestra workflow automation:

```yaml
id: duckdb-spatial-extraction
namespace: production

inputs:
  - name: config_file
    type: STRING
    defaults: example_config.json

tasks:
  - id: extract_and_upload
    type: io.kestra.plugin.scripts.python.Commands
    env:
      BOX_CLIENT_ID: "{{ secret('BOX_CLIENT_ID') }}"
      BOX_CLIENT_SECRET: "{{ secret('BOX_CLIENT_SECRET') }}"
      BOX_ACCESS_TOKEN: "{{ secret('BOX_ACCESS_TOKEN') }}"
      BOX_FOLDER_ID: "{{ secret('BOX_FOLDER_ID') }}"
    commands:
      - pip install -r requirements.txt
      - python main.py --config {{ inputs.config_file }} --upload-to-box
```

## Error Handling

The application includes comprehensive error handling:

- Network timeouts and retries for data downloads
- Graceful handling of missing geometry columns
- Fallback options for FileGDB export (FileGDB → OpenFileGDB)
- Detailed logging for debugging
- Transaction rollback on failures

## Logging

Logs are written to both console and file:
- Console: INFO level and above
- File: `extraction_YYYYMMDD_HHMMSS.log` with all levels

## Performance Considerations

- DuckDB uses memory-efficient streaming for large datasets
- Bounding box filtering reduces data transfer
- Parallel processing for multiple format exports
- Chunked uploads for large files to Box

## Troubleshooting

### FileGDB Export Issues

If FileGDB export fails, ensure GDAL is properly installed:

```bash
# Using conda
conda install -c conda-forge gdal

# Or check GDAL drivers
python -c "from osgeo import ogr; print([ogr.GetDriver(i).GetName() for i in range(ogr.GetDriverCount())])"
```

### Box Upload Issues

- Verify environment variables are set correctly
- Check Box app permissions and folder access
- Ensure metadata template exists if using metadata

### Memory Issues

For large datasets, increase DuckDB memory limit:

```python
con = duckdb.connect(database=':memory:', config={'memory_limit': '4GB'})
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## License

[Your License Here]
