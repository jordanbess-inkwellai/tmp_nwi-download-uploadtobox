# API Downloader Module Documentation

## Overview

The API Downloader module extends the DuckDB Spatial Data Extractor with capabilities to download data from various APIs and cloud storage systems. It leverages DuckDB's HTTP client extension and Python's fsspec library to provide a unified interface for accessing data from multiple sources.

## Features

### 1. DuckDB HTTP Client Integration
- Native HTTP requests within DuckDB queries
- JSON parsing support
- Efficient data loading directly into DuckDB tables

### 2. Fsspec File System Support
Supports various cloud and remote file systems:
- **S3**: Amazon S3 buckets
- **GCS**: Google Cloud Storage
- **Azure**: Azure Blob Storage and Data Lake
- **FTP/SFTP**: File Transfer Protocol
- **SSH**: Secure Shell file access
- **HDFS**: Hadoop Distributed File System
- **HTTP/HTTPS**: Web-based file access

### 3. Spatial API Support
Pre-configured support for common GIS APIs:
- **ArcGIS REST Services**: ESRI map and feature services
- **WFS (Web Feature Service)**: OGC standard for vector data
- **OGC API Features**: Modern OGC API standard
- **Custom APIs**: Flexible configuration for any JSON/GeoJSON API

## Installation

```bash
pip install -r requirements.txt
```

Key dependencies:
- `duckdb` with http_client extension
- `fsspec` and protocol-specific libraries (s3fs, gcsfs, etc.)
- `geopandas` for spatial data handling

## Usage Examples

### 1. Download from ArcGIS REST Service

```python
from api_downloader import download_from_arcgis
from config import BoundingBox

# Define area of interest
bbox = BoundingBox(
    min_lon=-81.5,
    min_lat=28.3,
    max_lon=-81.2,
    max_lat=28.6
)

# Download features
gdf = download_from_arcgis(
    service_url="https://services.arcgis.com/example/MapServer",
    layer_id=0,
    bbox=bbox
)

# Save to file
gdf.to_file("output.geojson", driver="GeoJSON")
```

### 2. Download from S3 with fsspec

```python
from api_downloader import APIDownloader

with APIDownloader() as downloader:
    # Download Parquet file from S3
    df = downloader.download_with_fsspec(
        "s3://my-bucket/data/spatial_data.parquet",
        storage_options={
            'key': 'AWS_ACCESS_KEY',
            'secret': 'AWS_SECRET_KEY'
        }
    )
```

### 3. Custom API with HTTP Client

```python
from api_downloader import APIDownloader

with APIDownloader() as downloader:
    # Download from custom API
    df = downloader.download_with_http_client(
        url="https://api.example.com/data",
        headers={"Authorization": "Bearer TOKEN"},
        params={"format": "geojson", "limit": 1000}
    )
```

### 4. Integrated with Data Extractor

```python
from config import DataSource, ExtractionConfig, BoundingBox
from data_extractor import DuckDBSpatialExtractor

# Configure API source
config = ExtractionConfig(
    job_name="api_extraction",
    data_source=DataSource(
        name="API Data",
        url="https://api.example.com/spatial/data"
    ),
    bounding_box=BoundingBox(-82, 28, -81, 29),
    metadata={
        'api_type': 'arcgis',
        'api_params': {'where': '1=1'}
    }
)

# Extract data
with DuckDBSpatialExtractor(config) as extractor:
    gdf = extractor.extract_to_geodataframe()
```

## Bounding Box Calculator

The `bbox_calculator.py` script helps calculate bounding boxes from existing spatial data:

### Command Line Usage

```bash
# Calculate bbox from GeoJSON
python bbox_calculator.py input.geojson

# Calculate from GeoPackage with specific layer
python bbox_calculator.py data.gpkg --layer buildings

# Filter features before calculation
python bbox_calculator.py data.geojson --filter '{"state": "FL"}'

# Use DuckDB for large files
python bbox_calculator.py large_data.gpkg --use-duckdb --where "population > 10000"

# Add buffer and save result
python bbox_calculator.py input.geojson --buffer 0.01 --save-to bbox.json
```

### Python API Usage

```python
from bbox_calculator import BBoxCalculator

calculator = BBoxCalculator()

# From GeoJSON
bbox = calculator.calculate_from_geojson("polygons.geojson")

# From GeoPackage with filter
bbox = calculator.calculate_from_geopackage(
    "data.gpkg",
    layer_name="parcels",
    feature_filter={"county": "Orange"}
)

# Expand bbox with buffer
bbox_buffered = calculator.expand_bbox(bbox, buffer_degrees=0.01)

# Calculate union of multiple bboxes
union_bbox = calculator.calculate_union([bbox1, bbox2, bbox3])
```

## Configuration

### API Types Configuration

The module supports different API types with specific configurations:

#### ArcGIS REST
```python
api_config = {
    'type': 'arcgis',
    'url': 'https://services.arcgis.com/example/MapServer/0',
    'params': {
        'where': '1=1',
        'outFields': '*'
    }
}
```

#### WFS
```python
api_config = {
    'type': 'wfs',
    'url': 'https://example.com/geoserver/wfs',
    'layer_name': 'workspace:layer',
    'version': '2.0.0'
}
```

#### OGC API Features
```python
api_config = {
    'type': 'ogcapi',
    'url': 'https://api.example.com',
    'collection': 'buildings',
    'limit': 1000
}
```

### Storage Options for fsspec

Different protocols require different authentication:

#### S3
```python
storage_options = {
    'key': 'AWS_ACCESS_KEY_ID',
    'secret': 'AWS_SECRET_ACCESS_KEY',
    'endpoint_url': 'https://s3.amazonaws.com'  # Optional
}
```

#### Google Cloud Storage
```python
storage_options = {
    'token': 'path/to/service-account.json',
    # or
    'project': 'my-project',
    'access': 'read_only'
}
```

#### Azure
```python
storage_options = {
    'account_name': 'myaccount',
    'account_key': 'mykey',
    # or
    'sas_token': 'my-sas-token'
}
```

## Error Handling

The module includes comprehensive error handling:

```python
try:
    gdf = download_from_arcgis(service_url, layer_id, bbox)
except ConnectionError as e:
    logger.error(f"Network error: {e}")
except ValueError as e:
    logger.error(f"Invalid parameters: {e}")
except Exception as e:
    logger.error(f"Unexpected error: {e}")
```

## Performance Considerations

1. **Use DuckDB for large datasets**: The `--use-duckdb` flag in bbox_calculator is faster for large files
2. **Implement pagination**: The module automatically handles pagination for supported APIs
3. **Cache fsspec filesystems**: Filesystem instances are cached to avoid repeated authentication
4. **Streaming downloads**: Large files are streamed rather than loaded entirely into memory

## Extending the Module

### Adding New API Types

Create a new method in `APIDownloader`:

```python
def _download_custom_api(self, base_url: str, config: Dict, 
                        bbox: Optional[BoundingBox]) -> gpd.GeoDataFrame:
    """Download from custom API."""
    # Implement custom logic
    params = self._build_custom_params(config, bbox)
    df = self.download_with_http_client(base_url, params=params)
    return self._convert_to_geodataframe(df, config)
```

### Adding New Storage Protocols

Fsspec automatically supports new protocols when their libraries are installed:

```bash
pip install s3fs  # For S3
pip install gcsfs  # For Google Cloud
pip install adlfs  # For Azure
pip install sshfs  # For SSH/SFTP
```

## Best Practices

1. **Always use context managers** to ensure proper connection cleanup
2. **Specify bounding boxes** to limit data downloads
3. **Use appropriate output formats** for your use case
4. **Cache API responses** when developing/testing
5. **Handle rate limits** with retry logic
6. **Validate SSL certificates** in production

## Troubleshooting

### Common Issues

1. **SSL Certificate Errors**
   ```python
   # Disable SSL verification (development only!)
   storage_options = {'verify': False}
   ```

2. **Authentication Failures**
   - Check credentials are properly set
   - Verify API keys/tokens are valid
   - Ensure proper permissions for cloud storage

3. **Memory Issues with Large Downloads**
   - Use streaming/chunked downloads
   - Process data in batches
   - Increase DuckDB memory limits

4. **Geometry Parsing Errors**
   - Verify geometry column name
   - Check coordinate system
   - Validate geometry data format

## Examples Directory

See the `examples/api_download_examples.py` file for complete working examples of:
- ArcGIS REST service downloads
- WFS service integration
- S3/cloud storage access
- Custom API integration
- Bounding box calculation workflows