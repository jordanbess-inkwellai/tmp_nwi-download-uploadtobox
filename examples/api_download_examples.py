"""
Examples of using the API downloader module.
"""
import sys
sys.path.append('..')

from api_downloader import APIDownloader, download_from_arcgis, download_from_wfs
from bbox_calculator import BBoxCalculator
from config import BoundingBox, DataSource, ExtractionConfig
import logging

logging.basicConfig(level=logging.INFO)


def example_arcgis_download():
    """Example: Download from ArcGIS REST service."""
    print("\n=== ArcGIS REST Service Example ===")
    
    # Florida DEP Water Resources
    service_url = "https://ca.dep.state.fl.us/arcgis/rest/services/OpenData/WATER_RESOURCES/MapServer"
    layer_id = 0  # Usually represents a specific layer
    
    # Define a bounding box (e.g., around Orlando)
    bbox = BoundingBox(
        min_lon=-81.5,
        min_lat=28.3,
        max_lon=-81.2,
        max_lat=28.6
    )
    
    try:
        gdf = download_from_arcgis(service_url, layer_id, bbox)
        print(f"Downloaded {len(gdf)} features")
        print(f"Columns: {list(gdf.columns)}")
        
        # Save to file
        gdf.to_file("arcgis_water_resources.geojson", driver="GeoJSON")
        print("Saved to arcgis_water_resources.geojson")
        
    except Exception as e:
        print(f"Error: {e}")


def example_wfs_download():
    """Example: Download from WFS service."""
    print("\n=== WFS Service Example ===")
    
    # Example WFS service (USGS)
    wfs_url = "https://mrdata.usgs.gov/services/sgmc"
    layer_name = "sgmc:sgmc_geology"
    
    # Define a small bounding box
    bbox = BoundingBox(
        min_lon=-82.0,
        min_lat=28.0,
        max_lon=-81.0,
        max_lat=29.0
    )
    
    try:
        gdf = download_from_wfs(wfs_url, layer_name, bbox)
        print(f"Downloaded {len(gdf)} features")
        print(f"Columns: {list(gdf.columns)}")
        
    except Exception as e:
        print(f"Error: {e}")


def example_s3_download():
    """Example: Download from S3 using fsspec."""
    print("\n=== S3 Download Example ===")
    
    with APIDownloader() as downloader:
        # Example S3 URL (requires credentials)
        s3_url = "s3://my-bucket/data/florida_wetlands.parquet"
        
        try:
            # Note: You need to set AWS credentials
            df = downloader.download_with_fsspec(
                s3_url,
                storage_options={
                    'key': 'YOUR_AWS_ACCESS_KEY',
                    'secret': 'YOUR_AWS_SECRET_KEY'
                }
            )
            print(f"Downloaded {len(df)} rows from S3")
            
        except Exception as e:
            print(f"Error (expected without valid credentials): {e}")


def example_ogc_api_features():
    """Example: Download from OGC API Features."""
    print("\n=== OGC API Features Example ===")
    
    with APIDownloader() as downloader:
        # Example OGC API (OS UK)
        api_config = {
            'type': 'ogcapi',
            'url': 'https://api.os.uk/features/v1',
            'collection': 'bld-fts-buildingpart',
            'params': {
                'limit': 100
            }
        }
        
        # Small bbox in UK
        bbox = BoundingBox(
            min_lon=-0.2,
            min_lat=51.4,
            max_lon=-0.1,
            max_lat=51.5
        )
        
        try:
            gdf = downloader.download_spatial_api(api_config, bbox)
            print(f"Downloaded {len(gdf)} features")
            
        except Exception as e:
            print(f"Error: {e}")


def example_custom_api():
    """Example: Download from custom API with HTTP client."""
    print("\n=== Custom API Example ===")
    
    with APIDownloader() as downloader:
        # Example: OpenWeatherMap API (requires API key)
        api_url = "https://api.openweathermap.org/data/2.5/box/city"
        
        bbox = BoundingBox(
            min_lon=-82.0,
            min_lat=28.0,
            max_lon=-81.0,
            max_lat=29.0
        )
        
        params = {
            'bbox': f"{bbox.min_lon},{bbox.min_lat},{bbox.max_lon},{bbox.max_lat},10",
            'appid': 'YOUR_API_KEY'  # Replace with actual API key
        }
        
        try:
            df = downloader.download_with_http_client(api_url, params=params)
            print(f"Downloaded data: {df.shape}")
            
        except Exception as e:
            print(f"Error (expected without valid API key): {e}")


def example_calculate_bbox_then_download():
    """Example: Calculate bbox from local file, then use for API download."""
    print("\n=== Calculate BBox then Download Example ===")
    
    # First, create a sample GeoJSON file
    import json
    from shapely.geometry import Polygon
    
    # Create a polygon around Disney World
    polygon = {
        "type": "Feature",
        "properties": {"name": "Disney World Area"},
        "geometry": {
            "type": "Polygon",
            "coordinates": [[
                [-81.61, 28.34],
                [-81.56, 28.34],
                [-81.56, 28.38],
                [-81.61, 28.38],
                [-81.61, 28.34]
            ]]
        }
    }
    
    geojson = {
        "type": "FeatureCollection",
        "features": [polygon]
    }
    
    # Save to file
    with open("disney_area.geojson", "w") as f:
        json.dump(geojson, f)
    
    # Calculate bbox from the polygon
    calculator = BBoxCalculator()
    bbox = calculator.calculate_from_geojson("disney_area.geojson")
    print(f"Calculated bbox: {bbox}")
    
    # Add a small buffer
    bbox = calculator.expand_bbox(bbox, 0.01)  # ~1km buffer
    print(f"Expanded bbox: {bbox}")
    
    # Now use this bbox to download data
    # Example: Download from a hypothetical API
    print("\nUsing calculated bbox for API download...")
    
    # Clean up
    import os
    os.remove("disney_area.geojson")


def example_integrated_extraction():
    """Example: Use the integrated extraction with API sources."""
    print("\n=== Integrated Extraction Example ===")
    
    # Create a data source that points to an API
    api_source = DataSource(
        name="USGS Water Data",
        url="https://waterservices.usgs.gov/nwis/site/",
        description="USGS Water Information System"
    )
    
    # Create extraction config with API metadata
    config = ExtractionConfig(
        job_name="water_sites_extraction",
        data_source=api_source,
        bounding_box=BoundingBox(
            min_lon=-82.0,
            min_lat=28.0,
            max_lon=-81.0,
            max_lat=29.0
        ),
        output_formats=["geojson", "csv"],
        metadata={
            'api_type': 'generic',
            'api_params': {
                'format': 'json',
                'siteType': 'ST',  # Stream sites
                'siteStatus': 'active'
            }
        }
    )
    
    print(f"Extraction config created for: {config.data_source.name}")
    print(f"BBox: {config.bounding_box}")
    print(f"API params: {config.metadata.get('api_params')}")


if __name__ == "__main__":
    # Run examples
    example_arcgis_download()
    example_wfs_download()
    example_s3_download()
    example_ogc_api_features()
    example_custom_api()
    example_calculate_bbox_then_download()
    example_integrated_extraction()
    
    print("\n=== Examples completed ===")
    print("Note: Some examples may fail without valid API keys or credentials.")