"""
API Downloader module using DuckDB HTTP client extension and fsspec.
Provides functionality to download data from various APIs and file systems.
"""
import duckdb
import fsspec
import pandas as pd
import geopandas as gpd
from typing import Optional, Dict, Any, List, Union
import logging
import json
import tempfile
from pathlib import Path
from urllib.parse import urlparse, parse_qs

from config import DataSource, BoundingBox

logger = logging.getLogger(__name__)


class APIDownloader:
    """Downloads data from APIs using DuckDB HTTP client and fsspec."""
    
    def __init__(self):
        self.connection: Optional[duckdb.DuckDBPyConnection] = None
        self._fsspec_cache = {}
        
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
        
    def connect(self):
        """Establish DuckDB connection with HTTP client extension."""
        try:
            self.connection = duckdb.connect(database=':memory:', read_only=False)
            
            # Install and load extensions
            extensions = ['httpfs', 'http_client', 'spatial', 'json']
            for ext in extensions:
                try:
                    self.connection.execute(f"INSTALL {ext};")
                    self.connection.execute(f"LOAD {ext};")
                except Exception as e:
                    logger.warning(f"Could not load extension {ext}: {e}")
                    
            logger.info("DuckDB connection established with HTTP client support")
            
        except Exception as e:
            logger.error(f"Failed to establish DuckDB connection: {e}")
            raise
            
    def disconnect(self):
        """Close DuckDB connection."""
        if self.connection:
            try:
                self.connection.close()
                logger.info("DuckDB connection closed")
            except Exception as e:
                logger.error(f"Error closing DuckDB connection: {e}")
                
    def download_with_http_client(self, url: str, headers: Optional[Dict[str, str]] = None,
                                 params: Optional[Dict[str, str]] = None) -> pd.DataFrame:
        """
        Download data using DuckDB's HTTP client extension.
        
        Args:
            url: API endpoint URL
            headers: Optional HTTP headers
            params: Optional query parameters
            
        Returns:
            DataFrame with downloaded data
        """
        if not self.connection:
            raise RuntimeError("No active DuckDB connection")
            
        try:
            # Build the HTTP request
            request_parts = [f"url := '{url}'"]
            
            if headers:
                headers_str = json.dumps(headers)
                request_parts.append(f"headers := '{headers_str}'::JSON")
                
            if params:
                params_str = json.dumps(params)
                request_parts.append(f"params := '{params_str}'::JSON")
                
            # Execute HTTP request using DuckDB
            query = f"""
            SELECT * FROM http_get({', '.join(request_parts)});
            """
            
            result = self.connection.execute(query).fetchdf()
            
            # Parse JSON response if applicable
            if 'body' in result.columns and result['body'].iloc[0]:
                try:
                    body = result['body'].iloc[0]
                    if isinstance(body, str):
                        data = json.loads(body)
                        if isinstance(data, list):
                            return pd.DataFrame(data)
                        elif isinstance(data, dict):
                            # Handle nested JSON structures
                            if 'features' in data:  # GeoJSON
                                return self._parse_geojson_response(data)
                            else:
                                return pd.DataFrame([data])
                except json.JSONDecodeError:
                    logger.warning("Response is not JSON, returning raw data")
                    
            return result
            
        except Exception as e:
            logger.error(f"Failed to download with HTTP client: {e}")
            raise
            
    def _parse_geojson_response(self, geojson_data: Dict) -> pd.DataFrame:
        """Parse GeoJSON response into DataFrame."""
        features = geojson_data.get('features', [])
        if not features:
            return pd.DataFrame()
            
        # Extract properties and geometries
        rows = []
        for feature in features:
            row = feature.get('properties', {}).copy()
            if 'geometry' in feature:
                row['geometry'] = json.dumps(feature['geometry'])
            rows.append(row)
            
        return pd.DataFrame(rows)
        
    def download_with_fsspec(self, url: str, storage_options: Optional[Dict[str, Any]] = None,
                           **kwargs) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
        """
        Download data using fsspec for various file systems.
        
        Supports: s3, gcs, azure, ftp, ssh, hdfs, and more.
        
        Args:
            url: File system URL (e.g., 's3://bucket/path', 'gcs://bucket/path')
            storage_options: Authentication and configuration options
            **kwargs: Additional arguments for reading the file
            
        Returns:
            DataFrame or GeoDataFrame with downloaded data
        """
        try:
            # Parse the URL to determine the file system
            parsed = urlparse(url)
            protocol = parsed.scheme or 'file'
            
            # Get or create filesystem instance
            fs_key = f"{protocol}_{hash(str(storage_options))}"
            if fs_key not in self._fsspec_cache:
                self._fsspec_cache[fs_key] = fsspec.filesystem(protocol, **(storage_options or {}))
            fs = self._fsspec_cache[fs_key]
            
            # Determine file type and read accordingly
            file_path = parsed.path
            if file_path.startswith('/'):
                file_path = file_path[1:]  # Remove leading slash
                
            file_ext = Path(file_path).suffix.lower()
            
            with fs.open(file_path, 'rb') as f:
                if file_ext in ['.geojson', '.json']:
                    # Read GeoJSON
                    data = json.load(f)
                    if 'features' in data:  # GeoJSON
                        return gpd.read_file(f, driver='GeoJSON')
                    else:
                        return pd.DataFrame(data)
                        
                elif file_ext == '.parquet':
                    return pd.read_parquet(f, **kwargs)
                    
                elif file_ext == '.csv':
                    return pd.read_csv(f, **kwargs)
                    
                elif file_ext in ['.shp', '.gpkg', '.gdb']:
                    # Spatial formats - need to save temporarily
                    with tempfile.NamedTemporaryFile(suffix=file_ext, delete=False) as tmp:
                        tmp.write(f.read())
                        tmp_path = tmp.name
                    try:
                        return gpd.read_file(tmp_path, **kwargs)
                    finally:
                        Path(tmp_path).unlink()
                        
                else:
                    # Try to read with pandas
                    return pd.read_csv(f, **kwargs)
                    
        except Exception as e:
            logger.error(f"Failed to download with fsspec: {e}")
            raise
            
    def download_spatial_api(self, api_config: Dict[str, Any], 
                           bbox: Optional[BoundingBox] = None) -> gpd.GeoDataFrame:
        """
        Download spatial data from common GIS APIs.
        
        Supports: ArcGIS REST, WFS, OGC API Features, etc.
        
        Args:
            api_config: API configuration including type, url, and parameters
            bbox: Optional bounding box filter
            
        Returns:
            GeoDataFrame with spatial data
        """
        api_type = api_config.get('type', '').lower()
        base_url = api_config.get('url')
        
        if api_type == 'arcgis':
            return self._download_arcgis_rest(base_url, api_config, bbox)
        elif api_type == 'wfs':
            return self._download_wfs(base_url, api_config, bbox)
        elif api_type == 'ogcapi':
            return self._download_ogc_api(base_url, api_config, bbox)
        else:
            # Generic API download
            params = api_config.get('params', {})
            if bbox:
                params.update(self._bbox_to_params(bbox, api_type))
                
            df = self.download_with_http_client(base_url, 
                                              headers=api_config.get('headers'),
                                              params=params)
            return self._convert_to_geodataframe(df, api_config)
            
    def _download_arcgis_rest(self, base_url: str, config: Dict, 
                            bbox: Optional[BoundingBox]) -> gpd.GeoDataFrame:
        """Download from ArcGIS REST API."""
        params = {
            'f': 'geojson',
            'outFields': '*',
            'where': '1=1',
            'returnGeometry': 'true'
        }
        
        if bbox:
            params['geometry'] = f"{bbox.min_lon},{bbox.min_lat},{bbox.max_lon},{bbox.max_lat}"
            params['geometryType'] = 'esriGeometryEnvelope'
            params['spatialRel'] = 'esriSpatialRelIntersects'
            
        # Add any custom parameters
        params.update(config.get('params', {}))
        
        # Handle pagination if needed
        all_features = []
        offset = 0
        limit = config.get('max_records', 1000)
        
        while True:
            params['resultOffset'] = offset
            params['resultRecordCount'] = limit
            
            df = self.download_with_http_client(f"{base_url}/query", params=params)
            
            if 'body' in df.columns:
                data = json.loads(df['body'].iloc[0])
                features = data.get('features', [])
                
                if not features:
                    break
                    
                all_features.extend(features)
                
                if len(features) < limit:
                    break
                    
                offset += limit
                
        # Create GeoDataFrame
        if all_features:
            geojson = {'type': 'FeatureCollection', 'features': all_features}
            return gpd.GeoDataFrame.from_features(geojson)
        else:
            return gpd.GeoDataFrame()
            
    def _download_wfs(self, base_url: str, config: Dict, 
                    bbox: Optional[BoundingBox]) -> gpd.GeoDataFrame:
        """Download from WFS service."""
        params = {
            'service': 'WFS',
            'version': config.get('version', '2.0.0'),
            'request': 'GetFeature',
            'typeName': config.get('layer_name'),
            'outputFormat': 'application/json'
        }
        
        if bbox:
            # WFS bbox format: minx,miny,maxx,maxy,CRS
            params['bbox'] = f"{bbox.min_lon},{bbox.min_lat},{bbox.max_lon},{bbox.max_lat},EPSG:4326"
            
        params.update(config.get('params', {}))
        
        df = self.download_with_http_client(base_url, params=params)
        
        if 'body' in df.columns:
            data = json.loads(df['body'].iloc[0])
            return gpd.GeoDataFrame.from_features(data.get('features', []))
        else:
            return gpd.GeoDataFrame()
            
    def _download_ogc_api(self, base_url: str, config: Dict,
                        bbox: Optional[BoundingBox]) -> gpd.GeoDataFrame:
        """Download from OGC API Features."""
        collection = config.get('collection')
        items_url = f"{base_url}/collections/{collection}/items"
        
        params = {
            'f': 'json',
            'limit': config.get('limit', 1000)
        }
        
        if bbox:
            params['bbox'] = f"{bbox.min_lon},{bbox.min_lat},{bbox.max_lon},{bbox.max_lat}"
            
        params.update(config.get('params', {}))
        
        # Handle pagination
        all_features = []
        next_url = items_url
        
        while next_url:
            df = self.download_with_http_client(next_url, params=params)
            
            if 'body' in df.columns:
                data = json.loads(df['body'].iloc[0])
                features = data.get('features', [])
                all_features.extend(features)
                
                # Check for next link
                links = data.get('links', [])
                next_link = next((link for link in links if link.get('rel') == 'next'), None)
                next_url = next_link.get('href') if next_link else None
                params = {}  # Clear params for next request
            else:
                break
                
        if all_features:
            return gpd.GeoDataFrame.from_features(all_features)
        else:
            return gpd.GeoDataFrame()
            
    def _bbox_to_params(self, bbox: BoundingBox, api_type: str) -> Dict[str, str]:
        """Convert bounding box to API-specific parameters."""
        if api_type in ['wfs', 'ogcapi']:
            return {'bbox': f"{bbox.min_lon},{bbox.min_lat},{bbox.max_lon},{bbox.max_lat}"}
        else:
            # Generic bbox parameters
            return {
                'minx': str(bbox.min_lon),
                'miny': str(bbox.min_lat),
                'maxx': str(bbox.max_lon),
                'maxy': str(bbox.max_lat)
            }
            
    def _convert_to_geodataframe(self, df: pd.DataFrame, config: Dict) -> gpd.GeoDataFrame:
        """Convert DataFrame to GeoDataFrame based on configuration."""
        geom_col = config.get('geometry_column', 'geometry')
        
        if geom_col in df.columns:
            # Parse geometry from JSON string
            from shapely import wkt, wkb
            import json
            
            geometries = []
            for geom in df[geom_col]:
                if isinstance(geom, str):
                    try:
                        # Try JSON first (GeoJSON geometry)
                        geom_dict = json.loads(geom)
                        from shapely.geometry import shape
                        geometries.append(shape(geom_dict))
                    except:
                        try:
                            # Try WKT
                            geometries.append(wkt.loads(geom))
                        except:
                            # Try WKB
                            geometries.append(wkb.loads(geom))
                else:
                    geometries.append(geom)
                    
            return gpd.GeoDataFrame(df, geometry=geometries)
        else:
            # Try to create from lat/lon columns
            lat_col = config.get('lat_column', 'lat')
            lon_col = config.get('lon_column', 'lon')
            
            if lat_col in df.columns and lon_col in df.columns:
                from shapely.geometry import Point
                geometry = [Point(xy) for xy in zip(df[lon_col], df[lat_col])]
                return gpd.GeoDataFrame(df, geometry=geometry)
            else:
                raise ValueError(f"Could not find geometry column '{geom_col}' or lat/lon columns")


# Example usage functions
def download_from_s3(bucket: str, key: str, aws_access_key: str, aws_secret_key: str) -> pd.DataFrame:
    """Download data from S3 using fsspec."""
    with APIDownloader() as downloader:
        return downloader.download_with_fsspec(
            f"s3://{bucket}/{key}",
            storage_options={
                'key': aws_access_key,
                'secret': aws_secret_key
            }
        )


def download_from_arcgis(service_url: str, layer_id: int, bbox: Optional[BoundingBox] = None) -> gpd.GeoDataFrame:
    """Download from ArcGIS REST service."""
    with APIDownloader() as downloader:
        return downloader.download_spatial_api({
            'type': 'arcgis',
            'url': f"{service_url}/{layer_id}",
            'params': {}
        }, bbox)


def download_from_wfs(wfs_url: str, layer_name: str, bbox: Optional[BoundingBox] = None) -> gpd.GeoDataFrame:
    """Download from WFS service."""
    with APIDownloader() as downloader:
        return downloader.download_spatial_api({
            'type': 'wfs',
            'url': wfs_url,
            'layer_name': layer_name,
            'version': '2.0.0'
        }, bbox)