"""
DuckDB-based spatial data extraction module.
"""
import duckdb
import geopandas as gpd
import pandas as pd
from shapely import wkb, wkt
from typing import Optional, List, Dict, Any
import logging
from pathlib import Path
import tempfile
import shutil

from config import ExtractionConfig, BoundingBox, DataSource


logger = logging.getLogger(__name__)


class DuckDBSpatialExtractor:
    """Handles spatial data extraction using DuckDB."""
    
    def __init__(self, config: ExtractionConfig):
        self.config = config
        self.connection: Optional[duckdb.DuckDBPyConnection] = None
        
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
        
    def connect(self):
        """Establish DuckDB connection and load extensions."""
        try:
            self.connection = duckdb.connect(database=':memory:', read_only=False)
            
            # Load required extensions
            extensions = ['spatial', 'httpfs']
            if 'zip://' in self.config.data_source.url:
                extensions.append('zipfs')
                
            for ext in extensions:
                self.connection.execute(f"INSTALL {ext};")
                self.connection.execute(f"LOAD {ext};")
                
            logger.info("DuckDB connection established with extensions: %s", extensions)
            
        except Exception as e:
            logger.error("Failed to establish DuckDB connection: %s", e)
            raise
            
    def disconnect(self):
        """Close DuckDB connection."""
        if self.connection:
            try:
                self.connection.close()
                logger.info("DuckDB connection closed")
            except Exception as e:
                logger.error("Error closing DuckDB connection: %s", e)
                
    def build_query(self) -> str:
        """Build the extraction query."""
        bbox = self.config.bounding_box.to_list()
        
        # Build the basic query
        query_parts = []
        
        # Add layer specification if provided
        read_args = [f"'{self.config.data_source.url}'"]
        
        if self.config.data_source.layer_name:
            read_args.append(f"layer='{self.config.data_source.layer_name}'")
            
        # Add bounding box filter
        read_args.append(f"bbox=[{', '.join(map(str, bbox))}]")
        
        query = f"""
        SELECT * 
        FROM st_read({', '.join(read_args)});
        """
        
        logger.debug("Built query: %s", query)
        return query
        
    def extract_data(self) -> Optional[pd.DataFrame]:
        """Execute the extraction query and return results as DataFrame."""
        if not self.connection:
            raise RuntimeError("No active DuckDB connection")
            
        try:
            query = self.build_query()
            result = self.connection.execute(query)
            df = result.fetchdf()
            
            logger.info("Extracted %d rows from data source", len(df))
            return df
            
        except Exception as e:
            logger.error("Failed to extract data: %s", e)
            raise
            
    def to_geodataframe(self, df: pd.DataFrame) -> Optional[gpd.GeoDataFrame]:
        """Convert DataFrame to GeoDataFrame."""
        if df is None or df.empty:
            logger.warning("No data to convert to GeoDataFrame")
            return None
            
        try:
            # Try to find the geometry column
            geom_col = self.config.data_source.geometry_column
            
            # Check if geometry column exists
            if geom_col not in df.columns:
                # Try to find a geometry column
                possible_geom_cols = [col for col in df.columns 
                                    if col.lower() in ['geom', 'geometry', 'shape', 'wkb_geometry']]
                if possible_geom_cols:
                    geom_col = possible_geom_cols[0]
                    logger.info("Using detected geometry column: %s", geom_col)
                else:
                    raise ValueError(f"Geometry column '{geom_col}' not found in data")
                    
            # Convert geometry column
            # Try different geometry formats
            geometry_series = None
            
            # First, check if it's already a geometry type
            if hasattr(df[geom_col].iloc[0], '__geo_interface__'):
                geometry_series = df[geom_col]
            else:
                # Try WKB format
                try:
                    geometry_series = df[geom_col].apply(lambda x: wkb.loads(x) if x else None)
                    logger.info("Geometry loaded from WKB format")
                except:
                    # Try WKT format
                    try:
                        geometry_series = df[geom_col].apply(lambda x: wkt.loads(x) if x else None)
                        logger.info("Geometry loaded from WKT format")
                    except Exception as e:
                        logger.error("Failed to parse geometry: %s", e)
                        raise
                        
            # Create GeoDataFrame
            gdf = gpd.GeoDataFrame(df, geometry=geometry_series)
            
            # Set CRS if not already set
            if gdf.crs is None:
                # Assume WGS84 if not specified
                gdf.set_crs('EPSG:4326', inplace=True)
                logger.info("Set CRS to EPSG:4326 (WGS84)")
                
            logger.info("Created GeoDataFrame with %d features", len(gdf))
            return gdf
            
        except Exception as e:
            logger.error("Failed to convert to GeoDataFrame: %s", e)
            raise
            
    def extract_to_geodataframe(self) -> Optional[gpd.GeoDataFrame]:
        """Extract data and convert directly to GeoDataFrame."""
        df = self.extract_data()
        if df is not None:
            return self.to_geodataframe(df)
        return None


class DataExporter:
    """Handles exporting GeoDataFrame to various formats."""
    
    SUPPORTED_FORMATS = {
        'geojson': {'driver': 'GeoJSON', 'extension': '.geojson'},
        'shapefile': {'driver': 'ESRI Shapefile', 'extension': '.shp'},
        'gpkg': {'driver': 'GPKG', 'extension': '.gpkg'},
        'filegdb': {'driver': 'FileGDB', 'extension': '.gdb'},
        'csv': {'driver': None, 'extension': '.csv'},  # Special handling
        'parquet': {'driver': None, 'extension': '.parquet'},  # Special handling
    }
    
    def __init__(self, gdf: gpd.GeoDataFrame):
        self.gdf = gdf
        
    def export(self, output_path: str, format: str, layer_name: Optional[str] = None) -> bool:
        """Export GeoDataFrame to specified format."""
        format = format.lower()
        
        if format not in self.SUPPORTED_FORMATS:
            logger.error("Unsupported format: %s", format)
            return False
            
        try:
            if format == 'csv':
                # Special handling for CSV - drop geometry
                df = pd.DataFrame(self.gdf.drop(columns='geometry'))
                df.to_csv(output_path, index=False)
                
            elif format == 'parquet':
                # Use geoparquet
                self.gdf.to_parquet(output_path)
                
            elif format == 'filegdb':
                # FileGDB requires special handling
                return self._export_filegdb(output_path, layer_name)
                
            else:
                # Standard vector formats
                driver = self.SUPPORTED_FORMATS[format]['driver']
                self.gdf.to_file(output_path, driver=driver)
                
            logger.info("Successfully exported data to %s format: %s", format, output_path)
            return True
            
        except Exception as e:
            logger.error("Failed to export to %s format: %s", format, e)
            return False
            
    def _export_filegdb(self, output_path: str, layer_name: Optional[str] = None) -> bool:
        """Export to FileGDB with proper error handling."""
        try:
            # Ensure output path ends with .gdb
            if not output_path.endswith('.gdb'):
                output_path += '.gdb'
                
            # Remove existing GDB if it exists
            output_path_obj = Path(output_path)
            if output_path_obj.exists():
                shutil.rmtree(output_path_obj)
                
            # Create parent directory if needed
            output_path_obj.parent.mkdir(parents=True, exist_ok=True)
            
            # Export with layer name
            layer = layer_name or 'data'
            self.gdf.to_file(output_path, driver='FileGDB', layer=layer)
            
            logger.info("Successfully exported to FileGDB: %s (layer: %s)", output_path, layer)
            return True
            
        except Exception as e:
            # If FileGDB driver is not available, try OpenFileGDB
            try:
                logger.warning("FileGDB driver not available, trying OpenFileGDB: %s", e)
                self.gdf.to_file(output_path, driver='OpenFileGDB', layer=layer)
                logger.info("Successfully exported using OpenFileGDB driver")
                return True
            except Exception as e2:
                logger.error("Failed to export FileGDB with both drivers: %s", e2)
                return False
                
    def export_multiple(self, output_dir: str, base_name: str, 
                       formats: List[str]) -> Dict[str, str]:
        """Export to multiple formats."""
        output_dir_path = Path(output_dir)
        output_dir_path.mkdir(parents=True, exist_ok=True)
        
        results = {}
        
        for format in formats:
            if format not in self.SUPPORTED_FORMATS:
                logger.warning("Skipping unsupported format: %s", format)
                continue
                
            extension = self.SUPPORTED_FORMATS[format]['extension']
            output_path = str(output_dir_path / f"{base_name}{extension}")
            
            success = self.export(output_path, format)
            if success:
                results[format] = output_path
            else:
                results[format] = None
                
        return results