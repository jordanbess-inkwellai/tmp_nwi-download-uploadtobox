"""
Bounding Box Calculator for polygon features.
Calculates bounding boxes from GeoJSON files or GeoPackage vector tables.
"""
import geopandas as gpd
import json
import argparse
import logging
from pathlib import Path
from typing import Union, Dict, List, Optional, Tuple
import duckdb
from shapely.geometry import box, shape
import sys

from config import BoundingBox

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class BBoxCalculator:
    """Calculate bounding boxes from various spatial data sources."""
    
    def __init__(self):
        self.connection: Optional[duckdb.DuckDBPyConnection] = None
        
    def calculate_from_geojson(self, geojson_path: str, 
                             feature_filter: Optional[Dict[str, any]] = None) -> BoundingBox:
        """
        Calculate bounding box from GeoJSON file.
        
        Args:
            geojson_path: Path to GeoJSON file
            feature_filter: Optional filter to select specific features
            
        Returns:
            BoundingBox object
        """
        try:
            # Read GeoJSON
            gdf = gpd.read_file(geojson_path)
            
            # Apply filter if provided
            if feature_filter:
                for key, value in feature_filter.items():
                    if key in gdf.columns:
                        gdf = gdf[gdf[key] == value]
                        
            if gdf.empty:
                raise ValueError("No features found after filtering")
                
            # Calculate bounds
            bounds = gdf.total_bounds  # Returns [minx, miny, maxx, maxy]
            
            bbox = BoundingBox(
                min_lon=bounds[0],
                min_lat=bounds[1],
                max_lon=bounds[2],
                max_lat=bounds[3]
            )
            
            logger.info(f"Calculated bbox from {len(gdf)} features: {bbox}")
            return bbox
            
        except Exception as e:
            logger.error(f"Failed to calculate bbox from GeoJSON: {e}")
            raise
            
    def calculate_from_geopackage(self, gpkg_path: str, layer_name: Optional[str] = None,
                                feature_filter: Optional[Dict[str, any]] = None) -> BoundingBox:
        """
        Calculate bounding box from GeoPackage.
        
        Args:
            gpkg_path: Path to GeoPackage file
            layer_name: Optional layer name (if not provided, uses first layer)
            feature_filter: Optional filter to select specific features
            
        Returns:
            BoundingBox object
        """
        try:
            # List available layers if layer_name not provided
            if not layer_name:
                import fiona
                layers = fiona.listlayers(gpkg_path)
                if not layers:
                    raise ValueError("No layers found in GeoPackage")
                layer_name = layers[0]
                logger.info(f"Using first layer: {layer_name}")
                
            # Read GeoPackage layer
            gdf = gpd.read_file(gpkg_path, layer=layer_name)
            
            # Apply filter if provided
            if feature_filter:
                for key, value in feature_filter.items():
                    if key in gdf.columns:
                        gdf = gdf[gdf[key] == value]
                        
            if gdf.empty:
                raise ValueError("No features found after filtering")
                
            # Calculate bounds
            bounds = gdf.total_bounds
            
            bbox = BoundingBox(
                min_lon=bounds[0],
                min_lat=bounds[1],
                max_lon=bounds[2],
                max_lat=bounds[3]
            )
            
            logger.info(f"Calculated bbox from {len(gdf)} features in layer '{layer_name}': {bbox}")
            return bbox
            
        except Exception as e:
            logger.error(f"Failed to calculate bbox from GeoPackage: {e}")
            raise
            
    def calculate_from_shapefile(self, shp_path: str,
                               feature_filter: Optional[Dict[str, any]] = None) -> BoundingBox:
        """
        Calculate bounding box from Shapefile.
        
        Args:
            shp_path: Path to Shapefile
            feature_filter: Optional filter to select specific features
            
        Returns:
            BoundingBox object
        """
        try:
            # Read Shapefile
            gdf = gpd.read_file(shp_path)
            
            # Apply filter if provided
            if feature_filter:
                for key, value in feature_filter.items():
                    if key in gdf.columns:
                        gdf = gdf[gdf[key] == value]
                        
            if gdf.empty:
                raise ValueError("No features found after filtering")
                
            # Calculate bounds
            bounds = gdf.total_bounds
            
            bbox = BoundingBox(
                min_lon=bounds[0],
                min_lat=bounds[1],
                max_lon=bounds[2],
                max_lat=bounds[3]
            )
            
            logger.info(f"Calculated bbox from {len(gdf)} features: {bbox}")
            return bbox
            
        except Exception as e:
            logger.error(f"Failed to calculate bbox from Shapefile: {e}")
            raise
            
    def calculate_with_duckdb(self, data_path: str, layer_name: Optional[str] = None,
                            where_clause: Optional[str] = None) -> BoundingBox:
        """
        Calculate bounding box using DuckDB spatial extension.
        More efficient for large datasets.
        
        Args:
            data_path: Path to spatial data file
            layer_name: Optional layer name for multi-layer formats
            where_clause: Optional SQL WHERE clause for filtering
            
        Returns:
            BoundingBox object
        """
        try:
            # Create DuckDB connection
            conn = duckdb.connect(':memory:')
            
            # Load spatial extension
            conn.execute("INSTALL spatial;")
            conn.execute("LOAD spatial;")
            
            # Build read query
            read_args = [f"'{data_path}'"]
            if layer_name:
                read_args.append(f"layer='{layer_name}'")
                
            # Query to calculate bounds
            base_query = f"SELECT * FROM st_read({', '.join(read_args)})"
            
            if where_clause:
                query = f"""
                WITH filtered AS (
                    SELECT * FROM ({base_query}) 
                    WHERE {where_clause}
                )
                SELECT 
                    ST_XMin(ST_Extent(geom)) as min_lon,
                    ST_YMin(ST_Extent(geom)) as min_lat,
                    ST_XMax(ST_Extent(geom)) as max_lon,
                    ST_YMax(ST_Extent(geom)) as max_lat,
                    COUNT(*) as feature_count
                FROM filtered
                """
            else:
                query = f"""
                SELECT 
                    ST_XMin(ST_Extent(geom)) as min_lon,
                    ST_YMin(ST_Extent(geom)) as min_lat,
                    ST_XMax(ST_Extent(geom)) as max_lon,
                    ST_YMax(ST_Extent(geom)) as max_lat,
                    COUNT(*) as feature_count
                FROM ({base_query})
                """
                
            result = conn.execute(query).fetchone()
            
            if not result or result[0] is None:
                raise ValueError("No features found or unable to calculate bounds")
                
            bbox = BoundingBox(
                min_lon=result[0],
                min_lat=result[1],
                max_lon=result[2],
                max_lat=result[3]
            )
            
            logger.info(f"Calculated bbox from {result[4]} features using DuckDB: {bbox}")
            
            conn.close()
            return bbox
            
        except Exception as e:
            logger.error(f"Failed to calculate bbox with DuckDB: {e}")
            raise
            
    def calculate_from_wkt(self, wkt_string: str) -> BoundingBox:
        """
        Calculate bounding box from WKT string.
        
        Args:
            wkt_string: Well-Known Text representation of geometry
            
        Returns:
            BoundingBox object
        """
        try:
            from shapely import wkt
            
            geom = wkt.loads(wkt_string)
            bounds = geom.bounds  # Returns (minx, miny, maxx, maxy)
            
            bbox = BoundingBox(
                min_lon=bounds[0],
                min_lat=bounds[1],
                max_lon=bounds[2],
                max_lat=bounds[3]
            )
            
            logger.info(f"Calculated bbox from WKT: {bbox}")
            return bbox
            
        except Exception as e:
            logger.error(f"Failed to calculate bbox from WKT: {e}")
            raise
            
    def expand_bbox(self, bbox: BoundingBox, buffer_degrees: float) -> BoundingBox:
        """
        Expand bounding box by a buffer in degrees.
        
        Args:
            bbox: Original bounding box
            buffer_degrees: Buffer size in degrees
            
        Returns:
            Expanded BoundingBox
        """
        return BoundingBox(
            min_lon=bbox.min_lon - buffer_degrees,
            min_lat=bbox.min_lat - buffer_degrees,
            max_lon=bbox.max_lon + buffer_degrees,
            max_lat=bbox.max_lat + buffer_degrees
        )
        
    def calculate_union(self, bboxes: List[BoundingBox]) -> BoundingBox:
        """
        Calculate union of multiple bounding boxes.
        
        Args:
            bboxes: List of BoundingBox objects
            
        Returns:
            Union BoundingBox
        """
        if not bboxes:
            raise ValueError("No bounding boxes provided")
            
        min_lon = min(bbox.min_lon for bbox in bboxes)
        min_lat = min(bbox.min_lat for bbox in bboxes)
        max_lon = max(bbox.max_lon for bbox in bboxes)
        max_lat = max(bbox.max_lat for bbox in bboxes)
        
        return BoundingBox(
            min_lon=min_lon,
            min_lat=min_lat,
            max_lon=max_lon,
            max_lat=max_lat
        )
        
    def calculate_intersection(self, bboxes: List[BoundingBox]) -> Optional[BoundingBox]:
        """
        Calculate intersection of multiple bounding boxes.
        
        Args:
            bboxes: List of BoundingBox objects
            
        Returns:
            Intersection BoundingBox or None if no intersection
        """
        if not bboxes:
            raise ValueError("No bounding boxes provided")
            
        min_lon = max(bbox.min_lon for bbox in bboxes)
        min_lat = max(bbox.min_lat for bbox in bboxes)
        max_lon = min(bbox.max_lon for bbox in bboxes)
        max_lat = min(bbox.max_lat for bbox in bboxes)
        
        # Check if valid intersection
        if min_lon >= max_lon or min_lat >= max_lat:
            return None
            
        return BoundingBox(
            min_lon=min_lon,
            min_lat=min_lat,
            max_lon=max_lon,
            max_lat=max_lat
        )


def main():
    """Command-line interface for bbox calculator."""
    parser = argparse.ArgumentParser(
        description="Calculate bounding box from polygon features",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Calculate bbox from GeoJSON
  python bbox_calculator.py input.geojson
  
  # Calculate bbox from GeoPackage with specific layer
  python bbox_calculator.py data.gpkg --layer buildings
  
  # Calculate bbox with filter
  python bbox_calculator.py data.geojson --filter '{"state": "FL"}'
  
  # Calculate bbox with SQL filter using DuckDB
  python bbox_calculator.py data.gpkg --where "population > 10000" --use-duckdb
  
  # Add buffer to bbox
  python bbox_calculator.py input.geojson --buffer 0.01
  
  # Output as JSON
  python bbox_calculator.py input.geojson --output-format json
        """
    )
    
    parser.add_argument('input_file', help='Input spatial data file (GeoJSON, GeoPackage, Shapefile)')
    parser.add_argument('--layer', '-l', help='Layer name for multi-layer formats')
    parser.add_argument('--filter', '-f', help='JSON filter for features (e.g., \'{"key": "value"}\')')
    parser.add_argument('--where', '-w', help='SQL WHERE clause for filtering (requires --use-duckdb)')
    parser.add_argument('--use-duckdb', action='store_true', help='Use DuckDB for calculation (faster for large files)')
    parser.add_argument('--buffer', '-b', type=float, help='Buffer in degrees to expand bbox')
    parser.add_argument('--output-format', '-o', choices=['text', 'json', 'wkt'], default='text',
                       help='Output format (default: text)')
    parser.add_argument('--save-to', '-s', help='Save bbox to JSON file')
    
    args = parser.parse_args()
    
    # Validate input file
    input_path = Path(args.input_file)
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        sys.exit(1)
        
    # Determine file type
    file_ext = input_path.suffix.lower()
    
    try:
        calculator = BBoxCalculator()
        
        # Calculate bbox based on file type and options
        if args.use_duckdb or args.where:
            bbox = calculator.calculate_with_duckdb(
                str(input_path),
                layer_name=args.layer,
                where_clause=args.where
            )
        elif file_ext == '.geojson':
            feature_filter = json.loads(args.filter) if args.filter else None
            bbox = calculator.calculate_from_geojson(str(input_path), feature_filter)
        elif file_ext == '.gpkg':
            feature_filter = json.loads(args.filter) if args.filter else None
            bbox = calculator.calculate_from_geopackage(str(input_path), args.layer, feature_filter)
        elif file_ext in ['.shp', '.dbf']:
            feature_filter = json.loads(args.filter) if args.filter else None
            bbox = calculator.calculate_from_shapefile(str(input_path), feature_filter)
        else:
            logger.error(f"Unsupported file type: {file_ext}")
            sys.exit(1)
            
        # Apply buffer if requested
        if args.buffer:
            bbox = calculator.expand_bbox(bbox, args.buffer)
            logger.info(f"Applied buffer of {args.buffer} degrees")
            
        # Output results
        if args.output_format == 'json':
            output = {
                'min_lon': bbox.min_lon,
                'min_lat': bbox.min_lat,
                'max_lon': bbox.max_lon,
                'max_lat': bbox.max_lat,
                'bbox_array': bbox.to_list()
            }
            print(json.dumps(output, indent=2))
        elif args.output_format == 'wkt':
            wkt = bbox.to_shapely_box().wkt
            print(wkt)
        else:
            print(f"Bounding Box:")
            print(f"  Min Longitude: {bbox.min_lon}")
            print(f"  Min Latitude:  {bbox.min_lat}")
            print(f"  Max Longitude: {bbox.max_lon}")
            print(f"  Max Latitude:  {bbox.max_lat}")
            print(f"  Array format:  [{bbox.min_lon}, {bbox.min_lat}, {bbox.max_lon}, {bbox.max_lat}]")
            
        # Save to file if requested
        if args.save_to:
            save_path = Path(args.save_to)
            save_data = {
                'bbox': {
                    'min_lon': bbox.min_lon,
                    'min_lat': bbox.min_lat,
                    'max_lon': bbox.max_lon,
                    'max_lat': bbox.max_lat
                },
                'source_file': str(input_path),
                'layer': args.layer,
                'filter': args.filter,
                'buffer': args.buffer
            }
            
            with open(save_path, 'w') as f:
                json.dump(save_data, f, indent=2)
                
            logger.info(f"Saved bbox to: {save_path}")
            
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()