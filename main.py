"""
Main script for DuckDB spatial data extraction and Box upload.
"""
import argparse
import logging
import sys
import json
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any

from config import (
    ExtractionConfig, BoxConfig, BoundingBox, DataSource,
    DATA_SOURCES, LOCATIONS, load_config_from_file
)
from data_extractor import DuckDBSpatialExtractor, DataExporter
from box_uploader import BoxUploader


# Configure logging
def setup_logging(level: str = "INFO"):
    """Configure logging for the application."""
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(f"extraction_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        ]
    )


def create_extraction_config(args) -> ExtractionConfig:
    """Create extraction configuration from command line arguments."""
    # Load from config file if provided
    if args.config:
        return load_config_from_file(args.config)
    
    # Build from command line arguments
    # Get data source
    if args.source in DATA_SOURCES:
        data_source = DATA_SOURCES[args.source]
    else:
        # Assume it's a URL
        data_source = DataSource(
            name="custom",
            url=args.source,
            layer_name=args.layer,
            geometry_column=args.geom_column or "geom"
        )
    
    # Get bounding box
    if args.location in LOCATIONS:
        bbox = LOCATIONS[args.location]
    elif args.bbox:
        bbox = BoundingBox(*args.bbox)
    else:
        raise ValueError("Either --location or --bbox must be provided")
    
    # Parse metadata
    metadata = {}
    if args.metadata:
        try:
            metadata = json.loads(args.metadata)
        except json.JSONDecodeError:
            logging.warning("Failed to parse metadata JSON, using empty metadata")
    
    # Add default metadata
    metadata.update({
        "extraction_date": datetime.now().isoformat(),
        "source": data_source.name,
        "bbox": bbox.to_list()
    })
    
    return ExtractionConfig(
        job_name=args.job_name or "extraction",
        data_source=data_source,
        bounding_box=bbox,
        output_formats=args.formats,
        output_prefix=args.output_prefix or "data",
        metadata=metadata
    )


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description="Extract spatial data using DuckDB and upload to Box"
    )
    
    # Data source arguments
    parser.add_argument(
        "--source", "-s",
        help="Data source name (from predefined) or URL"
    )
    parser.add_argument(
        "--layer", "-l",
        help="Layer name for multi-layer sources"
    )
    parser.add_argument(
        "--geom-column",
        help="Geometry column name (default: geom)"
    )
    
    # Location arguments
    parser.add_argument(
        "--location",
        help="Predefined location name"
    )
    parser.add_argument(
        "--bbox",
        nargs=4,
        type=float,
        metavar=("MIN_LON", "MIN_LAT", "MAX_LON", "MAX_LAT"),
        help="Bounding box coordinates"
    )
    
    # Output arguments
    parser.add_argument(
        "--formats", "-f",
        nargs="+",
        default=["geojson", "filegdb"],
        choices=["geojson", "shapefile", "gpkg", "filegdb", "csv", "parquet"],
        help="Output formats"
    )
    parser.add_argument(
        "--output-dir", "-o",
        default="./output",
        help="Output directory"
    )
    parser.add_argument(
        "--output-prefix",
        help="Prefix for output files"
    )
    
    # Job configuration
    parser.add_argument(
        "--job-name", "-j",
        help="Job name for identification"
    )
    parser.add_argument(
        "--config", "-c",
        help="Configuration file path (JSON)"
    )
    
    # Box upload arguments
    parser.add_argument(
        "--upload-to-box",
        action="store_true",
        help="Upload results to Box"
    )
    parser.add_argument(
        "--box-folder-name",
        help="Create subfolder in Box with this name"
    )
    
    # Metadata
    parser.add_argument(
        "--metadata", "-m",
        help="Additional metadata as JSON string"
    )
    
    # Other options
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without executing"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    try:
        # Create extraction configuration
        if not args.config and not args.source:
            parser.error("Either --config or --source must be provided")
            
        config = create_extraction_config(args)
        
        if args.dry_run:
            logger.info("DRY RUN - Configuration:")
            logger.info("  Job name: %s", config.job_name)
            logger.info("  Data source: %s", config.data_source.name)
            logger.info("  Bounding box: %s", config.bounding_box.to_list())
            logger.info("  Output formats: %s", config.output_formats)
            logger.info("  Output directory: %s", args.output_dir)
            return
        
        # Extract data
        logger.info("Starting data extraction job: %s", config.job_name)
        
        with DuckDBSpatialExtractor(config) as extractor:
            gdf = extractor.extract_to_geodataframe()
            
            if gdf is None or gdf.empty:
                logger.error("No data extracted")
                sys.exit(1)
                
            logger.info("Extracted %d features", len(gdf))
            
            # Export to requested formats
            exporter = DataExporter(gdf)
            
            # Generate base filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_name = f"{config.output_prefix}_{config.job_name}_{timestamp}"
            
            export_results = exporter.export_multiple(
                args.output_dir,
                base_name,
                config.output_formats
            )
            
            # Log export results
            for format_type, file_path in export_results.items():
                if file_path:
                    logger.info("Exported %s: %s", format_type, file_path)
                else:
                    logger.error("Failed to export %s", format_type)
            
            # Upload to Box if requested
            if args.upload_to_box:
                box_config = BoxConfig.from_env()
                if not box_config:
                    logger.error("Box configuration not found in environment variables")
                    sys.exit(1)
                    
                logger.info("Uploading to Box...")
                
                with BoxUploader(box_config) as uploader:
                    # Create subfolder if requested
                    upload_folder_id = box_config.folder_id
                    if args.box_folder_name:
                        folder_id = uploader.create_folder(args.box_folder_name)
                        if folder_id:
                            upload_folder_id = folder_id
                            
                    # Upload files
                    upload_results = uploader.upload_multiple(
                        export_results,
                        metadata=config.metadata
                    )
                    
                    # Log upload results
                    for format_type, file_id in upload_results.items():
                        if file_id:
                            logger.info("Uploaded %s to Box (ID: %s)", format_type, file_id)
                        else:
                            logger.error("Failed to upload %s to Box", format_type)
                            
        logger.info("Job completed successfully")
        
    except Exception as e:
        logger.error("Job failed: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()