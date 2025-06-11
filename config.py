"""
Configuration module for DuckDB data extraction and Box upload.
"""
import os
import json
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class BoundingBox:
    """Represents a geographic bounding box."""
    min_lon: float
    min_lat: float
    max_lon: float
    max_lat: float
    
    def to_list(self) -> List[float]:
        """Convert to list format for DuckDB."""
        return [self.min_lon, self.min_lat, self.max_lon, self.max_lat]
    
    def to_shapely_box(self):
        """Convert to shapely box geometry."""
        from shapely.geometry import box
        return box(self.min_lon, self.min_lat, self.max_lon, self.max_lat)


@dataclass
class DataSource:
    """Configuration for a data source."""
    name: str
    url: str
    layer_name: Optional[str] = None
    geometry_column: str = "geom"
    description: str = ""


@dataclass
class BoxConfig:
    """Box.com configuration."""
    client_id: str
    client_secret: str
    access_token: str
    folder_id: str
    metadata_template_key: Optional[str] = None
    
    @classmethod
    def from_env(cls) -> Optional['BoxConfig']:
        """Create BoxConfig from environment variables."""
        client_id = os.environ.get("BOX_CLIENT_ID")
        client_secret = os.environ.get("BOX_CLIENT_SECRET")
        access_token = os.environ.get("BOX_ACCESS_TOKEN")
        folder_id = os.environ.get("BOX_FOLDER_ID")
        metadata_template_key = os.environ.get("BOX_METADATA_TEMPLATE_KEY")
        
        if not all([client_id, client_secret, access_token, folder_id]):
            return None
            
        return cls(
            client_id=client_id,
            client_secret=client_secret,
            access_token=access_token,
            folder_id=folder_id,
            metadata_template_key=metadata_template_key
        )


@dataclass
class ExtractionConfig:
    """Configuration for data extraction job."""
    job_name: str
    data_source: DataSource
    bounding_box: BoundingBox
    output_formats: List[str] = field(default_factory=lambda: ["geojson", "filegdb"])
    output_prefix: str = "extracted_data"
    metadata: Dict[str, any] = field(default_factory=dict)
    
    def get_output_filename(self, format: str) -> str:
        """Generate output filename for given format."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = f"{self.output_prefix}_{self.job_name}_{timestamp}"
        
        if format == "geojson":
            return f"{base_name}.geojson"
        elif format == "filegdb":
            return f"{base_name}.gdb"
        elif format == "shapefile":
            return f"{base_name}.shp"
        else:
            return f"{base_name}.{format}"


# Predefined data sources
DATA_SOURCES = {
    "fws_wetlands_fl": DataSource(
        name="FWS Florida Wetlands",
        url="zip://https://documentst.ecosphere.fws.gov/wetlands/data/State-Downloads/FL_geodatabase_wetlands.zip!FL_geodatabase_wetlands.gdb",
        geometry_column="geom",
        description="US Fish and Wildlife Service wetlands data for Florida"
    ),
    # Add more data sources here as needed
}


# Predefined locations
LOCATIONS = {
    "disney_animal_kingdom": BoundingBox(
        min_lon=-81.61303476507055,
        min_lat=28.34441345722594,
        max_lon=-81.56830935354668,
        max_lat=28.3768179430727
    ),
    # Add more locations here as needed
}


def load_config_from_file(config_path: str) -> ExtractionConfig:
    """Load extraction configuration from JSON file."""
    with open(config_path, 'r') as f:
        config_data = json.load(f)
    
    # Parse bounding box
    bbox_data = config_data.get('bounding_box', {})
    if isinstance(bbox_data, list) and len(bbox_data) == 4:
        bbox = BoundingBox(*bbox_data)
    else:
        bbox = BoundingBox(
            min_lon=bbox_data.get('min_lon'),
            min_lat=bbox_data.get('min_lat'),
            max_lon=bbox_data.get('max_lon'),
            max_lat=bbox_data.get('max_lat')
        )
    
    # Parse data source
    source_data = config_data.get('data_source', {})
    if isinstance(source_data, str):
        # Reference to predefined source
        data_source = DATA_SOURCES.get(source_data)
        if not data_source:
            raise ValueError(f"Unknown data source: {source_data}")
    else:
        data_source = DataSource(**source_data)
    
    return ExtractionConfig(
        job_name=config_data.get('job_name', 'extraction'),
        data_source=data_source,
        bounding_box=bbox,
        output_formats=config_data.get('output_formats', ['geojson', 'filegdb']),
        output_prefix=config_data.get('output_prefix', 'extracted_data'),
        metadata=config_data.get('metadata', {})
    )