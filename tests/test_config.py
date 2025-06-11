"""
Unit tests for configuration module.
"""
import unittest
import json
import tempfile
from pathlib import Path

from config import (
    BoundingBox, DataSource, BoxConfig, ExtractionConfig,
    load_config_from_file
)


class TestBoundingBox(unittest.TestCase):
    def test_creation(self):
        bbox = BoundingBox(-180, -90, 180, 90)
        self.assertEqual(bbox.min_lon, -180)
        self.assertEqual(bbox.min_lat, -90)
        self.assertEqual(bbox.max_lon, 180)
        self.assertEqual(bbox.max_lat, 90)
        
    def test_to_list(self):
        bbox = BoundingBox(-180, -90, 180, 90)
        self.assertEqual(bbox.to_list(), [-180, -90, 180, 90])
        
    def test_to_shapely_box(self):
        bbox = BoundingBox(-180, -90, 180, 90)
        box_geom = bbox.to_shapely_box()
        self.assertAlmostEqual(box_geom.bounds[0], -180)
        self.assertAlmostEqual(box_geom.bounds[1], -90)
        self.assertAlmostEqual(box_geom.bounds[2], 180)
        self.assertAlmostEqual(box_geom.bounds[3], 90)


class TestDataSource(unittest.TestCase):
    def test_creation(self):
        source = DataSource(
            name="Test Source",
            url="https://example.com/data.zip",
            layer_name="test_layer",
            geometry_column="geometry"
        )
        self.assertEqual(source.name, "Test Source")
        self.assertEqual(source.url, "https://example.com/data.zip")
        self.assertEqual(source.layer_name, "test_layer")
        self.assertEqual(source.geometry_column, "geometry")
        
    def test_defaults(self):
        source = DataSource(name="Test", url="test.zip")
        self.assertIsNone(source.layer_name)
        self.assertEqual(source.geometry_column, "geom")
        self.assertEqual(source.description, "")


class TestBoxConfig(unittest.TestCase):
    def test_from_env_missing(self):
        # Test with no environment variables
        config = BoxConfig.from_env()
        self.assertIsNone(config)
        
    def test_creation(self):
        config = BoxConfig(
            client_id="test_id",
            client_secret="test_secret",
            access_token="test_token",
            folder_id="test_folder"
        )
        self.assertEqual(config.client_id, "test_id")
        self.assertEqual(config.client_secret, "test_secret")
        self.assertEqual(config.access_token, "test_token")
        self.assertEqual(config.folder_id, "test_folder")


class TestExtractionConfig(unittest.TestCase):
    def test_creation(self):
        source = DataSource(name="Test", url="test.zip")
        bbox = BoundingBox(-180, -90, 180, 90)
        
        config = ExtractionConfig(
            job_name="test_job",
            data_source=source,
            bounding_box=bbox
        )
        
        self.assertEqual(config.job_name, "test_job")
        self.assertEqual(config.data_source, source)
        self.assertEqual(config.bounding_box, bbox)
        self.assertEqual(config.output_formats, ["geojson", "filegdb"])
        
    def test_get_output_filename(self):
        source = DataSource(name="Test", url="test.zip")
        bbox = BoundingBox(-180, -90, 180, 90)
        
        config = ExtractionConfig(
            job_name="test_job",
            data_source=source,
            bounding_box=bbox,
            output_prefix="test"
        )
        
        # Test different formats
        geojson_name = config.get_output_filename("geojson")
        self.assertTrue(geojson_name.startswith("test_test_job_"))
        self.assertTrue(geojson_name.endswith(".geojson"))
        
        gdb_name = config.get_output_filename("filegdb")
        self.assertTrue(gdb_name.endswith(".gdb"))
        
    def test_load_from_file(self):
        config_data = {
            "job_name": "test_extraction",
            "data_source": {
                "name": "Test Source",
                "url": "https://example.com/data.zip",
                "geometry_column": "geom"
            },
            "bounding_box": [-180, -90, 180, 90],
            "output_formats": ["geojson"],
            "metadata": {"test": "value"}
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config_data, f)
            temp_path = f.name
            
        try:
            config = load_config_from_file(temp_path)
            self.assertEqual(config.job_name, "test_extraction")
            self.assertEqual(config.data_source.name, "Test Source")
            self.assertEqual(config.bounding_box.to_list(), [-180, -90, 180, 90])
            self.assertEqual(config.metadata["test"], "value")
        finally:
            Path(temp_path).unlink()


if __name__ == '__main__':
    unittest.main()