"""
Validation script to check if the environment is properly set up.
"""
import sys
import os
import importlib
from pathlib import Path


def check_python_version():
    """Check if Python version is compatible."""
    print("Checking Python version...")
    version = sys.version_info
    if version.major >= 3 and version.minor >= 8:
        print(f"✓ Python {version.major}.{version.minor}.{version.micro} - OK")
        return True
    else:
        print(f"✗ Python {version.major}.{version.minor}.{version.micro} - Requires Python 3.8+")
        return False


def check_required_packages():
    """Check if required packages are installed."""
    print("\nChecking required packages...")
    
    packages = {
        'duckdb': 'duckdb',
        'geopandas': 'geopandas',
        'shapely': 'shapely',
        'pandas': 'pandas',
        'boxsdk': 'boxsdk',
        'pyarrow': 'pyarrow',
        'fiona': 'fiona'
    }
    
    all_good = True
    for package_name, import_name in packages.items():
        try:
            module = importlib.import_module(import_name)
            version = getattr(module, '__version__', 'unknown')
            print(f"✓ {package_name} ({version}) - OK")
        except ImportError:
            print(f"✗ {package_name} - NOT INSTALLED")
            all_good = False
            
    return all_good


def check_gdal():
    """Check GDAL installation and drivers."""
    print("\nChecking GDAL...")
    
    try:
        from osgeo import ogr, gdal
        print(f"✓ GDAL {gdal.__version__} - OK")
        
        # Check for important drivers
        important_drivers = ['GeoJSON', 'ESRI Shapefile', 'GPKG', 'FileGDB', 'OpenFileGDB']
        print("\nChecking GDAL drivers:")
        
        for driver_name in important_drivers:
            driver = ogr.GetDriverByName(driver_name)
            if driver:
                print(f"  ✓ {driver_name} - Available")
            else:
                print(f"  ✗ {driver_name} - Not available")
                
        return True
    except ImportError:
        print("✗ GDAL - NOT INSTALLED (optional, but recommended for FileGDB support)")
        return False


def check_duckdb_extensions():
    """Check DuckDB extensions."""
    print("\nChecking DuckDB extensions...")
    
    try:
        import duckdb
        con = duckdb.connect(':memory:')
        
        extensions = ['spatial', 'httpfs', 'zipfs']
        all_good = True
        
        for ext in extensions:
            try:
                con.execute(f"INSTALL {ext};")
                con.execute(f"LOAD {ext};")
                print(f"✓ {ext} - OK")
            except Exception as e:
                print(f"✗ {ext} - Failed: {e}")
                all_good = False
                
        con.close()
        return all_good
        
    except Exception as e:
        print(f"✗ DuckDB extensions check failed: {e}")
        return False


def check_environment_variables():
    """Check Box.com environment variables."""
    print("\nChecking environment variables...")
    
    env_vars = {
        'BOX_CLIENT_ID': 'Box Client ID',
        'BOX_CLIENT_SECRET': 'Box Client Secret',
        'BOX_ACCESS_TOKEN': 'Box Access Token',
        'BOX_FOLDER_ID': 'Box Folder ID',
        'BOX_METADATA_TEMPLATE_KEY': 'Box Metadata Template Key (optional)'
    }
    
    all_good = True
    for var_name, description in env_vars.items():
        value = os.environ.get(var_name)
        if value:
            # Mask the value for security
            masked_value = value[:4] + '...' + value[-4:] if len(value) > 8 else '***'
            print(f"✓ {var_name} - Set ({masked_value})")
        else:
            if 'optional' in description.lower():
                print(f"○ {var_name} - Not set ({description})")
            else:
                print(f"✗ {var_name} - NOT SET ({description})")
                all_good = False
                
    return all_good


def check_file_structure():
    """Check if all required files exist."""
    print("\nChecking file structure...")
    
    required_files = [
        'config.py',
        'data_extractor.py',
        'box_uploader.py',
        'main.py',
        'requirements.txt'
    ]
    
    optional_files = [
        'example_config.json',
        '.env',
        'Dockerfile',
        'docker-compose.yml'
    ]
    
    all_good = True
    
    for file_name in required_files:
        if Path(file_name).exists():
            print(f"✓ {file_name} - Found")
        else:
            print(f"✗ {file_name} - NOT FOUND")
            all_good = False
            
    print("\nOptional files:")
    for file_name in optional_files:
        if Path(file_name).exists():
            print(f"✓ {file_name} - Found")
        else:
            print(f"○ {file_name} - Not found")
            
    return all_good


def test_basic_import():
    """Test basic imports."""
    print("\nTesting basic imports...")
    
    try:
        from config import ExtractionConfig, BoxConfig, BoundingBox, DataSource
        from data_extractor import DuckDBSpatialExtractor, DataExporter
        from box_uploader import BoxUploader
        print("✓ All modules imported successfully")
        return True
    except Exception as e:
        print(f"✗ Import failed: {e}")
        return False


def main():
    """Run all validation checks."""
    print("="*60)
    print("DuckDB Spatial Extractor - Setup Validation")
    print("="*60)
    
    checks = [
        ("Python Version", check_python_version),
        ("Required Packages", check_required_packages),
        ("GDAL Support", check_gdal),
        ("DuckDB Extensions", check_duckdb_extensions),
        ("Environment Variables", check_environment_variables),
        ("File Structure", check_file_structure),
        ("Module Imports", test_basic_import)
    ]
    
    results = {}
    for check_name, check_func in checks:
        try:
            results[check_name] = check_func()
        except Exception as e:
            print(f"\n✗ {check_name} check failed with error: {e}")
            results[check_name] = False
            
    # Summary
    print("\n" + "="*60)
    print("VALIDATION SUMMARY")
    print("="*60)
    
    total_checks = len(results)
    passed_checks = sum(1 for v in results.values() if v)
    
    for check_name, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{check_name}: {status}")
        
    print(f"\nTotal: {passed_checks}/{total_checks} checks passed")
    
    if passed_checks == total_checks:
        print("\n✓ All checks passed! The system is ready to use.")
        print("\nTry running:")
        print("  python main.py --help")
    else:
        print("\n✗ Some checks failed. Please address the issues above.")
        print("\nFor missing packages, run:")
        print("  pip install -r requirements.txt")
        print("\nFor environment variables, copy and edit .env.example:")
        print("  cp .env.example .env")
        

if __name__ == "__main__":
    main()