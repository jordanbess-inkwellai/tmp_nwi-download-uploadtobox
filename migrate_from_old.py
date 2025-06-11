"""
Migration script to help users transition from the old duckdb_extract.py to the new system.
"""
import json
from pathlib import Path
from config import BoundingBox, DATA_SOURCES, LOCATIONS


def migrate_old_script():
    """Create a configuration file based on the old script's hardcoded values."""
    
    # Old script's hardcoded values
    old_bbox = [-81.61303476507055, 28.34441345722594, -81.56830935354668, 28.3768179430727]
    old_url = 'zip://https://documentst.ecosphere.fws.gov/wetlands/data/State-Downloads/FL_geodatabase_wetlands.zip!FL_geodatabase_wetlands.gdb'
    
    # Create new configuration
    config = {
        "job_name": "animal_kingdom_wetlands",
        "data_source": "fws_wetlands_fl",  # Using predefined source
        "bounding_box": {
            "min_lon": old_bbox[0],
            "min_lat": old_bbox[1],
            "max_lon": old_bbox[2],
            "max_lat": old_bbox[3]
        },
        "output_formats": ["geojson", "filegdb"],
        "output_prefix": "animal_kingdom_wetlands",
        "metadata": {
            "project_name": "Animal Kingdom Wetlands",
            "data_source": "FWS",
            "extraction_date": "{{ now }}"  # Will be replaced at runtime
        }
    }
    
    # Save configuration
    config_path = Path("migrated_config.json")
    with open(config_path, 'w') as f:
        json.dump(config, f, indent=2)
        
    print(f"Created configuration file: {config_path}")
    print("\nTo run with the new system:")
    print(f"  python main.py --config {config_path}")
    print("\nWith Box upload:")
    print(f"  python main.py --config {config_path} --upload-to-box")
    
    # Also create a command line example
    print("\nOr use command line directly:")
    print("  python main.py --source fws_wetlands_fl --location disney_animal_kingdom")
    
    return config_path


def show_environment_setup():
    """Show how to set up environment variables."""
    print("\n" + "="*60)
    print("ENVIRONMENT SETUP")
    print("="*60)
    print("\nCreate a .env file with your Box credentials:")
    print("  cp .env.example .env")
    print("  # Edit .env with your credentials")
    print("\nOr export them directly:")
    print("  export BOX_CLIENT_ID='your_client_id'")
    print("  export BOX_CLIENT_SECRET='your_client_secret'")
    print("  export BOX_ACCESS_TOKEN='your_access_token'")
    print("  export BOX_FOLDER_ID='your_folder_id'")
    

def main():
    print("Migration Helper - Old Script to New System")
    print("="*60)
    
    # Check if old script exists
    old_script = Path("duckdb_extract.py")
    if old_script.exists():
        print(f"Found old script: {old_script}")
        print("The new system provides:")
        print("  - Parameterized functions")
        print("  - Configuration file support")
        print("  - Better error handling")
        print("  - Multiple output formats")
        print("  - Extensible architecture")
        
    # Create migration config
    config_path = migrate_old_script()
    
    # Show environment setup
    show_environment_setup()
    
    print("\n" + "="*60)
    print("NEXT STEPS")
    print("="*60)
    print("1. Review the generated configuration file")
    print("2. Set up your environment variables")
    print("3. Run the new extraction script")
    print("4. (Optional) Remove the old duckdb_extract.py file")
    

if __name__ == "__main__":
    main()