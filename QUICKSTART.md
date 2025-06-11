# Quick Start Guide

## 1. Installation (5 minutes)

```bash
# Clone repository
git clone <repository-url>
cd duckdb-spatial-extractor

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## 2. Configuration (2 minutes)

```bash
# Copy environment template
cp .env.example .env

# Edit .env with your Box.com credentials
# Get these from: https://app.box.com/developers/console
```

## 3. Validate Setup (1 minute)

```bash
python validate_setup.py
```

## 4. Run Your First Extraction

### Option A: Using Predefined Location
```bash
python main.py \
  --source fws_wetlands_fl \
  --location disney_animal_kingdom \
  --formats geojson gpkg
```

### Option B: Using Custom Bounding Box
```bash
python main.py \
  --source fws_wetlands_fl \
  --bbox -81.61 28.34 -81.56 28.37 \
  --formats geojson
```

### Option C: Using Configuration File
```bash
python main.py --config example_config.json
```

## 5. Upload to Box

Add the `--upload-to-box` flag to any command:

```bash
python main.py --config example_config.json --upload-to-box
```

## Common Use Cases

### Extract Multiple Formats
```bash
python main.py \
  --source fws_wetlands_fl \
  --location disney_animal_kingdom \
  --formats geojson shapefile gpkg csv
```

### Custom Output Directory
```bash
python main.py \
  --config example_config.json \
  --output-dir /path/to/output
```

### Create Box Subfolder
```bash
python main.py \
  --config example_config.json \
  --upload-to-box \
  --box-folder-name "2024-01-Extractions"
```

### Debug Mode
```bash
python main.py \
  --config example_config.json \
  --log-level DEBUG
```

## Docker Usage

### Build and Run
```bash
docker-compose build
docker-compose run --rm extractor python main.py --config /app/configs/example_config.json
```

### Development Mode
```bash
docker-compose run --rm dev
# Inside container:
python main.py --help
```

## Troubleshooting

### Missing GDAL/FileGDB Support
```bash
# Ubuntu/Debian
sudo apt-get install gdal-bin libgdal-dev

# macOS
brew install gdal

# Windows (use OSGeo4W or conda)
conda install -c conda-forge gdal
```

### Box Upload Fails
1. Check credentials in `.env`
2. Verify folder permissions in Box
3. Check network connectivity
4. Review logs for specific errors

### Memory Issues with Large Datasets
Edit `data_extractor.py` line 34:
```python
self.connection = duckdb.connect(database=':memory:', config={'memory_limit': '4GB'})
```

## Next Steps

1. Review [README.md](README.md) for detailed documentation
2. Customize `config.py` with your data sources
3. Set up automated workflows with Kestra
4. Explore the Python API for integration