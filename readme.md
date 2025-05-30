# Supabase CSV Importer

High-performance CSV importer for Supabase. Handles files from 100MB to 50GB+ with automatic optimization.

## Quick Start

### 1. Install
```bash
pip install -r requirements.txt
```

### 2. Configure
Edit `config.yaml`:
```yaml
database:
  connection_string: "postgresql://postgres.xxx:password@xxx.supabase.com:5432/postgres"
  table_name: "your_table"
```

### 3. Run
```bash
# Basic import
python main.py data.csv

# With performance optimization (recommended)
python main.py data.csv --optimize

# Direct optimization
python main.py data.csv --instance-size 5 --performance-level 3
```

## Performance Optimization

### Instance Sizes
- **1-3**: Nano/Micro/Small (0.5-2GB RAM)
- **4-6**: Medium/Large/XL (4-16GB RAM)  
- **7-11**: 2XL-16XL (32-256GB RAM)

### Performance Levels
- **1**: Conservative (stable, slower)
- **2**: Balanced (recommended)
- **3**: Aggressive (fastest, monitor for errors)

## Common Issues

**Timeouts**: Increase in `config.yaml`:
```yaml
optimization:
  statement_timeout: "2h"  # or "0" for no timeout
```

**Connection errors**: Reduce workers:
```yaml
import:
  parallel_workers: 2  # reduce this
```

**Out of memory**: Reduce chunk size:
```yaml
import:
  chunk_size_mb: 50  # reduce this
```

## Examples

```bash
# Generate test data
python generate_test_csv.py 500  # Creates 500MB test file

# Import with optimization wizard
python main.py test_data_500mb.csv --optimize

# Import directory of CSVs
python main.py ./csv_folder/

# Dry run (analyze only)
python main.py data.csv --dry-run
```

## Key Features
- Automatic file splitting for large files
- Parallel chunk processing
- COPY command for speed (falls back to INSERT)
- Progress tracking with ETA
- Automatic retry on failures
- Database optimization during import

## Performance Tips
1. Use `--optimize` for automatic tuning
2. Import during off-peak hours
3. Larger instances = faster imports
4. Close other apps to free local resources

That's it! The importer handles everything else automatically.