# Taxonomy Reconcile

Reconcile biodiversity taxonomy CSV data with TAIBIF/TaiCOL database.

## Requirements

```bash
python3 -m venv venv
source venv/bin/activate
```

## Configuration

Edit `taibol-sample.ini`:

```ini
[settings]
input_csv = your-input.csv
output_csv = output.csv

[fields]
unit_id = 遺傳物質標本號
species_name = 物種學名
family_name = 科名
# ... more field mappings
```

## Usage

### Basic usage (use INI settings)
```bash
venv/bin/python3 reconcile.py -c taibol-sample.ini
```

### With logging
```bash
venv/bin/python3 reconcile.py -c taibol-sample.ini -s -v
```

### Override INI settings
```bash
venv/bin/python3 reconcile.py -c taibol-sample.ini -i input.csv -o output.csv -s -v
```

### Verbosity levels
- No flag: Warnings only (quiet)
- `-v`: Info level (recommended)
- `-vv`: Debug level (detailed)

## Output

**Main output**: CSV file with enriched taxonomy data
- Adds: kingdom_name, phylum_name, class_name, order_name, genus_name
- Adds: Chinese common names (*_zh fields)
- Adds: __source (taicol/col/gbif), __namecode

**Unmatched log**: `{output}_unmatched.csv` (if any species failed to match)
- Contains: unit_id, species_name, family_name

## Example

```bash
venv/bin/python3 reconcile.py -c taibol-sample.ini -s -v
```

Output:
```
13:43:50 - INFO - Starting reconciliation: 114-mid-sample.csv -> out.csv
13:43:51 - INFO - Reconciling: Ursus thibetanus formosanus
13:43:51 - INFO -   ✓ Matched: Ursus thibetanus formosanus -> Animalia/Ursus (source: taicol)
...
13:44:37 - INFO - Processed 236 rows
13:44:37 - INFO - Unique species reconciled: 94
13:44:37 - INFO - Successfully matched: 91/94 (96.8%)
13:44:37 - WARNING - Unmatched records (5): out_unmatched.csv
```
