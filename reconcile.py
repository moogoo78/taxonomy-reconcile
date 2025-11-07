import argparse
import sys
import csv
import requests
import jq
import configparser
import logging

RECONCILE_MAP = {
    "kingdom": "kingdom_name",
    "phylum": "phylum_name",
    "class": "class_name",
    "order": "order_name",
    "genus": "genus_name",
    "source": "__source",
    "namecode": "__namecode"
}
RECONCILE_MAP2 ={
    "Kingdom": "kingdom_name_zh",
    "Phylum": "phylum_name_zh",
    "Class": "class_name_zh",
    "Order": "order_name_zh",
    "Family": "family_name_zh",
    "Genus": "genus_name_zh",
}


def process_reconcile(value, stats, logger):
    """Process reconciliation for a given value"""
    ## apply higher order and common name
    url = f'https://match.taibif.tw/v2/api.php?names={value}&format=json'
    logger.info(f"Reconciling: {value}")
    logger.debug(f"API URL: {url}")

    response = requests.get(url)
    resp_json = response.json()

    # Use "best" match strategy (hardcoded)
    # if result := jq.compile(".data[0][0].results | max_by(.score)").input(resp_json).first():
    #     record = {}
    #     for k, v in result.items():
    #         if mapped_key := RECONCILE_MAP.get(k):
    #             record[mapped_key] = v
    #     records = [record]
    #     logger.info(f"  ✓ Matched: {value} -> {record.get('kingdom_name', 'N/A')}/{record.get('genus_name', 'N/A')} (source: {record.get('__source', 'N/A')})")
    #     return records

    data = {}
    if results := jq.compile('.data[0][0].results').input(resp_json).first():
        for res in results:
            if res['source'] == 'taicol':
                data[RECONCILE_MAP['namecode']] = res['accepted_namecode']
                data[RECONCILE_MAP['source']] = res['source']
                resp2 = requests.get(f"https://api.taicol.tw/v2/higherTaxa?taxon_id={res['accepted_namecode']}")
                resp2_json = resp2.json()
                for x in resp2_json['data']:
                    if x['rank'] in RECONCILE_MAP2:
                        data[RECONCILE_MAP2[x['rank']]] = x['common_name_c']
                        key = x['rank'].lower() + '_name'
                        data[key] = x['simple_name']
                break
            elif res['source'] == 'col':
                for k, v in res.items():
                    if mapped_key := RECONCILE_MAP.get(k):
                        data[mapped_key] = v
            elif res['source'] == 'gbif':
                for k, v in res.items():
                    if mapped_key := RECONCILE_MAP.get(k):
                        data[mapped_key] = v

        return data

    else:
        logger.warning(f"  ✗ No match for: {value}")
        if stats:
            logger.debug(f"    Response: {resp_json}")
        return None


def reconcile(input_csv, output_csv, ini_config, stats, logger):
    """Main reconciliation function"""
    logger.info(f"Starting reconciliation: {input_csv} -> {output_csv}")

    fout = open(output_csv, 'w')
    unmatched_records = []  # Track unmatched species

    with open(input_csv, 'r') as f:
        reader = csv.DictReader(f)

        # Build field mappings from INI file
        map_json = {}
        if ini_config:
            config = configparser.ConfigParser()
            # Try multiple encodings for INI file
            for encoding in ['utf-8', 'big5', 'gb18030', 'gbk', 'latin1']:
                try:
                    config.read(ini_config, encoding=encoding)
                    break
                except (UnicodeDecodeError, configparser.Error):
                    continue

            if 'fields' in config:
                for key, value in config['fields'].items():
                    map_json[value] = key
        else:
            # If no INI config, use identity mapping
            for x in reader.fieldnames:
                map_json[x] = x

        fieldnames = [map_json.get(x, x) for x in reader.fieldnames]

        # Add reconciliation fields to output (hardcoded)
        reconcile_headers = list(RECONCILE_MAP.values()) + list(RECONCILE_MAP2.values())
        for field_name in reconcile_headers:
            if field_name not in fieldnames:
                fieldnames.append(field_name)

        writer = csv.DictWriter(fout, fieldnames=fieldnames)
        writer.writeheader()

        reconcile_cache = {}
        row_count = 0
        for row in reader:
            row_count += 1
            if row_count % 10 == 0:
                logger.info(f"Processing row {row_count}...")
            new_dict = {}
            row_matched = True  # Track if this row matched
            current_species = None
            current_unit_id = None
            current_voucher_id = None
            current_family = None

            for key, value in row.items():
                # Map the field using INI config
                mapped_key = map_json.get(key, key)
                new_dict[mapped_key] = value

                # Track unit_id, species_name, and family_name for unmatched log
                if mapped_key == 'unit_id':
                    current_unit_id = value
                elif mapped_key == 'voucher_id':
                    current_unit_id = value
                elif mapped_key == 'species_name':
                    current_species = value
                elif mapped_key == 'family_name':
                    current_family = value

            # Process reconciliation for species_name (hardcoded)
            if current_species and current_species.strip():
                if current_species not in reconcile_cache:
                    if cache := process_reconcile(current_species, stats, logger):
                        reconcile_cache[current_species] = cache
                        new_dict.update(cache)
                    else:
                        # Mark as unmatched
                        reconcile_cache[current_species] = None
                        row_matched = False
                else:
                    if reconcile_cache[current_species] is not None:
                        logger.debug(f"  Cache hit: {current_species}")
                        new_dict.update(reconcile_cache[current_species])
                    else:
                        row_matched = False

            # Record unmatched species
            if not row_matched and current_species:
                unmatched_records.append({
                    'voucher_id': current_voucher_id or '',
                    'unit_id': current_unit_id or '',
                    'species_name': current_species,
                    'family_name': current_family or ''
                })

            writer.writerow(new_dict)

        logger.info(f"Processed {row_count} rows")
        logger.info(f"Unique species reconciled: {len(reconcile_cache)}")

        if stats:
            matched = sum(1 for v in reconcile_cache.values() if v)
            logger.info(f"Successfully matched: {matched}/{len(reconcile_cache)} ({matched/len(reconcile_cache)*100:.1f}%)")

    fout.close()
    logger.info(f"Output written to: {output_csv}")

    # Write unmatched records to log file
    if unmatched_records:
        log_file = output_csv.replace('.csv', '_unmatched.csv')
        with open(log_file, 'w', newline='') as log_out:
            log_writer = csv.DictWriter(log_out, fieldnames=['voucher_id', 'unit_id', 'species_name', 'family_name'])
            log_writer.writeheader()
            log_writer.writerows(unmatched_records)
        logger.warning(f"Unmatched records ({len(unmatched_records)}): {log_file}")
    else:
        logger.info("All records matched successfully!")


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        prog='reconcile.py',
        description='Reconcile biodiversity taxonomy CSV using INI config and reconciliation rules',
        epilog='Examples:\n'
               '  %(prog)s -c taibol-sample.ini              # Use all settings from INI\n'
               '  %(prog)s input.csv output.csv -c taibol-sample.ini\n'
               '  %(prog)s -c taibol-sample.ini -i input.csv -o output.csv',
        formatter_class=argparse.RawDescriptionHelpFormatter)

    # Optional arguments (can be in INI or command line)
    parser.add_argument('input_csv', nargs='?',
                       help='Input CSV file (or use --input/-i or set in INI)')
    parser.add_argument('output_csv', nargs='?',
                       help='Output CSV file with reconciled taxonomy data (or use --output/-o or set in INI)')

    parser.add_argument('--config', '-c',
                       required=True,
                       help='Input INI configuration file (required)')
    parser.add_argument('--input', '-i',
                       help='Input CSV file (overrides positional and INI setting)')
    parser.add_argument('--output', '-o',
                       help='Output CSV file (overrides positional and INI setting)')

    parser.add_argument('--stats', '-s',
                       action='store_true',
                       help='Display reconciliation statistics')

    parser.add_argument('--verbose', '-v',
                       action='count',
                       default=0,
                       help='Increase verbosity (can be used multiple times: -v, -vv, -vvv)')

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()

    # Setup logging
    log_levels = [logging.WARNING, logging.INFO, logging.DEBUG]
    log_level = log_levels[min(args.verbose, len(log_levels) - 1)]

    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    logger = logging.getLogger(__name__)

    # Read settings from INI file
    config = configparser.ConfigParser()
    ini_settings = {}

    # Validate and read INI config
    if args.config:
        try:
            with open(args.config, 'r') as f:
                pass
            # Try multiple encodings
            for encoding in ['utf-8', 'big5', 'gb18030', 'gbk', 'latin1']:
                try:
                    config.read(args.config, encoding=encoding)
                    break
                except (UnicodeDecodeError, configparser.Error):
                    continue

            if 'settings' in config:
                ini_settings = dict(config['settings'])
                logger.debug(f"INI settings loaded: {ini_settings}")
        except FileNotFoundError:
            print(f"Error: Config file '{args.config}' not found.", file=sys.stderr)
            sys.exit(1)

    # Determine input_csv: --input > positional > INI
    input_csv = args.input or args.input_csv or ini_settings.get('input_csv')
    if not input_csv:
        print("Error: No input CSV file specified (use positional arg, --input, or set in INI)", file=sys.stderr)
        sys.exit(1)

    # Determine output_csv: --output > positional > INI
    output_csv = args.output or args.output_csv or ini_settings.get('output_csv')
    if not output_csv:
        print("Error: No output CSV file specified (use positional arg, --output, or set in INI)", file=sys.stderr)
        sys.exit(1)

    # Validate input file
    try:
        with open(input_csv, 'r') as f:
            pass
    except FileNotFoundError:
        print(f"Error: Input file '{input_csv}' not found.", file=sys.stderr)
        sys.exit(1)
    except PermissionError:
        print(f"Error: Permission denied reading '{input_csv}'.", file=sys.stderr)
        sys.exit(1)

    reconcile(input_csv, output_csv, args.config, args.stats, logger)
