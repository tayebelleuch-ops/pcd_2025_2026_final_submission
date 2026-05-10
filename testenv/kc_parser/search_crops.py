#!/usr/bin/env python3
"""
Search utility for crop coefficient data.
Allows searching for crops by name, category, or coefficient values.
"""

import argparse
import sys
from table_parser import CropTableParser


def search_by_name(data, query):
    """Search for crops by name (case-insensitive)."""
    query = query.lower()
    results = []
    for record in data:
        crop_name = record['crop'].lower()
        variant = record.get('variant', '') or ''
        if query in crop_name or query in variant.lower():
            results.append(record)
    return results


def search_by_category(data, category):
    """Search for crops by category."""
    category = category.lower()
    results = []
    for record in data:
        if category in record['category'].lower():
            results.append(record)
    return results


def search_by_kc_range(data, kc_type, min_val, max_val):
    """Search for crops within a Kc value range."""
    results = []
    for record in data:
        kc_value = record.get(kc_type)
        if not kc_value:
            continue
        
        try:
            # Handle ranges like "0.70-0.90"
            if '-' in kc_value:
                kc_value = kc_value.split('-')[0]
            
            value = float(kc_value)
            if min_val <= value <= max_val:
                results.append(record)
        except ValueError:
            pass
    
    return results


def print_results(results, show_all=False):
    """Print search results in a formatted way."""
    if not results:
        print("No results found.")
        return
    
    print(f"\nFound {len(results)} result(s):\n")
    
    for i, record in enumerate(results, 1):
        variant = f" ({record['variant']})" if record['variant'] else ""
        print(f"{i}. {record['crop']}{variant}")
        print(f"   Category: {record['category']}")
        print(f"   Kc values: ini={record['kc_ini']}, mid={record['kc_mid']}, end={record['kc_end']}")
        print(f"   Max height: {record['max_height_m']}m")
        print()
        
        if not show_all and i >= 10:
            remaining = len(results) - 10
            if remaining > 0:
                print(f"... and {remaining} more results (use --all to show all)")
            break


def main():
    parser = argparse.ArgumentParser(
        description='Search crop coefficient data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Search for tomato crops
  python3 search_crops.py --name tomato
  
  # Search for all vegetables
  python3 search_crops.py --category vegetable
  
  # Find crops with Kc_mid between 1.0 and 1.2
  python3 search_crops.py --kc-range mid 1.0 1.2
  
  # Search for wheat and show all results
  python3 search_crops.py --name wheat --all
        """
    )
    
    parser.add_argument(
        'html_file',
        nargs='?',
        default='html.html',
        help='HTML file to parse (default: html.html)'
    )
    
    parser.add_argument(
        '--name',
        help='Search by crop name (case-insensitive substring match)'
    )
    
    parser.add_argument(
        '--category',
        help='Search by category (case-insensitive substring match)'
    )
    
    parser.add_argument(
        '--kc-range',
        nargs=3,
        metavar=('TYPE', 'MIN', 'MAX'),
        help='Search by Kc value range. TYPE must be ini, mid, or end'
    )
    
    parser.add_argument(
        '--all',
        action='store_true',
        help='Show all results (default: limit to 10)'
    )
    
    args = parser.parse_args()
    
    # Parse the HTML file
    try:
        table_parser = CropTableParser(args.html_file)
        data = table_parser.parse()
    except FileNotFoundError:
        print(f"Error: File '{args.html_file}' not found.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error parsing file: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Perform search
    results = None
    
    if args.name:
        results = search_by_name(data, args.name)
        print(f"Searching for crops matching '{args.name}'...")
    
    elif args.category:
        results = search_by_category(data, args.category)
        print(f"Searching for crops in category '{args.category}'...")
    
    elif args.kc_range:
        kc_type, min_val, max_val = args.kc_range
        kc_field = f'kc_{kc_type}'
        
        if kc_type not in ['ini', 'mid', 'end']:
            print("Error: Kc type must be 'ini', 'mid', or 'end'", file=sys.stderr)
            sys.exit(1)
        
        try:
            min_val = float(min_val)
            max_val = float(max_val)
        except ValueError:
            print("Error: Min and max values must be numbers", file=sys.stderr)
            sys.exit(1)
        
        results = search_by_kc_range(data, kc_field, min_val, max_val)
        print(f"Searching for crops with Kc_{kc_type} between {min_val} and {max_val}...")
    
    else:
        parser.print_help()
        sys.exit(0)
    
    # Print results
    print_results(results, show_all=args.all)


if __name__ == '__main__':
    main()
