#!/usr/bin/env python3
"""
HTML Table Parser for Crop Coefficient Data

This parser extracts crop coefficient data from an HTML table containing
agricultural crop information including Kc values and maximum crop heights.
"""

from bs4 import BeautifulSoup
from typing import List, Dict, Optional
import re
import json


class CropTableParser:
    """Parser for extracting crop coefficient data from HTML tables."""
    
    def __init__(self, html_file: str):
        """
        Initialize the parser with an HTML file.
        
        Args:
            html_file: Path to the HTML file containing the table
        """
        self.html_file = html_file
        self.data = []
        self.current_category = None
        self.current_crop = None
        
    def parse(self) -> List[Dict]:
        """
        Parse the HTML table and extract crop data.
        
        Returns:
            List of dictionaries containing crop information
        """
        with open(self.html_file, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        soup = BeautifulSoup(html_content, 'html.parser')
        table = soup.find('table')
        
        if not table:
            raise ValueError("No table found in HTML file")
        
        rows = table.find_all('tr')
        
        # Skip the header row (first row)
        for row in rows[1:]:
            self._process_row(row)
        
        return self.data
    
    def _clean_text(self, text: str) -> str:
        """
        Clean text by removing extra whitespace and special characters.
        
        Args:
            text: Raw text to clean
            
        Returns:
            Cleaned text
        """
        if not text:
            return ""
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        return text.strip()
    
    def _extract_numeric_value(self, text: str) -> Optional[str]:
        """
        Extract numeric value from text, handling ranges and special cases.
        
        Args:
            text: Text potentially containing numeric values
            
        Returns:
            Extracted numeric value or None
        """
        from bs4 import BeautifulSoup
        
        # Parse the text as HTML to handle superscript tags
        soup = BeautifulSoup(text, 'html.parser')
        
        # Remove superscript tags (footnote references)
        for sup in soup.find_all('sup'):
            sup.decompose()
        
        # Get the cleaned text
        text = soup.get_text()
        text = self._clean_text(text)
        
        if not text or text == '':
            return None
            
        return text
    
    def _is_category_row(self, row) -> bool:
        """
        Check if a row represents a category header.
        
        Args:
            row: BeautifulSoup row element
            
        Returns:
            True if row is a category header
        """
        cells = row.find_all('td')
        if not cells:
            return False
            
        # Check for colspan (category headers often span the table)
        if len(cells) == 1 and cells[0].get('colspan', '1') != '1':
            first_cell_text = self._clean_text(cells[0].get_text())
            # Check for bold tag
            has_bold = cells[0].find('b') is not None
            # Check for pattern "a. Category Name"
            matches_pattern = re.match(r'^[a-p]\.\s+', first_cell_text, re.IGNORECASE)
            return has_bold and matches_pattern

        if len(cells) < 2:
            return False
        
        first_cell_text = self._clean_text(cells[0].get_text())
        
        # Category rows typically have bold text and start with a letter followed by a dot
        has_bold = cells[0].find('b') is not None or cells[1].find('b') is not None
        matches_pattern = re.match(r'^[a-p]\.\s+', first_cell_text, re.IGNORECASE)
        
        return has_bold and matches_pattern
    
    def _is_subcategory_row(self, row) -> bool:
        """
        Check if a row represents a subcategory (indented crop variant).
        
        Args:
            row: BeautifulSoup row element
            
        Returns:
            True if row is a subcategory
        """
        cells = row.find_all('td')
        if len(cells) < 2:
            return False
        
        # Subcategory rows have empty first cell and text starting with '-' in second cell
        first_cell_text = self._clean_text(cells[0].get_text())
        second_cell_text = self._clean_text(cells[1].get_text())
        
        return first_cell_text == '' and second_cell_text.startswith('-')
    
    def _process_row(self, row):
        """
        Process a single table row and extract data.
        
        Args:
            row: BeautifulSoup row element
        """
        cells = row.find_all('td')
        
        if not cells:
            return
        
        # Check if this is a category row
        if self._is_category_row(row):
            self._process_category_row(cells)
            return
            
        if len(cells) < 2:
            return
        
        # Check if this is a subcategory row
        if self._is_subcategory_row(row):
            self._process_subcategory_row(cells)
            return
        
        # Regular crop row
        self._process_crop_row(cells)
    
    def _process_category_row(self, cells):
        """
        Process a category header row.
        
        Args:
            cells: List of table cells
        """
        # Extract category name from first or second cell
        category_text = self._clean_text(cells[0].get_text())
        if not category_text and len(cells) > 1:
            category_text = self._clean_text(cells[1].get_text())
        
        # Clean category name (remove "a. " prefix)
        category_text = re.sub(r'^[a-p]\.\s*', '', category_text)
        
        self.current_category = category_text
        self.current_crop = None
        
        # Determine start index for values
        # If first cell is colspan="2", values start at index 1
        # Otherwise (e.g. separate cells or indentation), values start at index 2
        value_start_idx = 1 if cells[0].get('colspan') == '2' else 2
        
        kc_ini = None
        kc_mid = None
        kc_end = None
        max_height = None
        
        # Try to extract values
        if len(cells) > value_start_idx:
            val = self._clean_text(cells[value_start_idx].get_text())
            if val and re.search(r'\d', val):
                kc_ini = self._extract_numeric_value(val)
                
        if len(cells) > value_start_idx + 1:
             val = self._clean_text(cells[value_start_idx + 1].get_text())
             if val and re.search(r'\d', val):
                kc_mid = self._extract_numeric_value(val)
                
        if len(cells) > value_start_idx + 2:
             val = self._clean_text(cells[value_start_idx + 2].get_text())
             if val and re.search(r'\d', val):
                kc_end = self._extract_numeric_value(val)
                
        if len(cells) > value_start_idx + 3:
             val = self._clean_text(cells[value_start_idx + 3].get_text())
             if val and re.search(r'\d', val):
                max_height = self._extract_numeric_value(val)
                
        # If we found at least one value, treat this category as a crop too
        if kc_ini or kc_mid or kc_end or max_height:
            # Category name is already cleaned above, so crop name is same as category
            crop_name = category_text
            
            crop_data = {
                'category': self.current_category,
                'crop': crop_name,
                'variant': None,
                'kc_ini': kc_ini,
                'kc_mid': kc_mid,
                'kc_end': kc_end,
                'max_height_m': max_height
            }
            self.data.append(crop_data)
    
    def _process_subcategory_row(self, cells):
        """
        Process a subcategory (crop variant) row.
        
        Args:
            cells: List of table cells
        """
        if not self.current_crop:
            return
        
        variant = self._clean_text(cells[1].get_text())
        # Clean variant name (remove "- " prefix)
        variant = re.sub(r'^-\s*', '', variant)
        
        # Extract values
        kc_ini = self._extract_numeric_value(cells[2].get_text()) if len(cells) > 2 else None
        kc_mid = self._extract_numeric_value(cells[3].get_text()) if len(cells) > 3 else None
        kc_end = self._extract_numeric_value(cells[4].get_text()) if len(cells) > 4 else None
        max_height = self._extract_numeric_value(cells[5].get_text()) if len(cells) > 5 else None
        
        crop_data = {
            'category': self.current_category,
            'crop': self.current_crop,
            'variant': variant,
            'kc_ini': kc_ini,
            'kc_mid': kc_mid,
            'kc_end': kc_end,
            'max_height_m': max_height
        }
        
        self.data.append(crop_data)
    
    def _process_crop_row(self, cells):
        """
        Process a regular crop data row.
        
        Args:
            cells: List of table cells
        """
        # Get crop name
        crop_name = self._clean_text(cells[0].get_text())
        
        # If first cell is empty, check second (though subcategories usually handled separately)
        if not crop_name and len(cells) > 1:
            crop_name = self._clean_text(cells[1].get_text())
        
        # Skip empty rows
        if not crop_name:
            return
        
        self.current_crop = crop_name
        
        # Determine start index for values
        value_start_idx = 1 if cells[0].get('colspan') == '2' else 2
        
        kc_ini = None
        kc_mid = None
        kc_end = None
        max_height = None
        
        # Try to extract from expected positions
        if len(cells) > value_start_idx:
            kc_ini = self._extract_numeric_value(cells[value_start_idx].get_text())
        if len(cells) > value_start_idx + 1:
            kc_mid = self._extract_numeric_value(cells[value_start_idx + 1].get_text())
        if len(cells) > value_start_idx + 2:
            kc_end = self._extract_numeric_value(cells[value_start_idx + 2].get_text())
        if len(cells) > value_start_idx + 3:
            max_height = self._extract_numeric_value(cells[value_start_idx + 3].get_text())
        
        # Only add if we have at least some data
        if kc_ini or kc_mid or kc_end or max_height:
            crop_data = {
                'category': self.current_category,
                'crop': crop_name,
                'variant': None,
                'kc_ini': kc_ini,
                'kc_mid': kc_mid,
                'kc_end': kc_end,
                'max_height_m': max_height
            }
            
            self.data.append(crop_data)
    
    def to_json(self, output_file: str = None, indent: int = 2) -> str:
        """
        Convert parsed data to JSON format.
        
        Args:
            output_file: Optional file path to save JSON
            indent: Indentation level for JSON formatting
            
        Returns:
            JSON string
        """
        json_str = json.dumps(self.data, indent=indent, ensure_ascii=False)
        
        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(json_str)
        
        return json_str
    
    def to_csv(self, output_file: str):
        """
        Convert parsed data to CSV format.
        
        Args:
            output_file: File path to save CSV
        """
        import csv
        
        if not self.data:
            return
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['category', 'crop', 'variant', 'kc_ini', 'kc_mid', 'kc_end', 'max_height_m']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            
            writer.writeheader()
            for row in self.data:
                writer.writerow(row)
    
    def print_summary(self):
        """Print a summary of the parsed data."""
        print(f"Total records parsed: {len(self.data)}")
        
        categories = {}
        for item in self.data:
            cat = item.get('category', 'Unknown')
            categories[cat] = categories.get(cat, 0) + 1
        
        print("\nRecords by category:")
        for cat, count in sorted(categories.items()):
            print(f"  {cat}: {count}")


def main():
    """Main function to demonstrate parser usage."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Parse crop coefficient data from HTML table'
    )
    parser.add_argument(
        'input_file',
        help='Input HTML file containing the table'
    )
    parser.add_argument(
        '--json',
        help='Output JSON file path',
        default=None
    )
    parser.add_argument(
        '--csv',
        help='Output CSV file path',
        default=None
    )
    parser.add_argument(
        '--summary',
        action='store_true',
        help='Print summary of parsed data'
    )
    
    args = parser.parse_args()
    
    # Parse the table
    table_parser = CropTableParser(args.input_file)
    data = table_parser.parse()
    
    # Output results
    if args.json:
        table_parser.to_json(args.json)
        print(f"JSON output saved to: {args.json}")
    
    if args.csv:
        table_parser.to_csv(args.csv)
        print(f"CSV output saved to: {args.csv}")
    
    if args.summary or (not args.json and not args.csv):
        table_parser.print_summary()
    
    # If no output specified, print first few records as example
    if not args.json and not args.csv and not args.summary:
        print("\nFirst 5 records:")
        for i, record in enumerate(data[:5], 1):
            print(f"\n{i}. {record}")


if __name__ == '__main__':
    main()
