"""
AGRIS Open Data Set Catalog Parser
Extracts dataset information from the AGRIS.ODS.xml catalog file.

This catalog contains metadata about agricultural research datasets from
institutions worldwide. Each dataset can be downloaded and searched for
pest risk information.
"""

import xml.etree.ElementTree as ET
import json
from typing import List, Dict
from collections import defaultdict


class AGRISCatalogParser:
    """Parse AGRIS Open Data Set catalog to extract dataset information."""
    
    # XML namespaces used in the AGRIS catalog
    NAMESPACES = {
        'dcat': 'http://www.w3.org/ns/dcat#',
        'dc': 'http://purl.org/dc/elements/1.1/',
        'dct': 'http://purl.org/dc/terms/'
    }
    
    def __init__(self, xml_file_path: str):
        """
        Initialize the parser with the path to AGRIS.ODS.xml file.
        
        Args:
            xml_file_path: Path to the AGRIS.ODS.xml catalog file
        """
        self.xml_file_path = xml_file_path
        self.tree = None
        self.root = None
        self.datasets = []
        
    def parse(self) -> List[Dict]:
        """
        Parse the XML catalog and extract all dataset information.
        
        Returns:
            List of dictionaries containing dataset metadata
        """
        print(f"Parsing {self.xml_file_path}...")
        
        # Parse XML file
        self.tree = ET.parse(self.xml_file_path)
        self.root = self.tree.getroot()
        
        # Find all dataset elements
        datasets = self.root.findall('.//dcat:Dataset', self.NAMESPACES)
        print(f"Found {len(datasets)} datasets in catalog")
        
        # Extract information from each dataset
        for dataset in datasets:
            dataset_info = self._extract_dataset_info(dataset)
            self.datasets.append(dataset_info)
        
        return self.datasets
    
    def _extract_dataset_info(self, dataset_elem) -> Dict:
        """
        Extract information from a single dataset element.
        
        Args:
            dataset_elem: XML element representing a dataset
            
        Returns:
            Dictionary containing dataset metadata
        """
        # Get dataset ID from XML attribute
        dataset_id = dataset_elem.get('{http://www.w3.org/XML/1998/namespace}id', 'Unknown')
        
        # Extract basic metadata
        identifier = self._get_text(dataset_elem, 'dc:identifier')
        title = self._get_text(dataset_elem, 'dc:title')
        description = self._get_text(dataset_elem, 'dc:description')
        creator = self._get_text(dataset_elem, 'dc:creator')
        publisher = self._get_text(dataset_elem, 'dc:publisher')
        modified = self._get_text(dataset_elem, 'dct:modified')
        
        # Extract subjects/keywords
        subjects = [elem.text for elem in dataset_elem.findall('dc:subject', self.NAMESPACES) if elem.text]
        
        # Extract download URL
        download_url = None
        distribution = dataset_elem.find('.//dcat:Distribution', self.NAMESPACES)
        if distribution is not None:
            url_elem = distribution.find('dcat:downloadURL', self.NAMESPACES)
            if url_elem is not None:
                download_url = url_elem.text
        
        return {
            'id': dataset_id,
            'identifier': identifier,
            'title': title,
            'description': description,
            'creator': creator,
            'publisher': publisher,
            'modified': modified,
            'subjects': subjects,
            'download_url': download_url
        }
    
    def _get_text(self, parent_elem, tag_path: str) -> str:
        """
        Get text content from an XML element.
        
        Args:
            parent_elem: Parent XML element
            tag_path: Path to the child element (e.g., 'dc:title')
            
        Returns:
            Text content or empty string if not found
        """
        elem = parent_elem.find(tag_path, self.NAMESPACES)
        return elem.text if elem is not None and elem.text else ''
    
    def get_datasets_by_country(self, country_code: str) -> List[Dict]:
        """
        Filter datasets by country code (based on dataset ID prefix).
        
        Args:
            country_code: Two-letter country code (e.g., 'TN' for Tunisia)
            
        Returns:
            List of datasets from that country
        """
        return [ds for ds in self.datasets if ds['id'].startswith(country_code)]
    
    def get_datasets_by_keyword(self, keyword: str) -> List[Dict]:
        """
        Search datasets by keyword in title, description, or subjects.
        
        Args:
            keyword: Keyword to search for (case-insensitive)
            
        Returns:
            List of matching datasets
        """
        keyword_lower = keyword.lower()
        matching = []
        
        for ds in self.datasets:
            # Search in title
            if keyword_lower in ds.get('title', '').lower():
                matching.append(ds)
                continue
            
            # Search in description
            if keyword_lower in ds.get('description', '').lower():
                matching.append(ds)
                continue
            
            # Search in subjects
            subjects_str = ' '.join(ds.get('subjects', [])).lower()
            if keyword_lower in subjects_str:
                matching.append(ds)
                continue
            
            # Search in creator
            if keyword_lower in ds.get('creator', '').lower():
                matching.append(ds)
                continue
        
        return matching
    
    def get_statistics(self) -> Dict:
        """
        Get statistics about the datasets in the catalog.
        
        Returns:
            Dictionary containing various statistics
        """
        stats = {
            'total_datasets': len(self.datasets),
            'datasets_by_country': defaultdict(int),
            'top_creators': defaultdict(int),
            'datasets_with_downloads': 0
        }
        
        for ds in self.datasets:
            # Count by country (first 2 chars of ID)
            country_code = ds['id'][:2] if len(ds['id']) >= 2 else 'Unknown'
            stats['datasets_by_country'][country_code] += 1
            
            # Count by creator
            creator = ds.get('creator', 'Unknown')
            stats['top_creators'][creator] += 1
            
            # Count datasets with download URLs
            if ds.get('download_url'):
                stats['datasets_with_downloads'] += 1
        
        # Convert defaultdicts to regular dicts and sort
        stats['datasets_by_country'] = dict(sorted(
            stats['datasets_by_country'].items(),
            key=lambda x: x[1],
            reverse=True
        )[:20])  # Top 20 countries
        
        stats['top_creators'] = dict(sorted(
            stats['top_creators'].items(),
            key=lambda x: x[1],
            reverse=True
        )[:10])  # Top 10 creators
        
        return stats
    
    def save_to_json(self, output_file: str):
        """
        Save parsed datasets to a JSON file.
        
        Args:
            output_file: Path to output JSON file
        """
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(self.datasets, f, indent=2, ensure_ascii=False)
        print(f"Saved {len(self.datasets)} datasets to {output_file}")


def main():
    """Main execution function."""
    print("=" * 70)
    print("AGRIS Open Data Set Catalog Parser")
    print("=" * 70)
    print()
    
    # Initialize parser
    parser = AGRISCatalogParser('AGRIS.ODS.xml')
    
    # Parse the catalog
    datasets = parser.parse()
    print(f"\nSuccessfully parsed {len(datasets)} datasets")
    print()
    
    # Get statistics
    print("Catalog Statistics:")
    print("-" * 70)
    stats = parser.get_statistics()
    print(f"Total datasets: {stats['total_datasets']}")
    print(f"Datasets with download URLs: {stats['datasets_with_downloads']}")
    print()
    
    print("Top 10 Countries by Dataset Count:")
    for country, count in list(stats['datasets_by_country'].items())[:10]:
        print(f"  {country}: {count} datasets")
    print()
    
    # Search for pest-related datasets
    print("Searching for pest-related datasets...")
    print("-" * 70)
    pest_keywords = ['pest', 'disease', 'pathogen', 'insect', 'crop protection', 'plant health']
    all_pest_related = []
    
    for keyword in pest_keywords:
        matches = parser.get_datasets_by_keyword(keyword)
        all_pest_related.extend(matches)
        if matches:
            print(f"'{keyword}': {len(matches)} datasets found")
    
    # Remove duplicates
    unique_pest_datasets = {ds['id']: ds for ds in all_pest_related}.values()
    print(f"\nTotal unique pest-related datasets: {len(unique_pest_datasets)}")
    print()
    
    # Show Tunisia-specific datasets
    print("Tunisia-specific datasets:")
    print("-" * 70)
    tunisia_datasets = parser.get_datasets_by_country('TN')
    if tunisia_datasets:
        for ds in tunisia_datasets:
            print(f"ID: {ds['id']}")
            print(f"Title: {ds['title']}")
            print(f"Creator: {ds['creator']}")
            print(f"Download: {ds['download_url']}")
            print()
    else:
        print("No Tunisia-specific datasets found in catalog")
    print()
    
    # Save all datasets to JSON
    parser.save_to_json('agris_catalog_datasets.json')
    
    # Save pest-related datasets separately
    if unique_pest_datasets:
        with open('agris_pest_related_datasets.json', 'w', encoding='utf-8') as f:
            json.dump(list(unique_pest_datasets), f, indent=2, ensure_ascii=False)
        print(f"Saved {len(unique_pest_datasets)} pest-related datasets to agris_pest_related_datasets.json")
    
    print()
    print("Next Steps:")
    print("-" * 70)
    print("1. Download individual dataset XML files using the download URLs")
    print("2. Parse each dataset XML to search for pest risk information")
    print("3. Filter by geographic region (Tunisia, North Africa)")
    print("4. Extract temporal data to identify weekly/7-day frequency data")
    print("5. Combine with other sources (FAO GIEWS, locust monitoring)")


if __name__ == "__main__":
    main()
