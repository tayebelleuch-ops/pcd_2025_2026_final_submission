# Weekly Pest Risk Data Extractor

## Overview
This script extracts **weekly pest risk data** (risques ravageurs - 7j) for agricultural planning in Tunisia.

## Data Element
- **Name**: risques ravageurs(7j)
- **Frequency**: Weekly (7 days)
- **Priority**: Should have
- **Sources**: FAO, IRESA

## Files
- `weekly_pest_risk.py` - Main extraction script
- `weekly_pest_risk_data.json` - Output data file

## Usage

```bash
cd testenv
python3 weekly_pest_risk.py
```

## Features

### Data Sources Implemented
1. **FAO GIEWS** (Global Information and Early Warning System)
   - Status: Requires web scraping
   - URL: https://www.fao.org/giews/

2. **FAO Locust Hub**
   - Status: Requires API research
   - Relevant for North African desert locust monitoring

3. **IRESA** (Tunisian Agricultural Research Institution)
   - Status: Requires research
   - URL: http://www.iresa.agrinet.tn

4. **FAO AGRIS** (Agricultural Science and Technology)
   - Status: API access restricted
   - Provides research papers, not real-time data

### Mock Data Generator
For testing purposes, the script includes a mock data generator that creates realistic weekly pest risk assessments with:
- Week start/end dates
- Country and region information
- Multiple pest types (locusts, aphids, whiteflies, armyworms, fruit flies)
- Risk levels (low, medium, high, very_high)
- Affected crops
- Confidence scores

## Output Format

```json
{
  "extraction_date": "2025-12-22T23:22:42.276868",
  "country": "TN",
  "region": "Tunis",
  "sources": { ... },
  "mock_data": [
    {
      "week_start": "2025-12-22",
      "week_end": "2025-12-28",
      "country": "TN",
      "region": "Tunis",
      "pest_risks": [
        {
          "pest_type": "locusts",
          "risk_level": "low",
          "affected_crops": ["wheat", "barley", "olive"],
          "confidence": 0.7
        }
      ]
    }
  ]
}
```

## Next Steps

1. **Research actual API endpoints** for FAO GIEWS and IRESA
2. **Implement web scraping** if no API is available
3. **Set up authentication** if required by data sources
4. **Validate data format** and ensure 7-day intervals
5. **Integrate with Airflow** for automated weekly extraction

## Dependencies

```bash
pip install requests
```

## Notes
- Currently, most sources require additional research to find proper API endpoints
- FAO GIEWS likely requires HTML parsing
- IRESA may not have a public API
- Mock data is provided for testing and development purposes
