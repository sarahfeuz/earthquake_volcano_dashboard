#!/usr/bin/env python3
"""
World Bank Data Fetcher
"""

import requests
import pandas as pd
import time
from datetime import datetime

# Countries for economic and disaster risk analysis (reduced for memory efficiency)
COUNTRIES = [
    "MX",  # Mexico
    "ID",  # Indonesia
    "GR",  # Greece
    "TR",  # Turkey
    "CN",  # China
    "JP",  # Japan
    "US",  # United States
    "CL",  # Chile
    "RU",  # Russia
    "IN"   # India
]

# Economic and Disaster Risk Indicators (reduced for memory efficiency)
INDICATORS = [
    "NY.GDP.MKTP.CD",      # GDP (current US$) – total economic output at current prices
    "NY.GDP.MKTP.KD.ZG",   # GDP growth (annual %) – year-over-year growth rate of GDP
    "FP.CPI.TOTL.ZG",      # Inflation, consumer prices (annual %) – price level changes
    "EN.POP.DNST"          # Population density (people per sq. km of land area)
]

# Indicator descriptions for better labeling
INDICATOR_DESCRIPTIONS = {
    "NY.GDP.MKTP.CD": "GDP (current US$)",
    "NY.GDP.MKTP.KD.ZG": "GDP Growth (annual %)",
    "FP.CPI.TOTL.ZG": "Inflation, Consumer Prices (annual %)",
    "EN.POP.DNST": "Population Density"
}

# Indicator categories for analysis
INDICATOR_CATEGORIES = {
    "NY.GDP.MKTP.CD": "Economic",
    "NY.GDP.MKTP.KD.ZG": "Economic",
    "FP.CPI.TOTL.ZG": "Economic",
    "EN.POP.DNST": "Demographic"
}

def get_country_name(country_code):
    """Get country name from country code"""
    country_names = {
        "MX": "Mexico", "ID": "Indonesia", "GR": "Greece", "TR": "Turkey",
        "CN": "China", "JP": "Japan", "IR": "Iran", "US": "United States",
        "CL": "Chile", "RU": "Russia", "NP": "Nepal", "IN": "India",
        "PK": "Pakistan", "PH": "Philippines", "HN": "Honduras",
        "NI": "Nicaragua", "TO": "Tonga", "TJ": "Tajikistan",
        "AR": "Argentina", "AF": "Afghanistan"
    }
    return country_names.get(country_code, country_code)

def fetch_world_bank_data(country_code, indicator, start_year=2015, end_year=2025):
    """Fetch data for a specific country and indicator"""
    url = f"https://api.worldbank.org/v2/country/{country_code}/indicator/{indicator}"
    params = {
        'format': 'json',
        'date': f"{start_year}:{end_year}",
        'per_page': 1000
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if len(data) > 1 and data[1]:
            return data[1]  # Return the actual data
        else:
            return []
            
    except Exception as e:
        print(f"Error fetching {indicator} for {country_code}: {e}")
        return []

def fetch_all_world_bank_data():
    """Fetch all World Bank data for specified countries and indicators"""
    print("=" * 60)
    print("FETCHING WORLD BANK DATA")
    print("=" * 60)
    
    all_data = []
    total_requests = len(COUNTRIES) * len(INDICATORS)
    current_request = 0
    
    for country_code in COUNTRIES:
        country_name = get_country_name(country_code)
        print(f"\n Processing {country_name} ({country_code})...")
        
        for indicator in INDICATORS:
            current_request += 1
            indicator_name = INDICATOR_DESCRIPTIONS.get(indicator, indicator)
            print(f"  [{current_request}/{total_requests}] {indicator_name}...")
            
            data = fetch_world_bank_data(country_code, indicator)
            
            for entry in data:
                if entry.get('value') is not None:
                    all_data.append({
                        'country_code': country_code,
                        'country_name': country_name,
                        'indicator_id': indicator,
                        'indicator_name': indicator_name,
                        'indicator_category': INDICATOR_CATEGORIES.get(indicator, 'Other'),
                        'year': entry.get('date'),
                        'value': entry.get('value'),
                        'unit': entry.get('unit', ''),
                        'obs_status': entry.get('obs_status', ''),
                        'decimal': entry.get('decimal', 0),
                        'indicator_description': entry.get('indicator', {}).get('value', indicator_name),
                        'source': entry.get('source', {}).get('value', 'World Bank')
                    })
            
            # Be respectful to the API - add a small delay
            time.sleep(0.1)
    
    return all_data

# Note: Sample/mock data generation has been removed. Only real World Bank API
# responses are used. If the API is unavailable, the pipeline will return
# an empty DataFrame rather than fabricating data.

def fetch_world_bank_data_main():
    """Main function to fetch World Bank data"""
    print(f" Fetching World Bank data for {len(COUNTRIES)} countries")
    print(f" {len(INDICATORS)} indicators from 2015-2025")
    print("=" * 60)
    
    try:
        # Fetch real data from the API only
        data = fetch_all_world_bank_data()
        if data:
            print(f"Successfully fetched {len(data)} data points from World Bank API")
        else:
            print("  No data fetched from API")
    except Exception as e:
        print(f"Error fetching from World Bank API: {e}")
        data = []
    
    if data:
        df = pd.DataFrame(data)
        
        # Convert year to numeric
        df['year'] = pd.to_numeric(df['year'], errors='coerce')
        
        # Filter to valid years
        df = df[df['year'].between(2015, 2025)]
        
        # Ensure all columns have proper data types for Spark
        df['country_code'] = df['country_code'].astype(str)
        df['country_name'] = df['country_name'].astype(str)
        df['indicator_id'] = df['indicator_id'].astype(str)
        df['indicator_name'] = df['indicator_name'].astype(str)
        df['indicator_category'] = df['indicator_category'].astype(str)
        df['year'] = df['year'].astype('int32')
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        df['unit'] = df['unit'].astype(str)
        df['obs_status'] = df['obs_status'].astype(str)
        df['decimal'] = df['decimal'].astype('int32')
        df['indicator_description'] = df['indicator_description'].astype(str)
        df['source'] = df['source'].astype(str)
        
        # Remove any rows with null values in critical columns
        df = df.dropna(subset=['country_code', 'indicator_id', 'year', 'value'])
        
        print(f" Fetched {len(df)} data points")
        print(f" Data shape: {df.shape}")
        print(f" Countries: {df['country_name'].nunique()}")
        print(f" Indicators: {df['indicator_name'].nunique()}")
        print(f" Years: {df['year'].min()} - {df['year'].max()}")
        
        return df
    else:
        print(" No data was fetched")
        return pd.DataFrame()

if __name__ == "__main__":
    fetch_world_bank_data_main() 