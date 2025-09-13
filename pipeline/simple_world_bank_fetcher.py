#!/usr/bin/env python3
"""
Simple World Bank Data Fetcher
Fetches World Bank data and stores it directly in MinIO without Spark
"""

import requests
import pandas as pd
import boto3
import json
import os
from datetime import datetime

# MinIO configuration
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET_NAME = "gold"

def get_s3_client():
    """Get S3 client for MinIO operations"""
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

def fetch_world_bank_data():
    """Fetch World Bank data for key indicators"""
    print("Fetching World Bank data...")
    
    # Key indicators to fetch
    indicators = {
        'NY.GDP.MKTP.CD': 'GDP (current US$)',
        'SP.POP.TOTL': 'Population, total',
        'SP.URB.TOTL.IN.ZS': 'Urban population (% of total)',
        'SE.ADT.LITR.ZS': 'Literacy rate, adult total (% of people ages 15 and above)',
        'SP.DYN.LE00.IN': 'Life expectancy at birth, total (years)',
        'NY.GDP.PCAP.CD': 'GDP per capita (current US$)',
        'IT.NET.USER.ZS': 'Individuals using the Internet (% of population)',
        'EG.USE.ELEC.KH.PC': 'Electric power consumption (kWh per capita)',
        'SH.XPD.CHEX.GD.ZS': 'Current health expenditure (% of GDP)',
        'SE.XPD.TOTL.GD.ZS': 'Government expenditure on education, total (% of GDP)'
    }
    
    all_data = []
    
    for indicator_id, indicator_name in indicators.items():
        print(f"Fetching {indicator_name} ({indicator_id})...")
        
        try:
            # Fetch data for the last 10 years
            url = f"https://api.worldbank.org/v2/country/all/indicator/{indicator_id}"
            params = {
                'format': 'json',
                'date': '2015:2024',
                'per_page': 2000
            }
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if len(data) > 1 and data[1]:
                for item in data[1]:
                    if item.get('value') is not None:
                        all_data.append({
                            'country_code': item.get('country', {}).get('id', ''),
                            'country_name': item.get('country', {}).get('value', ''),
                            'indicator_id': indicator_id,
                            'indicator_name': indicator_name,
                            'year': int(item.get('date', 0)),
                            'value': float(item.get('value', 0)),
                            'indicator_category': get_indicator_category(indicator_id)
                        })
            
            print(f"  Fetched {len([d for d in all_data if d['indicator_id'] == indicator_id])} records")
            
        except Exception as e:
            print(f"  Error fetching {indicator_name}: {e}")
            continue
    
    print(f"Total records fetched: {len(all_data)}")
    return pd.DataFrame(all_data)

def get_indicator_category(indicator_id):
    """Categorize indicators"""
    if 'GDP' in indicator_id or 'GDP' in indicator_id:
        return 'Economic'
    elif 'POP' in indicator_id or 'URB' in indicator_id:
        return 'Demographics'
    elif 'LITR' in indicator_id or 'EDU' in indicator_id or 'SE.XPD' in indicator_id:
        return 'Education'
    elif 'LE00' in indicator_id or 'HEX' in indicator_id or 'SH.XPD' in indicator_id:
        return 'Health'
    elif 'IT.NET' in indicator_id or 'ELEC' in indicator_id:
        return 'Technology'
    else:
        return 'Other'

def create_aggregated_data(df):
    """Create aggregated data for the gold layer"""
    print("Creating aggregated data...")
    
    # Country-level aggregations
    country_agg = df.groupby(['country_code', 'country_name', 'indicator_category']).agg({
        'indicator_id': 'count',
        'value': ['mean', 'max', 'min', 'std'],
        'year': 'count'
    }).round(2)
    
    # Flatten column names
    country_agg.columns = ['indicator_count', 'avg_value', 'max_value', 'min_value', 'std_value', 'year_count']
    country_agg = country_agg.reset_index()
    
    # Global averages
    global_agg = df.groupby('indicator_category').agg({
        'value': 'mean',
        'indicator_id': 'nunique'
    }).round(2)
    global_agg.columns = ['global_avg', 'unique_indicators']
    global_agg = global_agg.reset_index()
    
    return country_agg, global_agg

def main():
    """Main function"""
    print("=" * 60)
    print("SIMPLE WORLD BANK DATA FETCHER")
    print("=" * 60)
    
    # Fetch data
    df = fetch_world_bank_data()
    
    if df.empty:
        print("No data fetched!")
        return
    
    print(f"Data shape: {df.shape}")
    print("Sample data:")
    print(df.head())
    
    # Add metadata columns for bronze layer
    df_bronze = df.copy()
    df_bronze['ingestion_timestamp'] = datetime.now().isoformat()
    df_bronze['data_source'] = 'world_bank_api'
    df_bronze['layer'] = 'bronze'
    
    # Create silver layer data (cleaned and processed)
    df_silver = df.copy()
    # Remove null values
    df_silver = df_silver.dropna(subset=['value', 'year', 'country_code'])
    # Filter valid years
    df_silver = df_silver[(df_silver['year'] >= 2015) & (df_silver['year'] <= 2024)]
    # Add data quality flags
    df_silver['data_quality_flag'] = df_silver.apply(
        lambda row: 'Missing Value' if pd.isna(row['value']) 
        else 'Negative Value' if row['value'] < 0 
        else 'Missing Indicator' if pd.isna(row['indicator_id'])
        else 'Valid', axis=1
    )
    df_silver['completeness_rate'] = 100.0  # All remaining data is complete
    df_silver['layer'] = 'silver'
    
    # Create aggregated data for gold layer
    country_agg, global_agg = create_aggregated_data(df_silver)
    
    # Store in MinIO
    s3_client = get_s3_client()
    
    try:
        # BRONZE LAYER - Raw data
        print("\n" + "=" * 40)
        print("STORING BRONZE LAYER DATA")
        print("=" * 40)
        
        bronze_csv = df_bronze.to_csv(index=False)
        s3_client.put_object(
            Bucket='bronze',
            Key='raw_data/world_bank/world_bank_raw.csv',
            Body=bronze_csv,
            ContentType='text/csv'
        )
        print("✓ Raw data stored in bronze layer")
        
        # SILVER LAYER - Processed data
        print("\n" + "=" * 40)
        print("STORING SILVER LAYER DATA")
        print("=" * 40)
        
        silver_csv = df_silver.to_csv(index=False)
        s3_client.put_object(
            Bucket='silver',
            Key='processed_data/world_bank/world_bank_processed.csv',
            Body=silver_csv,
            ContentType='text/csv'
        )
        print("✓ Processed data stored in silver layer")
        
        # GOLD LAYER - Aggregated data
        print("\n" + "=" * 40)
        print("STORING GOLD LAYER DATA")
        print("=" * 40)
        
        # Store raw data as CSV
        csv_data = df_silver.to_csv(index=False)
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key='aggregated_data/world_bank_raw.csv',
            Body=csv_data,
            ContentType='text/csv'
        )
        print("✓ Raw data stored in gold layer")
        
        # Store country aggregations as CSV
        country_csv = country_agg.to_csv(index=False)
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key='aggregated_data/world_bank_country_aggregations.csv',
            Body=country_csv,
            ContentType='text/csv'
        )
        print("✓ Country aggregations stored in gold layer")
        
        # Store global aggregations as CSV
        global_csv = global_agg.to_csv(index=False)
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key='aggregated_data/world_bank_global_aggregations.csv',
            Body=global_csv,
            ContentType='text/csv'
        )
        print("✓ Global aggregations stored in gold layer")
        
        print("\n" + "=" * 60)
        print("WORLD BANK DATA SUCCESSFULLY PROCESSED!")
        print("=" * 60)
        print(f"Bronze records: {len(df_bronze)}")
        print(f"Silver records: {len(df_silver)}")
        print(f"Countries: {df_silver['country_code'].nunique()}")
        print(f"Indicators: {df_silver['indicator_id'].nunique()}")
        print(f"Years: {df_silver['year'].min()} - {df_silver['year'].max()}")
        
    except Exception as e:
        print(f"Error storing data: {e}")

if __name__ == "__main__":
    main()
