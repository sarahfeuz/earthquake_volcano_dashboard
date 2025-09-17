#!/usr/bin/env python3
"""
Data Pipeline Visualization Dashboard
"""

import dash
from dash import dcc, html, Input, Output, callback_context
import dash_bootstrap_components as dbc
from dash import dash_table
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import boto3
import io
from datetime import datetime
from config.minio_config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, BUCKETS
import os
import json
import numpy as np

# Initialize Dash app with Bootstrap theme
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "Earthquake Dashboard"

# Add custom CSS for better dropdown spacing
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            .Select-menu-outer {
                max-height: 400px !important;
            }
            .Select-menu {
                padding: 5px 0 !important;
            }
            .Select-option {
                padding: 20px 15px !important;
                line-height: 1.6 !important;
                white-space: normal !important;
                word-wrap: break-word !important;
                min-height: 60px !important;
                display: flex !important;
                align-items: center !important;
            }
            .Select-option:hover {
                background-color: #f8f9fa !important;
            }
            .Select-option.is-selected {
                background-color: #007bff !important;
                color: white !important;
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

def get_s3_client():
    """Get S3 client for MinIO"""
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

def read_streaming_data():
    """Read streaming earthquake data from MinIO"""
    print(f" Reading streaming earthquake data at {datetime.now().strftime('%H:%M:%S')}")
    s3_client = get_s3_client()
    
    try:
        # Read from streaming bucket - look for earthquake data
        response = s3_client.list_objects_v2(Bucket=BUCKETS['streaming'], Prefix='earthquake_data/')
        objects = response.get('Contents', [])
        
        if not objects:
            print("No streaming earthquake data found")
            return pd.DataFrame()
        
        # Get the most recent processed earthquake file
        json_files = [obj for obj in objects if obj['Key'].endswith('.json')]
        if not json_files:
            print("No processed earthquake files found")
            return pd.DataFrame()
        
        # Sort by last modified and get the latest
        json_files.sort(key=lambda x: x['LastModified'], reverse=True)
        latest_file = json_files[0]
        
        print(f"Reading earthquake data: {latest_file['Key']}")
        
        # Read the file
        response = s3_client.get_object(Bucket=BUCKETS['streaming'], Key=latest_file['Key'])
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content)
        
        # Process GeoJSON data to extract earthquake features
        if 'features' in data:
            earthquake_list = []
            for feature in data['features']:
                if 'properties' in feature and 'geometry' in feature:
                    earthquake = {
                        'magnitude': feature['properties'].get('mag', 0),
                        'place': feature['properties'].get('place', 'Unknown'),
                        'time': feature['properties'].get('time', 0),
                        'latitude': feature['geometry']['coordinates'][1] if 'coordinates' in feature['geometry'] else 0,
                        'longitude': feature['geometry']['coordinates'][0] if 'coordinates' in feature['geometry'] else 0,
                        'depth': feature['geometry']['coordinates'][2] if len(feature['geometry'].get('coordinates', [])) > 2 else 0
                    }
                    earthquake_list.append(earthquake)
            
            df = pd.DataFrame(earthquake_list)
            print(f" Earthquake data loaded: {df.shape}")
            return df
        else:
            print("No features found in GeoJSON data")
            return pd.DataFrame()
        
    except Exception as e:
        print(f"Error reading streaming earthquake data: {e}")
        return pd.DataFrame()

def read_static_data():
    """Read static World Bank data, prioritizing raw bronze, then silver, then gold."""
    print(f" Reading static World Bank data at {datetime.now().strftime('%H:%M:%S')}")
    s3_client = get_s3_client()

    def read_csv_from(bucket, key):
        resp = s3_client.get_object(Bucket=bucket, Key=key)
        content = resp['Body'].read().decode('utf-8')
        return pd.read_csv(io.StringIO(content))

    try:
        # 1) Prefer bronze raw CSV
        try:
            print(f" Attempting to read from bronze bucket: {BUCKETS['bronze']}")
            resp = s3_client.list_objects_v2(Bucket=BUCKETS['bronze'], Prefix='raw_data/')
            objs = resp.get('Contents', [])
            print(f" Bronze objects found: {[o['Key'] for o in objs]}")
            raw = [o for o in objs if o['Key'].endswith('world_bank_raw.csv')]
            if raw:
                print(f" Using bronze file: {raw[0]['Key']}")
                df = read_csv_from(BUCKETS['bronze'], raw[0]['Key'])
                print(f" Static data loaded from bronze: {df.shape}")
                return df
            else:
                print(" No world_bank_raw.csv found in bronze bucket")
        except Exception as e:
            print(f"Bronze read attempt failed: {e}")

        # 2) Fallback to silver processed CSV
        try:
            resp = s3_client.list_objects_v2(Bucket=BUCKETS['silver'], Prefix='processed_data/')
            objs = resp.get('Contents', [])
            proc = [o for o in objs if o['Key'].endswith('world_bank_processed.csv')]
            if proc:
                print(f"Using silver file: {proc[0]['Key']}")
                df = read_csv_from(BUCKETS['silver'], proc[0]['Key'])
                print(f"Static data loaded from silver: {df.shape}")
                return df
        except Exception as e:
            print(f"Silver read attempt failed: {e}")

        # 3) Last resort: gold aggregated CSV (note: aggregated, not per-year detail)
        try:
            resp = s3_client.list_objects_v2(Bucket=BUCKETS['gold'], Prefix='aggregated_data/')
            objs = resp.get('Contents', [])
            gold_csvs = [o for o in objs if o['Key'].endswith('.csv')]
            if gold_csvs:
                # pick latest by LastModified
                latest = sorted(gold_csvs, key=lambda x: x['LastModified'], reverse=True)[0]
                print(f"Using gold aggregated file: {latest['Key']}")
                df = read_csv_from(BUCKETS['gold'], latest['Key'])
                print(f"Static data loaded from gold: {df.shape}")
                return df
        except Exception as e:
            print(f"Gold read attempt failed: {e}")

        print("No static data found in bronze/silver/gold")
        return pd.DataFrame()

    except Exception as e:
        print(f"Error reading static data: {e}")
        return pd.DataFrame()

def read_volcano_data():
    """Read volcano eruption data from MinIO"""
    print(f" Reading volcano eruption data at {datetime.now().strftime('%H:%M:%S')}")
    s3_client = get_s3_client()
    
    try:
        # Read from streaming bucket - look for volcano data
        response = s3_client.list_objects_v2(Bucket=BUCKETS['streaming'], Prefix='volcano_data/')
        objects = response.get('Contents', [])
        
        if not objects:
            print("No volcano data found")
            return pd.DataFrame()
        
        # Get the most recent volcano file
        json_files = [obj for obj in objects if obj['Key'].endswith('.json')]
        if not json_files:
            print("No volcano files found")
            return pd.DataFrame()
        
        # Sort by last modified and get the latest
        json_files.sort(key=lambda x: x['LastModified'], reverse=True)
        latest_file = json_files[0]
        
        print(f"Reading volcano data: {latest_file['Key']}")
        
        # Read the file
        response = s3_client.get_object(Bucket=BUCKETS['streaming'], Key=latest_file['Key'])
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content)
        
        # Extract events from the data
        events = data.get('events', [])
        if not events:
            print("No volcano events found in data")
            return pd.DataFrame()
        
        # Convert to DataFrame
        volcano_list = []
        for event in events:
            volcano = {
                'id': event.get('id'),
                'title': event.get('title'),
                'description': event.get('description'),
                'category': event.get('category'),
                'status': event.get('status')
            }
            
            # Extract geometry
            geometries = event.get('geometry', [])
            if geometries:
                # Get the most recent geometry
                latest_geom = geometries[-1]
                volcano['latitude'] = latest_geom.get('coordinates', [0, 0])[1]
                volcano['longitude'] = latest_geom.get('coordinates', [0, 0])[0]
                volcano['date'] = latest_geom.get('date')
            else:
                volcano['latitude'] = None
                volcano['longitude'] = None
                volcano['date'] = None
            
            volcano_list.append(volcano)
        
        df = pd.DataFrame(volcano_list)
        print(f" Volcano data loaded: {df.shape}")
        return df
        
    except Exception as e:
        print(f"Error reading volcano data: {e}")
        return pd.DataFrame()

def create_earthquake_map(df, n_intervals):
    """Create earthquake map chart with bubbles showing earthquake locations and volcano eruptions"""
    if df.empty:
        # Return empty map if no data
        fig = go.Figure()
        fig.add_annotation(
            text="No earthquake data available",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=16, color="gray")
        )
        fig.update_layout(
            title="Live Earthquake & Volcano Map",
            height=500
        )
        return fig
    
    # Get the most recent earthquakes (last 20)
    recent_df = df.tail(20).copy()
    
    # Convert time to datetime and create timestamp column
    recent_df['timestamp'] = pd.to_datetime(recent_df['time'], unit='ms')
    
    # Read volcano data
    volcano_df = read_volcano_data()
    print(f" Volcano data loaded: {volcano_df.shape}")
    if not volcano_df.empty:
        print(f" Sample volcano locations: {volcano_df[['title', 'latitude', 'longitude']].head(2).to_dict('records')}")
    else:
        print(" No volcano data found")
    
    # Create the map
    fig = go.Figure()
    
    # Get population density data from World Bank static data
    static_data = read_static_data()
    country_data = {}
    
    # Remove hardcoded sample population density overlay. If you want a density
    # overlay, fetch a real dataset and derive values accordingly.
    country_data = {}
    
    # Convert country codes to country names for choropleth
    country_code_to_name = {
        'US': 'United States', 'CN': 'China', 'JP': 'Japan', 'MX': 'Mexico', 'ID': 'Indonesia', 
        'TR': 'Turkey', 'RU': 'Russia', 'IN': 'India', 'BR': 'Brazil', 'CA': 'Canada', 
        'AU': 'Australia', 'DE': 'Germany', 'FR': 'France', 'IT': 'Italy', 'GB': 'United Kingdom', 
        'KR': 'South Korea', 'SA': 'Saudi Arabia', 'AR': 'Argentina', 'ZA': 'South Africa', 
        'CL': 'Chile', 'NP': 'Nepal', 'PK': 'Pakistan', 'PH': 'Philippines', 'HN': 'Honduras',
        'NI': 'Nicaragua', 'TO': 'Tonga', 'TJ': 'Tajikistan', 'AF': 'Afghanistan', 'GR': 'Greece', 'IR': 'Iran'
    }
    
    # Convert country codes to names
    country_names = {}
    for code, value in country_data.items():
        country_name = country_code_to_name.get(code, code)
        country_names[country_name] = value
    
    # Create choropleth for countries with population density
    if country_names:
        fig.add_trace(go.Choropleth(
            locations=list(country_names.keys()),
            z=list(country_names.values()),
            locationmode='country names',  # Changed from 'ISO-3' to 'country names' for better compatibility
            colorscale=[[0, 'rgba(0, 0, 128, 0.1)'], [1, 'rgba(0, 0, 128, 0.8)']],  # Light blue to dark blue (#000080) with 10% to 80% transparency
            colorbar=dict(
                title="Population Density (people/km²)",
                titleside="right",
                len=0.8,
                y=0.5
            ),
            hovertemplate="<b>%{location}</b><br>" +
                         "Population Density: %{z:.0f} people/km²<br>" +
                         "<extra></extra>",
            name="Population Density",
            showscale=False,  # Remove the color bar legend
            marker=dict(opacity=0.8),  # Increased opacity to make countries more visible
            zmid=None,  # Ensure proper color distribution
            zauto=True  # Auto-scale the color range
        ))
    
    # Add light grey overlay for countries without data
    # Get all world countries and filter out those we have data for
    all_countries = [
        'Afghanistan', 'Albania', 'Algeria', 'Andorra', 'Angola', 'Antigua and Barbuda', 'Argentina', 'Armenia', 'Australia', 'Austria',
        'Azerbaijan', 'Bahamas', 'Bahrain', 'Bangladesh', 'Barbados', 'Belarus', 'Belgium', 'Belize', 'Benin', 'Bhutan',
        'Bolivia', 'Bosnia and Herzegovina', 'Botswana', 'Brazil', 'Brunei', 'Bulgaria', 'Burkina Faso', 'Burundi', 'Cambodia', 'Cameroon',
        'Canada', 'Cape Verde', 'Central African Republic', 'Chad', 'Chile', 'China', 'Colombia', 'Comoros', 'Congo', 'Costa Rica',
        'Croatia', 'Cuba', 'Cyprus', 'Czech Republic', 'Democratic Republic of the Congo', 'Denmark', 'Djibouti', 'Dominica', 'Dominican Republic', 'East Timor',
        'Ecuador', 'Egypt', 'El Salvador', 'Equatorial Guinea', 'Eritrea', 'Estonia', 'Eswatini', 'Ethiopia', 'Fiji', 'Finland',
        'France', 'Gabon', 'Gambia', 'Georgia', 'Germany', 'Ghana', 'Greece', 'Grenada', 'Guatemala', 'Guinea',
        'Guinea-Bissau', 'Guyana', 'Haiti', 'Honduras', 'Hungary', 'Iceland', 'India', 'Indonesia', 'Iran', 'Iraq',
        'Ireland', 'Israel', 'Italy', 'Ivory Coast', 'Jamaica', 'Japan', 'Jordan', 'Kazakhstan', 'Kenya', 'Kiribati',
        'Kuwait', 'Kyrgyzstan', 'Laos', 'Latvia', 'Lebanon', 'Lesotho', 'Liberia', 'Libya', 'Liechtenstein', 'Lithuania',
        'Luxembourg', 'Madagascar', 'Malawi', 'Malaysia', 'Maldives', 'Mali', 'Malta', 'Marshall Islands', 'Mauritania', 'Mauritius',
        'Mexico', 'Micronesia', 'Moldova', 'Monaco', 'Mongolia', 'Montenegro', 'Morocco', 'Mozambique', 'Myanmar', 'Namibia',
        'Nauru', 'Nepal', 'Netherlands', 'New Zealand', 'Nicaragua', 'Niger', 'Nigeria', 'North Korea', 'North Macedonia', 'Norway',
        'Oman', 'Pakistan', 'Palau', 'Panama', 'Papua New Guinea', 'Paraguay', 'Peru', 'Philippines', 'Poland', 'Portugal',
        'Qatar', 'Romania', 'Russia', 'Rwanda', 'Saint Kitts and Nevis', 'Saint Lucia', 'Saint Vincent and the Grenadines', 'Samoa', 'San Marino', 'Sao Tome and Principe',
        'Saudi Arabia', 'Senegal', 'Serbia', 'Seychelles', 'Sierra Leone', 'Singapore', 'Slovakia', 'Slovenia', 'Solomon Islands', 'Somalia',
        'South Africa', 'South Korea', 'South Sudan', 'Spain', 'Sri Lanka', 'Sudan', 'Suriname', 'Sweden', 'Switzerland', 'Syria',
        'Taiwan', 'Tajikistan', 'Tanzania', 'Thailand', 'Togo', 'Tonga', 'Trinidad and Tobago', 'Tunisia', 'Turkey', 'Turkmenistan',
        'Tuvalu', 'Uganda', 'Ukraine', 'United Arab Emirates', 'United Kingdom', 'United States', 'Uruguay', 'Uzbekistan', 'Venezuela', 'Vietnam',
        'Yemen', 'Zambia', 'Zimbabwe'
    ]
    
    # Countries we have data for
    countries_with_data = set(country_names.keys())
    
    # Countries without data
    countries_without_data = [country for country in all_countries if country not in countries_with_data]
    
    # Add light grey overlay for countries without data
    if countries_without_data:
        fig.add_trace(go.Choropleth(
            locations=countries_without_data,
            z=[0] * len(countries_without_data),  # All same value for uniform color
            locationmode='country names',
            colorscale=[[0, 'lightgrey'], [1, 'lightgrey']],  # Uniform light grey
            showscale=False,  # No color bar for this overlay
            hovertemplate="<b>%{location}</b><br>" +
                         "No population density data available<br>" +
                         "<extra></extra>",
            name="No Data",
            marker=dict(opacity=0.4)  # Lower opacity for background countries
        ))
    
    # Add earthquake bubbles
    for idx, row in recent_df.iterrows():
        magnitude = row['magnitude']
        place = row['place']
        lat = row['latitude']
        lon = row['longitude']
        severity = row.get('severity_level', 'unknown')
        timestamp = row['timestamp']
        
        # Skip if coordinates are missing
        if pd.isna(lat) or pd.isna(lon):
            continue
        
        # Calculate bubble size based on severity level (made bigger)
        severity_size_map = {
            'low': 8,
            'moderate': 15,
            'high': 22,
            'critical': 30,
            'unknown': 12
        }
        size = severity_size_map.get(severity.lower(), 12)
        
        # Calculate color based on time (red = recent, green = older)
        # Get time difference in minutes from now
        time_diff_minutes = (pd.Timestamp.now() - timestamp).total_seconds() / 60
        
        # Color gradient: red (recent) to green (older)
        # Recent = red, 1 hour ago = yellow, older = green
        if time_diff_minutes <= 10:  # Very recent (red)
            color = '#ff0000'  # Red
        elif time_diff_minutes <= 30:  # Recent (orange-red)
            color = '#ff6600'  # Orange-red
        elif time_diff_minutes <= 60:  # Moderate (orange)
            color = '#ff9900'  # Orange
        else:  # Older (green)
            color = '#00ff00'  # Green
        
        # Add earthquake bubble
        fig.add_trace(go.Scattergeo(
            lon=[lon],
            lat=[lat],
            mode='markers',
            marker=dict(
                size=size,
                color=color,
                opacity=0.7,  # Reduced opacity to show country colors underneath
                line=dict(width=1, color='black')  # Thinner border
            ),
            text=f"M{magnitude:.1f} - {place}<br>Time: {timestamp.strftime('%H:%M:%S')}<br>Severity: {severity}<br>Age: {time_diff_minutes:.0f} min ago",
            hoverinfo='text',
            name=f"M{magnitude:.1f}",
            showlegend=False
        ))
    
    # Add volcano eruptions
    print(f" Adding {len(volcano_df)} volcano events to map")
    for idx, row in volcano_df.iterrows():
        title = row['title']
        lat = row['latitude']
        lon = row['longitude']
        date = row['date']
        
        if pd.isna(lat) or pd.isna(lon):
            print(f" Skipping {title} - missing coordinates")
            continue
        
        print(f" Adding volcano: {title} at {lat:.2f}, {lon:.2f}")
        
        # Use orange bubbles for volcanoes
        color = '#FF8C00' # Orange for volcanoes
        
        fig.add_trace(go.Scattergeo(
            lon=[lon],
            lat=[lat],
            mode='markers',
            marker=dict(
                size=12,
                color=color,
                opacity=0.8,
                line=dict(width=2, color='darkorange')
            ),
            text=f"Volcano: {title}<br>Date: {date}<br>Location: {lat:.2f}, {lon:.2f}",
            hoverinfo='text',
            name=f"Volcano: {title}",
            showlegend=False
        ))
    
    # Update layout for world map
    fig.update_layout(
        title=f" Live Earthquake & Volcano Map with Population Density Overlay (Last 20 Events) - Updated at {datetime.now().strftime('%H:%M:%S')}",
        xaxis_title="Time",
        yaxis_title="Magnitude",
        height=600,
        showlegend=True,
        hovermode='closest',
        template='plotly_white',
        geo=dict(
            scope='world',
            projection_type='natural earth',
            showland=True,
            landcolor='rgb(243, 243, 243)',
            showocean=True,
            oceancolor='rgb(204, 229, 255)',
            showlakes=True,
            lakecolor='rgb(255, 255, 255)',
            showrivers=True,
            rivercolor='rgb(255, 255, 255)',
            coastlinecolor='rgb(0, 0, 0)',
            coastlinewidth=1,
            showcountries=True,
            countrycolor='rgb(0, 0, 0)',
            countrywidth=0.5,
            showframe=False,
            framecolor='rgb(0, 0, 0)',
            framewidth=1,
            bgcolor='rgb(255, 255, 255)',
            center=dict(lon=0, lat=20),
            projection_scale=1.2
        ),
        margin=dict(l=0, r=0, t=40, b=0)
    )
    
    # Add legend for time-based colors and severity-based sizes
    legend_traces = []
    
    # Time-based color legend
    time_legend = [
        ('Very Recent (≤10 min)', '#ff0000'),  # Red
        ('Recent (10-30 min)', '#ff6600'),     # Orange-red
        ('Moderate (30-60 min)', '#ff9900'),   # Orange
        ('Older (>60 min)', '#00ff00')         # Green
    ]
    
    for label, color in time_legend:
        legend_traces.append(go.Scattergeo(
            lon=[None],
            lat=[None],
            mode='markers',
            marker=dict(size=10, color=color),
            name=f"Time: {label}",
            showlegend=True
        ))
    
    # Severity-based size legend
    severity_legend = [
        ('Low', 5),
        ('Moderate', 10),
        ('High', 15),
        ('Critical', 20)
    ]
    
    for label, size in severity_legend:
        legend_traces.append(go.Scattergeo(
            lon=[None],
            lat=[None],
            mode='markers',
            marker=dict(size=size, color='#666666', symbol='circle'),
            name=f"Severity: {label}",
            showlegend=True
        ))
    
    fig.add_traces(legend_traces)
    
    return fig

def create_earthquake_table(earthquake_data, n_intervals=0):
    """Create a table showing all earthquakes with detailed information"""
    if earthquake_data.empty:
        # Return empty table if no data
        return dash_table.DataTable(
            id='earthquake-table',
            columns=[
                {"name": "Time", "id": "timestamp"},
                {"name": "Magnitude", "id": "magnitude"},
                {"name": "Location", "id": "place"},
                {"name": "Depth (km)", "id": "depth"},
                {"name": "Severity", "id": "severity_level"},
                {"name": "Region", "id": "region"},
                {"name": "Status", "id": "status"}
            ],
            data=[],
            style_table={'overflowX': 'auto'},
            style_cell={'textAlign': 'left', 'padding': '10px'},
            style_header={'backgroundColor': 'rgb(230, 230, 230)', 'fontWeight': 'bold'},
            style_data_conditional=[
                {
                    'if': {'row_index': 'odd'},
                    'backgroundColor': 'rgb(248, 248, 248)'
                }
            ],
            page_size=10,
            sort_action='native',
            sort_mode='multi',
            filter_action='native',
            style_data={'whiteSpace': 'normal', 'height': 'auto'}
        )
    
    # Get all earthquakes (not just recent ones)
    table_df = earthquake_data.copy()
    
    # Convert time to datetime and create timestamp column
    table_df['timestamp'] = pd.to_datetime(table_df['time'], unit='ms')
    
    # Format timestamp for display
    table_df['time_display'] = table_df['timestamp'].dt.strftime('%H:%M:%S')
    table_df['date_display'] = table_df['timestamp'].dt.strftime('%Y-%m-%d')
    
    # Round magnitude to 1 decimal place
    table_df['magnitude_display'] = table_df['magnitude'].round(1)
    
    # Round depth to 1 decimal place
    table_df['depth_display'] = table_df['depth'].round(1)
    
    # Create severity color mapping
    severity_colors = {
        'critical': '#800000',  # Dark red
        'high': '#cc0000',      # Red
        'medium': '#ff6600',    # Orange
        'low': '#00ff00',       # Green
        'unknown': '#cccccc'    # Gray
    }
    
    # Prepare data for table
    table_data = []
    for idx, row in table_df.iterrows():
        severity = row.get('severity_level', 'unknown')
        color = severity_colors.get(severity, severity_colors['unknown'])
        
        table_data.append({
            'timestamp': f"{row['date_display']} {row['time_display']}",
            'magnitude': row['magnitude_display'],
            'place': row['place'],
            'depth': row['depth_display'],
            'severity_level': severity.title(),
            'region': row.get('region', 'Unknown'),
            'status': row.get('status', 'Unknown'),
            'significance': row.get('significance', 'N/A'),
            'alert': row.get('alert', 'None'),
            'tsunami': 'Yes' if row.get('tsunami') == 1 else 'No',
            'latitude': round(row['latitude'], 3),
            'longitude': round(row['longitude'], 3)
        })
    
    # Sort by timestamp (most recent first)
    table_data.sort(key=lambda x: x['timestamp'], reverse=True)
    
    # Create the table
    table = dash_table.DataTable(
        id='earthquake-table',
        columns=[
            {"name": "Time", "id": "timestamp", "type": "text"},
            {"name": "Magnitude", "id": "magnitude", "type": "numeric"},
            {"name": "Location", "id": "place", "type": "text"},
            {"name": "Depth (km)", "id": "depth", "type": "numeric"},
            {"name": "Severity", "id": "severity_level", "type": "text"},
            {"name": "Region", "id": "region", "type": "text"},
            {"name": "Status", "id": "status", "type": "text"},
            {"name": "Significance", "id": "significance", "type": "numeric"},
            {"name": "Alert", "id": "alert", "type": "text"},
            {"name": "Tsunami", "id": "tsunami", "type": "text"},
            {"name": "Latitude", "id": "latitude", "type": "numeric"},
            {"name": "Longitude", "id": "longitude", "type": "numeric"}
        ],
        data=table_data,
        style_table={
            'overflowX': 'auto',
            'maxHeight': '600px',
            'overflowY': 'auto'
        },
        style_cell={
            'textAlign': 'left',
            'padding': '8px',
            'fontSize': '12px',
            'fontFamily': 'Arial, sans-serif'
        },
        style_header={
            'backgroundColor': 'rgb(50, 50, 50)',
            'color': 'white',
            'fontWeight': 'bold',
            'textAlign': 'center',
            'fontSize': '13px'
        },
        style_data_conditional=[
            {
                'if': {'row_index': 'odd'},
                'backgroundColor': 'rgb(248, 248, 248)'
            },
            {
                'if': {'filter_query': '{severity_level} = "Critical"'},
                'backgroundColor': '#ffebee',
                'color': '#800000',
                'fontWeight': 'bold'
            },
            {
                'if': {'filter_query': '{severity_level} = "High"'},
                'backgroundColor': '#fff3e0',
                'color': '#cc0000',
                'fontWeight': 'bold'
            },
            {
                'if': {'filter_query': '{magnitude} >= 5'},
                'backgroundColor': '#fff8e1',
                'fontWeight': 'bold'
            },
            {
                'if': {'filter_query': '{tsunami} = "Yes"'},
                'backgroundColor': '#e3f2fd',
                'color': '#1565c0',
                'fontWeight': 'bold'
            }
        ],
        page_size=15,
        sort_action='native',
        sort_mode='multi',
        filter_action='native',
        style_data={'whiteSpace': 'normal', 'height': 'auto'},
        tooltip_header={
            'timestamp': 'Date and time of earthquake',
            'magnitude': 'Earthquake magnitude on Richter scale',
            'place': 'Location description',
            'depth': 'Depth below surface in kilometers',
            'severity_level': 'Severity classification',
            'region': 'Geographic region',
            'status': 'Data status (automatic/manual)',
            'significance': 'USGS significance score',
            'alert': 'Alert level (if any)',
            'tsunami': 'Tsunami warning issued',
            'latitude': 'Geographic latitude',
            'longitude': 'Geographic longitude'
        },
        tooltip_delay=0,
        tooltip_duration=None
    )
    
    return table

# Dashboard layout
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H1(" Real-Time Earthquake Dashboard", className="text-center mb-4"),
            html.P("Live earthquake monitoring with economic figures", className="text-center text-muted")
        ])
    ]),
    
    # Filter bar row
    dbc.Row([
        dbc.Col([
            html.Div([
                dbc.Button(" Manual Update", id="update-button", color="dark", className="me-3"),
                html.Span("Auto-refresh every 10 seconds", className="text-muted me-3"),
                html.Span("Country:", className="me-2 align-middle"),
                dcc.Dropdown(
                    id="country-dropdown",
                    options=[{"label": "All Countries", "value": "ALL"}],  # Will be populated dynamically
                    placeholder="Select a country...",
                    style={"width": "200px", "display": "inline-block", "verticalAlign": "middle"},
                    className="me-3"
                ),
                html.Span(" Indicator:", className="me-2 align-right", style={"marginLeft": "40px"}),
                dcc.Dropdown(
                    id="indicator-dropdown",
                    options=[],  # Will be populated dynamically
                    placeholder="Select an indicator...",
                    style={"width": "200px", "display": "inline-block", "verticalAlign": "middle", "marginLeft": "10px"},
                    className="me-3",
                    optionHeight=80
                ),
                dbc.Button(" Reset Filters", id="reset-button", color="dark", className="ms-auto")
            ], style={"display": "flex", "alignItems": "center", "flexWrap": "wrap"})
        ], width=12)
    ], className="mb-4"),
    
    # Map and Table row (side by side)
    dbc.Row([
        dbc.Col([
            dcc.Graph(
                id="earthquake-map",
                style={"height": "500px"}
            )
        ], width=8),
        dbc.Col([
            html.Div(id="earthquake-table-container")
        ], width=4)
    ], className="mb-4"),
    
    # Line chart row
    dbc.Row([
        dbc.Col([
            dcc.Graph(
                id="indicator-line-chart",
                style={"height": "400px"}
            )
        ], width=12)
    ], className="mb-4"),
    
    # Interval for auto-refresh
    dcc.Interval(
        id="interval-component",
        interval=10*1000,  # 10 seconds
        n_intervals=0
    ),
    
    # Store for selected country from map
    dcc.Store(id="selected-country-store", data="")
], fluid=True)

# Callback to populate indicator dropdown
@app.callback(
    Output('indicator-dropdown', 'options'),
    [Input('interval-component', 'n_intervals')]
)
def update_indicator_dropdown(n_intervals):
    """Update indicator dropdown options from World Bank data"""
    static_data = read_static_data()
    
    if static_data.empty or 'indicator_id' not in static_data.columns or 'indicator_name' not in static_data.columns:
        # Fallback to hardcoded options
        return [
            {"label": "GDP (current US$)", "value": "NY.GDP.MKTP.CD"},
            {"label": "Inflation, Consumer Prices (annual %)", "value": "FP.CPI.TOTL.ZG"}
        ]
    
    # Get unique indicators with data
    indicator_data = static_data.groupby('indicator_id').agg({
        'indicator_name': 'first',
        'value': 'count'
    }).reset_index()
    
    # Filter out indicators with no data
    indicator_data = indicator_data[indicator_data['value'] > 0]
    
    # Create options sorted by record count (most data first)
    options = []
    for _, row in indicator_data.sort_values('value', ascending=False).iterrows():
        options.append({
            "label": row['indicator_name'],
            "value": row['indicator_id']
        })
    
    return options

# Callback to populate country dropdown
@app.callback(
    Output('country-dropdown', 'options'),
    [Input('interval-component', 'n_intervals')]
)
def update_country_dropdown(n_intervals):
    """Update country dropdown options from World Bank data"""
    static_data = read_static_data()
    
    if static_data.empty or 'country_code' not in static_data.columns or 'country_name' not in static_data.columns:
        # Fallback to hardcoded options
        return [
            {"label": "All Countries", "value": "ALL"},
            {"label": "United States", "value": "US"},
            {"label": "China", "value": "CN"},
            {"label": "Japan", "value": "JP"},
            {"label": "Mexico", "value": "MX"},
            {"label": "Indonesia", "value": "ID"},
            {"label": "Turkey", "value": "TR"},
            {"label": "Russia", "value": "RU"},
            {"label": "India", "value": "IN"},
            {"label": "Brazil", "value": "BR"},
            {"label": "Canada", "value": "CA"},
            {"label": "Australia", "value": "AU"},
            {"label": "Germany", "value": "DE"},
            {"label": "France", "value": "FR"},
            {"label": "Italy", "value": "IT"},
            {"label": "United Kingdom", "value": "GB"},
            {"label": "South Korea", "value": "KR"},
            {"label": "Saudi Arabia", "value": "SA"},
            {"label": "Argentina", "value": "AR"},
            {"label": "South Africa", "value": "ZA"},
            {"label": "Chile", "value": "CL"},
            {"label": "Nepal", "value": "NP"},
            {"label": "Pakistan", "value": "PK"},
            {"label": "Philippines", "value": "PH"},
            {"label": "Honduras", "value": "HN"},
            {"label": "Nicaragua", "value": "NI"},
            {"label": "Tonga", "value": "TO"},
            {"label": "Tajikistan", "value": "TJ"},
            {"label": "Afghanistan", "value": "AF"},
            {"label": "Greece", "value": "GR"},
            {"label": "Iran", "value": "IR"}
        ]
    
    # Create options from World Bank data
    country_mapping = static_data[['country_code', 'country_name']].drop_duplicates()
    country_mapping = country_mapping.dropna()  # Remove any NaN values
    
    options = [{"label": "All Countries", "value": "ALL"}]
    
    # Separate individual countries from regional groupings
    # Individual countries are typically 2-letter codes and have standard country names
    individual_countries = country_mapping[
        (country_mapping['country_code'].str.len() == 2) & 
        (~country_mapping['country_name'].str.contains('Africa|Asia|Europe|America|Caribbean|Pacific|World|income|dividend|states|area|Union|IDA|IBRD|HIPC|fragile|conflict', case=False, na=False))
    ]
    
    # Add individual countries first
    for _, row in individual_countries.iterrows():
        options.append({
            "label": row['country_name'],
            "value": row['country_code']
        })
    
    # Add regional groupings (limit to most important ones)
    regional_countries = country_mapping[
        (~country_mapping['country_code'].str.len() == 2) | 
        (country_mapping['country_name'].str.contains('Africa|Asia|Europe|America|Caribbean|Pacific|World|income|dividend|states|area|Union|IDA|IBRD|HIPC|fragile|conflict', case=False, na=False))
    ]
    
    # Add only the most important regional groupings
    important_regions = regional_countries[
        regional_countries['country_name'].str.contains('World|European Union|Euro area|Arab World|East Asia|South Asia|Sub-Saharan Africa|Latin America', case=False, na=False)
    ]
    
    for _, row in important_regions.iterrows():
        options.append({
            "label": f"{row['country_name']} (Region)",
            "value": row['country_code']
        })
    
    # Sort by country name
    options[1:] = sorted(options[1:], key=lambda x: x['label'])
    
    return options

# Callback to update chart and status
@app.callback(
    [Output('earthquake-map', 'figure'),
     Output('earthquake-table-container', 'children')],
    [Input('interval-component', 'n_intervals'),
     Input('update-button', 'n_clicks')],
    prevent_initial_call=True
)
def update_chart(n_intervals, n_clicks):
    """Update earthquake map and table"""
    print(f" Updating charts (interval: {n_intervals}, clicks: {n_clicks})")
    
    # Read data
    earthquake_data = read_streaming_data()
    
    # Create charts and table
    map_fig = create_earthquake_map(earthquake_data, n_intervals)
    table = create_earthquake_table(earthquake_data, n_intervals)
    
    return map_fig, table

# Callback to update line chart based on country and indicator selection
@app.callback(
    Output('indicator-line-chart', 'figure'),
    [Input('country-dropdown', 'value'),
     Input('indicator-dropdown', 'value'),
     Input('selected-country-store', 'data')],
    prevent_initial_call=True
)
def update_line_chart(selected_country, selected_indicator, map_selected_country):
    """Update line chart based on country and indicator selection"""
    # Use dropdown selection if available, otherwise use map selection
    country = selected_country if selected_country else map_selected_country
    
    if not selected_indicator:
        # Return empty chart if no indicator selected
        fig = go.Figure()
        fig.add_annotation(
            text="Please select an indicator to view data",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=16, color="gray")
        )
        fig.update_layout(
            title="Country Indicator Over Time",
            height=400
        )
        return fig
    
    # Read static data
    static_data = read_static_data()
    
    if static_data.empty:
        return go.Figure().add_annotation(
            text="No data available",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=16, color="gray")
        )
    
    # Get country name from code - use actual World Bank data
    if not static_data.empty and 'country_code' in static_data.columns and 'country_name' in static_data.columns:
        # Create mapping from actual World Bank data
        country_mapping = static_data[['country_code', 'country_name']].drop_duplicates()
        country_code_to_name = dict(zip(country_mapping['country_code'], country_mapping['country_name']))
        print(f"Using {len(country_code_to_name)} countries from World Bank data")
    else:
        # Fallback to hardcoded mapping
        country_code_to_name = {
            'US': 'United States', 'CN': 'China', 'JP': 'Japan', 'MX': 'Mexico', 'ID': 'Indonesia', 
            'TR': 'Turkey', 'RU': 'Russia', 'IN': 'India', 'BR': 'Brazil', 'CA': 'Canada', 
            'AU': 'Australia', 'DE': 'Germany', 'FR': 'France', 'IT': 'Italy', 'GB': 'United Kingdom', 
            'KR': 'South Korea', 'SA': 'Saudi Arabia', 'AR': 'Argentina', 'ZA': 'South Africa', 
            'CL': 'Chile', 'NP': 'Nepal', 'PK': 'Pakistan', 'PH': 'Philippines', 'HN': 'Honduras',
            'NI': 'Nicaragua', 'TO': 'Tonga', 'TJ': 'Tajikistan', 'AF': 'Afghanistan', 'GR': 'Greece', 'IR': 'Iran'
        }
    
    # Initialize title variable
    title = "Country Indicator Over Time"
    
    years = list(range(2015, 2026))
    fig = go.Figure()
    
    # Define colors for different countries
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf',
              '#a6cee3', '#fb9a99', '#fdbf6f', '#cab2d6', '#ffff99', '#b15928', '#fbb4ae', '#b3cde3', '#ccebc5', '#decbe4',
              '#fed9a6', '#ffffcc', '#e5d8bd', '#fddaec', '#f2f2f2', '#b3e2cd', '#fdcdac', '#cbd5e8', '#f4cae4', '#e6f5c9']
    
    # Get indicator display name for title from actual data
    if not static_data.empty and 'indicator_id' in static_data.columns and 'indicator_name' in static_data.columns:
        # Get the actual indicator name from the data
        indicator_info = static_data[static_data['indicator_id'] == selected_indicator]
        if not indicator_info.empty:
            indicator_display_name = indicator_info['indicator_name'].iloc[0]
        else:
            indicator_display_name = selected_indicator
    else:
        # Fallback to hardcoded names
        indicator_names = {
            'NY.GDP.MKTP.CD': 'GDP (current US$)',
            'NY.GDP.MKTP.KD.ZG': 'GDP Growth (annual %)',
            'NV.AGR.TOTL.ZS': 'Agriculture (% of GDP)',
            'NV.IND.TOTL.ZS': 'Industry (% of GDP)',
            'NV.SRV.TOTL.ZS': 'Services (% of GDP)',
            'FP.CPI.TOTL.ZG': 'Inflation, Consumer Prices (annual %)',
            'EN.POP.DNST': 'Population density',
            'IP.PCR.SRCN.XQ': 'Disaster risk reduction score',
            'VC.DSR.DRPT.P3': 'People affected by disasters (%)'
        }
        indicator_display_name = indicator_names.get(selected_indicator, selected_indicator)
    
    # Filter data based on country selection
    if not static_data.empty and 'indicator_id' in static_data.columns:
        # Ensure types are correct and clean duplicates
        static_data['year'] = pd.to_numeric(static_data['year'], errors='coerce')
        static_data['value'] = pd.to_numeric(static_data['value'], errors='coerce')
        static_data = static_data.dropna(subset=['year', 'value'])
        static_data['year'] = static_data['year'].astype(int)

        # Filter data for the selected indicator
        indicator_data = static_data[static_data['indicator_id'] == selected_indicator].copy()
        # Normalize keys and restrict to ISO2 countries (exclude regions/aggregates)
        indicator_data['country_code'] = indicator_data['country_code'].astype(str).str.strip().str.upper()
        indicator_data = indicator_data[indicator_data['country_code'].str.len() == 2]
        # Deduplicate robustly across country/year
        indicator_data = (indicator_data
                          .groupby(['country_code', 'year'], as_index=False)['value']
                          .mean())
        
        if not indicator_data.empty:
            # Set title based on selection
            if country == "ALL" or not country:
                title = f" {indicator_display_name} - All Countries (2015-2025) - Click legend to filter"
                # Show all countries for the selected indicator
                countries_with_data = indicator_data['country_code'].dropna().unique()
                
                for i, country_code in enumerate(countries_with_data[:20]):  # Limit to first 20 countries
                    country_name = country_code_to_name.get(country_code, country_code)
                    
                    # Get data for this country from the pre-aggregated set
                    country_data = indicator_data[indicator_data['country_code'] == country_code].sort_values('year')
                    
                    if not country_data.empty:
                        # Sort by year and get values
                        years_data = country_data['year'].tolist()
                        values_data = country_data['value'].tolist()
                        
                        # Only add if we have data
                        if years_data and values_data:
                            fig.add_trace(go.Scatter(
                                x=years_data,
                                y=values_data,
                                mode='lines+markers',
                                name=country_name,
                                line=dict(color=colors[i % len(colors)], width=2),
                                marker=dict(size=6),
                                hovertemplate=f'<b>{country_name}</b><br>' +
                                             f'Year: %{{x}}<br>' +
                                             f'Value: %{{y:,.0f}}<br>' +
                                             '<extra></extra>',
                                customdata=[country_code] * len(years_data),
                                legendgroup=country_name
                            ))
            else:
                # Show only the selected country
                country_name = country_code_to_name.get(country, country)
                title = f" {indicator_display_name} - {country_name} (2015-2025)"
                
                # Get data for this specific country from the pre-aggregated set
                country = str(country).strip().upper()
                country_data = indicator_data[indicator_data['country_code'] == country]
                
                if not country_data.empty:
                    country_data = country_data.sort_values('year')
                    years_data = country_data['year'].tolist()
                    values_data = country_data['value'].tolist()
                    
                    if years_data and values_data:
                        fig.add_trace(go.Scatter(
                            x=years_data,
                            y=values_data,
                            mode='lines+markers',
                            name=country_name,
                            line=dict(color='#1f77b4', width=3),
                            marker=dict(size=8),
                            hovertemplate=f'<b>{country_name}</b><br>' +
                                         f'Year: %{{x}}<br>' +
                                         f'Value: %{{y:,.0f}}<br>' +
                                         '<extra></extra>'
                        ))
                    else:
                        # No data for this country/indicator combination
                        fig.add_annotation(
                            text=f"No {indicator_display_name} data available for {country_name}",
                            xref="paper", yref="paper",
                            x=0.5, y=0.5, showarrow=False,
                            font=dict(size=16, color="gray")
                        )
                else:
                    # No data for this country/indicator combination
                    fig.add_annotation(
                        text=f"No {indicator_display_name} data available for {country_name}",
                        xref="paper", yref="paper",
                        x=0.5, y=0.5, showarrow=False,
                        font=dict(size=16, color="gray")
                    )
        else:
            # No data for this indicator, show message
            fig.add_annotation(
                text=f"No data available for {selected_indicator}",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16, color="gray")
            )
    else:
        # Fallback to sample data if no real data available
        if country == "ALL" or not country:
            all_countries = list(country_code_to_name.keys())[:10]  # Limit to 10 countries
            
            for i, country_code in enumerate(all_countries):
                country_name = country_code_to_name[country_code]
                
                # Generate sample data based on indicator type and country
                if selected_indicator == "NY.GDP.MKTP.CD":  # GDP
                    base_value = 1000000000000 * (0.5 + i * 0.1)  # Vary by country
                    values = [base_value * (1 + 0.02 * (year - 2015) + np.random.normal(0, 0.05)) for year in years]
                elif selected_indicator == "FP.CPI.TOTL.ZG":  # Inflation
                    base_value = 100000000 * (0.3 + i * 0.05)  # Vary by country
                    values = [base_value * (1 + 0.01 * (year - 2015)) for year in years]
                elif selected_indicator == "EN.POP.DNST":  # Population density
                    base_value = 100 * (0.5 + i * 0.1)  # Vary by country
                    values = [base_value * (1 + 0.005 * (year - 2015)) for year in years]
                else:  # Other indicators
                    base_value = 50 * (0.5 + i * 0.1)  # Vary by country
                    values = [base_value + np.random.normal(0, 5) for year in years]
                
                fig.add_trace(go.Scatter(
                    x=years,
                    y=values,
                    mode='lines+markers',
                    name=country_name,
                    line=dict(color=colors[i % len(colors)], width=2),
                    marker=dict(size=6),
                    hovertemplate=f'<b>{country_name}</b><br>' +
                                 f'Year: %{{x}}<br>' +
                                 f'Value: %{{y:,.0f}}<br>' +
                                 '<extra></extra>',
                    customdata=[country_code] * len(years),
                    legendgroup=country_name
                ))
        else:
            # Show single country with sample data
            country_name = country_code_to_name.get(country, country)
            
            # Generate sample data for this country
            if selected_indicator == "NY.GDP.MKTP.CD":  # GDP
                base_value = 1000000000000 * 0.8
                values = [base_value * (1 + 0.02 * (year - 2015) + np.random.normal(0, 0.05)) for year in years]
            elif selected_indicator == "FP.CPI.TOTL.ZG":  # Inflation
                base_value = 100000000 * 0.5
                values = [base_value * (1 + 0.01 * (year - 2015)) for year in years]
            elif selected_indicator == "EN.POP.DNST":  # Population density
                base_value = 100 * 0.7
                values = [base_value * (1 + 0.005 * (year - 2015)) for year in years]
            else:  # Other indicators
                base_value = 50 * 0.8
                values = [base_value + np.random.normal(0, 5) for year in years]
            
            fig.add_trace(go.Scatter(
                x=years,
                y=values,
                mode='lines+markers',
                name=country_name,
                line=dict(color='#1f77b4', width=3),
                marker=dict(size=8),
                hovertemplate=f'<b>{country_name}</b><br>' +
                             f'Year: %{{x}}<br>' +
                             f'Value: %{{y:,.0f}}<br>' +
                             '<extra></extra>'
            ))
        
        # Get indicator display name for title from actual data
        if not static_data.empty and 'indicator_id' in static_data.columns and 'indicator_name' in static_data.columns:
            # Get the actual indicator name from the data
            indicator_info = static_data[static_data['indicator_id'] == selected_indicator]
            if not indicator_info.empty:
                indicator_display_name = indicator_info['indicator_name'].iloc[0]
            else:
                indicator_display_name = selected_indicator
        else:
            # Fallback to hardcoded names
            indicator_names = {
                'NY.GDP.MKTP.CD': 'GDP (current US$)',
                'NY.GDP.MKTP.KD.ZG': 'GDP Growth (annual %)',
                'NV.AGR.TOTL.ZS': 'Agriculture (% of GDP)',
                'NV.IND.TOTL.ZS': 'Industry (% of GDP)',
                'NV.SRV.TOTL.ZS': 'Services (% of GDP)',
                'FP.CPI.TOTL.ZG': 'Inflation, Consumer Prices (annual %)',
                'EN.POP.DNST': 'Population density',
                'IP.PCR.SRCN.XQ': 'Disaster risk reduction score',
                'VC.DSR.DRPT.P3': 'People affected by disasters (%)'
            }
            indicator_display_name = indicator_names.get(selected_indicator, selected_indicator)
        
        # Set title based on selection
        if country == "ALL" or not country:
            title = f" {indicator_display_name} - All Countries (2015-2025) - Click legend to filter"
        else:
            country_name = country_code_to_name.get(country, country)
            title = f" {indicator_display_name} - {country_name} (2015-2025)"
    
    # Update layout with interactive legend
    fig.update_layout(
        title=title,
        xaxis_title="Year",
        yaxis_title="Value",
        height=400,
        showlegend=True,
        hovermode='closest',
        template='plotly_white',
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=1.02,
            bgcolor='rgba(255, 255, 255, 0.8)',
            bordercolor='rgba(0, 0, 0, 0.2)',
            borderwidth=1
        )
    )
    
    return fig

# Callback to handle reset button, legend clicks, and map clicks
@app.callback(
    [Output('country-dropdown', 'value'),
     Output('indicator-dropdown', 'value'),
     Output('selected-country-store', 'data')],
    [Input('reset-button', 'n_clicks'),
     Input('indicator-line-chart', 'clickData'),
     Input('indicator-line-chart', 'selectedData'),
     Input('earthquake-map', 'clickData')],
    prevent_initial_call=True
)
def handle_filters_and_clicks(reset_clicks, click_data, selected_data, map_click_data):
    """Handle reset button, legend clicks, and map clicks"""
    ctx = callback_context
    
    if not ctx.triggered:
        return dash.no_update, dash.no_update, dash.no_update
    
    trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    # Handle reset button
    if trigger_id == 'reset-button' and reset_clicks:
        return None, None, ""  # Clear all selections
    
    # Handle legend clicks
    elif trigger_id == 'indicator-line-chart':
        if click_data and 'points' in click_data:
            for point in click_data['points']:
                if 'customdata' in point and point['customdata']:
                    # Return the country code from customdata, keep other outputs unchanged
                    return point['customdata'], dash.no_update, dash.no_update
                elif 'curveNumber' in point:
                    # Fallback: use curve number to get country
                    country_codes = ['US', 'CN', 'JP', 'MX', 'ID', 'TR', 'RU', 'IN', 'BR', 'CA', 
                                   'AU', 'DE', 'FR', 'IT', 'GB', 'KR', 'SA', 'AR', 'ZA', 'CL', 
                                   'NP', 'PK', 'PH', 'HN', 'NI', 'TO', 'TJ', 'AF', 'GR', 'IR']
                    if point['curveNumber'] < len(country_codes):
                        return country_codes[point['curveNumber']], dash.no_update, dash.no_update
    
    # Handle map clicks
    elif trigger_id == 'earthquake-map':
        if map_click_data and 'points' in map_click_data:
            for point in map_click_data['points']:
                if 'location' in point:
                    # Convert country name back to code
                    country_name_to_code = {
                        'United States': 'US', 'China': 'CN', 'Japan': 'JP', 'Mexico': 'MX', 'Indonesia': 'ID', 
                        'Turkey': 'TR', 'Russia': 'RU', 'India': 'IN', 'Brazil': 'BR', 'Canada': 'CA', 
                        'Australia': 'AU', 'Germany': 'DE', 'France': 'FR', 'Italy': 'IT', 'United Kingdom': 'GB', 
                        'South Korea': 'KR', 'Saudi Arabia': 'SA', 'Argentina': 'AR', 'South Africa': 'ZA', 
                        'Chile': 'CL', 'Nepal': 'NP', 'Pakistan': 'PK', 'Philippines': 'PH', 'Honduras': 'HN',
                        'Nicaragua': 'NI', 'Tonga': 'TO', 'Tajikistan': 'TJ', 'Afghanistan': 'AF', 'Greece': 'GR', 'Iran': 'IR'
                    }
                    country_code = country_name_to_code.get(point['location'], point['location'])
                    # Update both dropdown and store to enable map selection
                    return country_code, dash.no_update, country_code
    
    return dash.no_update, dash.no_update, dash.no_update

def main():
    """Main function to start the Dash server"""
    print("=" * 50)
    print("STARTING STREAMING DATA DASHBOARD")
    print("=" * 50)
    
    # Check MinIO connection
    try:
        s3_client = get_s3_client()
        s3_client.list_buckets()
        print("MinIO connection successful")
    except Exception as e:
        print(f"MinIO connection failed: {e}")
        return
    
    print("Starting Dash server...")
    print("Dashboard started successfully!")
    print("Access at: http://localhost:8050")
    
    # Run the app
    app.run_server(debug=True, host='0.0.0.0', port=8050)

if __name__ == "__main__":
    main() 