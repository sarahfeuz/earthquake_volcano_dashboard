#!/usr/bin/env python3
"""
Data Pipeline Visualization Dashboard
Simple streaming chart with real-time updates
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
        # Read from streaming bucket - look for processed earthquake data
        response = s3_client.list_objects_v2(Bucket=BUCKETS['streaming'], Prefix='processed_earthquakes/')
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
        
        df = pd.DataFrame(data)
        print(f" Earthquake data loaded: {df.shape}")
        return df
        
    except Exception as e:
        print(f" Error reading streaming earthquake data: {e}")
        return pd.DataFrame()

def read_static_data():
    """Read static World Bank data from gold layer"""
    print(f" Reading static World Bank data at {datetime.now().strftime('%H:%M:%S')}")
    s3_client = get_s3_client()
    
    try:
        # Read from gold layer - try the new parquet data first
        response = s3_client.list_objects_v2(Bucket=BUCKETS['gold'], Prefix='aggregated_data/')
        objects = response.get('Contents', [])
        
        if not objects:
            print("No static data found in gold layer")
            return pd.DataFrame()
        
        # Look for the new parquet data first, then fall back to old CSV data
        parquet_file = None
        csv_files = []
        
        for obj in objects:
            if obj['Key'].endswith('.parquet'):
                parquet_file = obj['Key']
                print(f"Found parquet data file: {parquet_file}")
            elif obj['Key'].endswith('.csv'):
                csv_files.append(obj['Key'])
        
        # Use parquet data if available, otherwise use the most recent CSV file
        if parquet_file:
            obj_key = parquet_file
            print(f"Using parquet data file: {obj_key}")
            
            # Read parquet file
            response = s3_client.get_object(Bucket=BUCKETS['gold'], Key=obj_key)
            content = response['Body'].read()
            
            # Write to temporary file and read with pandas
            import tempfile
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
                tmp_file.write(content)
                tmp_path = tmp_file.name
            
            try:
                df = pd.read_parquet(tmp_path)
                os.unlink(tmp_path)  # Clean up temp file
            except Exception as e:
                print(f"Error reading parquet file: {e}")
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
                return pd.DataFrame()
                
        elif csv_files:
            # Sort by last modified and use the most recent
            csv_files.sort(key=lambda x: [obj['LastModified'] for obj in objects if obj['Key'] == x][0], reverse=True)
            obj_key = csv_files[0]
            print(f"Using CSV data file: {obj_key}")
            
            # Read CSV file
            response = s3_client.get_object(Bucket=BUCKETS['gold'], Key=obj_key)
            content = response['Body'].read().decode('utf-8')
            df = pd.read_csv(io.StringIO(content))
        else:
            print("No data files found in gold layer")
            return pd.DataFrame()
        
        print(f" Static data loaded: {df.shape}")
        return df
        
    except Exception as e:
        print(f" Error reading static data: {e}")
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
        print(f" Error reading volcano data: {e}")
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
    
    # Convert timestamp to datetime
    recent_df['timestamp'] = pd.to_datetime(recent_df['timestamp'])
    
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
    
    # Convert timestamp to datetime
    table_df['timestamp'] = pd.to_datetime(table_df['timestamp'])
    
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
                    options=[
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
                    ],
                    placeholder="Select a country...",
                    style={"width": "200px", "display": "inline-block", "verticalAlign": "middle"},
                    className="me-3"
                ),
                html.Span(" Indicator:", className="me-2 align-right", style={"marginLeft": "40px"}),
                dcc.Dropdown(
                    id="indicator-dropdown",
                    options=[
                        {"label": "GDP (current US$)", "value": "NY.GDP.MKTP.CD"},
                        {"label": "GDP Growth (annual %)", "value": "NY.GDP.MKTP.KD.ZG"},
                        {"label": "Agriculture (% of GDP)", "value": "NV.AGR.TOTL.ZS"},
                        {"label": "Industry (% of GDP)", "value": "NV.IND.TOTL.ZS"},
                        {"label": "Services (% of GDP)", "value": "NV.SRV.TOTL.ZS"},
                        {"label": "Population, total", "value": "SP.POP.TOTL"},
                        {"label": "Population density", "value": "EN.POP.DNST"},
                        {"label": "Disaster risk reduction score", "value": "IP.PCR.SRCN.XQ"},
                        {"label": "People affected by disasters (%)", "value": "VC.DSR.DRPT.P3"}
                    ],
                    placeholder="Select an indicator...",
                    style={"width": "200px", "display": "inline-block", "verticalAlign": "middle", "marginLeft": "10px"},
                    className="me-3",
                    optionHeight=50
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
    
    # Get country name from code
    country_code_to_name = {
        'US': 'United States', 'CN': 'China', 'JP': 'Japan', 'MX': 'Mexico', 'ID': 'Indonesia', 
        'TR': 'Turkey', 'RU': 'Russia', 'IN': 'India', 'BR': 'Brazil', 'CA': 'Canada', 
        'AU': 'Australia', 'DE': 'Germany', 'FR': 'France', 'IT': 'Italy', 'GB': 'United Kingdom', 
        'KR': 'South Korea', 'SA': 'Saudi Arabia', 'AR': 'Argentina', 'ZA': 'South Africa', 
        'CL': 'Chile', 'NP': 'Nepal', 'PK': 'Pakistan', 'PH': 'Philippines', 'HN': 'Honduras',
        'NI': 'Nicaragua', 'TO': 'Tonga', 'TJ': 'Tajikistan', 'AF': 'Afghanistan', 'GR': 'Greece', 'IR': 'Iran'
    }
    
    years = list(range(2015, 2026))
    fig = go.Figure()
    
    # Define colors for different countries
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf',
              '#a6cee3', '#fb9a99', '#fdbf6f', '#cab2d6', '#ffff99', '#b15928', '#fbb4ae', '#b3cde3', '#ccebc5', '#decbe4',
              '#fed9a6', '#ffffcc', '#e5d8bd', '#fddaec', '#f2f2f2', '#b3e2cd', '#fdcdac', '#cbd5e8', '#f4cae4', '#e6f5c9']
    
    if country == "ALL" or not country:
        # Show all countries for the selected indicator
        all_countries = list(country_code_to_name.keys())
        
        for i, country_code in enumerate(all_countries):
            country_name = country_code_to_name[country_code]
            
            # Generate sample data based on indicator type and country
            if selected_indicator == "NY.GDP.MKTP.CD":  # GDP
                base_value = 1000000000000 * (0.5 + i * 0.1)  # Vary by country
                values = [base_value * (1 + 0.02 * (year - 2015) + np.random.normal(0, 0.05)) for year in years]
            elif selected_indicator == "SP.POP.TOTL":  # Population
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
                name=country_name,  # Use natural country name in legend
                line=dict(color=colors[i % len(colors)], width=2),
                marker=dict(size=6),
                hovertemplate=f'<b>{country_name}</b><br>' +
                             f'Year: %{{x}}<br>' +
                             f'Value: %{{y:,.0f}}<br>' +
                             '<extra></extra>',
                customdata=[country_code] * len(years),  # Store country code for legend clicks
                legendgroup=country_name,  # Use country name for legend grouping
                legendgrouptitle_text=country_name
            ))
        
        # Get indicator display name for title
        indicator_names = {
            'NY.GDP.MKTP.CD': 'GDP (current US$)',
            'NY.GDP.MKTP.KD.ZG': 'GDP Growth (annual %)',
            'NV.AGR.TOTL.ZS': 'Agriculture (% of GDP)',
            'NV.IND.TOTL.ZS': 'Industry (% of GDP)',
            'NV.SRV.TOTL.ZS': 'Services (% of GDP)',
            'SP.POP.TOTL': 'Population, total',
            'EN.POP.DNST': 'Population density',
            'IP.PCR.SRCN.XQ': 'Disaster risk reduction score',
            'VC.DSR.DRPT.P3': 'People affected by disasters (%)'
        }
        indicator_display_name = indicator_names.get(selected_indicator, selected_indicator)
        title = f" {indicator_display_name} - All Countries (2015-2025) - Click legend to filter"
        
    else:
        # Show single country
        country_name = country_code_to_name.get(country, country)
        
        # No sample series. If static data is needed for the line chart,
        # integrate real World Bank time series from the gold layer.
        values = []
        
        # Get indicator display name
        indicator_names = {
            'NY.GDP.MKTP.CD': 'GDP (current US$)',
            'NY.GDP.MKTP.KD.ZG': 'GDP Growth (annual %)',
            'NV.AGR.TOTL.ZS': 'Agriculture (% of GDP)',
            'NV.IND.TOTL.ZS': 'Industry (% of GDP)',
            'NV.SRV.TOTL.ZS': 'Services (% of GDP)',
            'SP.POP.TOTL': 'Population, total',
            'EN.POP.DNST': 'Population density',
            'IP.PCR.SRCN.XQ': 'Disaster risk reduction score',
            'VC.DSR.DRPT.P3': 'People affected by disasters (%)'
        }
        indicator_display_name = indicator_names.get(selected_indicator, selected_indicator)
        
        # If no real values are available, return an empty figure with a note
        if not values:
            return go.Figure().add_annotation(
                text=f"No {indicator_display_name} data available for {country_name}",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16, color="gray")
            )
        
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
        print(" MinIO connection successful")
    except Exception as e:
        print(f" MinIO connection failed: {e}")
        return
    
    print("Starting Dash server...")
    print(" Dashboard started successfully!")
    print(" Access at: http://localhost:8050")
    
    # Run the app
    app.run_server(debug=True, host='0.0.0.0', port=8050)

if __name__ == "__main__":
    main() 