import streamlit as st
import mysql.connector
import pandas as pd
import PIL
from PIL import Image
from streamlit_option_menu import option_menu
import plotly.express as px
import matplotlib.pyplot as plt
import requests
import pydeck as pdk
import geopandas as gpd
import os
import json
import requests 
import folium
from streamlit_folium import folium_static
import numpy as np
from shapely.geometry import shape, Polygon
from folium.plugins import MousePosition
# Function to establish and return a MySQL connection
def get_db_connection():
    try:
        connection = mysql.connector.connect(
            user='root',
            password='Arjunkutty22@',
            host='localhost',
            database='phonepe_pulse'
        )
        return connection
    except mysql.connector.Error as err:
        st.error(f"Error: {err}")
        return None

# Establishing the connection
mydb = get_db_connection()

if mydb is not None:
    # Create a cursor object
    mycursor = mydb.cursor()

# Function to create tables
def create_tables(mycursor, mydb):
    try:
        mycursor.execute("""
            CREATE TABLE IF NOT EXISTS Aggregated_transaction (
                States VARCHAR(255), 
                Years INT, 
                Quarter INT, 
                Transaction_type VARCHAR(255), 
                Transaction_count BIGINT, 
                Transaction_amount BIGINT
            )
        """)
        mycursor.execute("""
            CREATE TABLE IF NOT EXISTS Aggregated_user (
                States VARCHAR(255), 
                Years INT, 
                Quarter INT, 
                Brands VARCHAR(255), 
                Transaction_count BIGINT, 
                Percentage BIGINT
            )
        """)
        mycursor.execute("""
            CREATE TABLE IF NOT EXISTS Map_transaction (
                States VARCHAR(255), 
                Years INT, 
                Quarter INT, 
                Districts VARCHAR(255), 
                Transaction_count BIGINT, 
                Transaction_amount BIGINT
            )
        """)
        mycursor.execute("""
            CREATE TABLE IF NOT EXISTS Map_user (
                States VARCHAR(255), 
                Years INT, 
                Quarter INT, 
                Districts VARCHAR(255), 
                RegisteredUsers BIGINT, 
                AppOpens BIGINT
            )
        """)
        mycursor.execute("""
            CREATE TABLE IF NOT EXISTS Top_transaction (
                States VARCHAR(255), 
                Years INT, 
                Quarter INT, 
                Pincodes BIGINT, 
                Transaction_count BIGINT, 
                Transaction_amount BIGINT
            )
        """)
        mycursor.execute("""
            CREATE TABLE IF NOT EXISTS Top_user (
                States VARCHAR(255), 
                Years INT, 
                Quarter INT, 
                Pincodes BIGINT, 
                RegisteredUsers BIGINT
            )
        """)
        mydb.commit()
        st.success("Tables created successfully")
    except mysql.connector.Error as err:
        st.error(f"Error: {err}")

# Function to insert data from JSON files
def insert_data_from_file(file_path, table_name, columns, data_extractor, mycursor, mydb):
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
        
        # Extract state, year, and quarter from the file path
        path_parts = file_path.split(os.sep)
        state = path_parts[-4]
        year = int(path_parts[-2])
        quarter = int(path_parts[-1].split('.')[0])
        
        records = data_extractor(data, state, year, quarter)
        for record in records:
            values = tuple(record[col] for col in columns)
            placeholders = ', '.join(['%s'] * len(values))
            insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            mycursor.execute(insert_query, values)
        
        mydb.commit()
        st.success(f"Data inserted into {table_name} from {file_path} successfully")
    except Exception as err:
        st.error(f"Error: {err}")

# Data extraction functions for each table
def extract_aggregated_transaction(data, state, year, quarter):
    rows = []
    if isinstance(data, dict) and 'data' in data and 'transactionData' in data['data']:
        transaction_data = data['data']['transactionData']
        for transaction_entry in transaction_data:
            transaction_type = transaction_entry['name']
            for payment_instrument in transaction_entry['paymentInstruments']:
                transaction_count = payment_instrument['count']
                transaction_amount = payment_instrument['amount']
                row = {
                    'States': state,
                    'Years': year,
                    'Quarter': quarter,
                    'Transaction_type': transaction_type,
                    'Transaction_count': transaction_count,
                    'Transaction_amount': transaction_amount
                }
                rows.append(row)
    return rows

def extract_aggregated_user(data, state, year, quarter):
    rows = []
    if isinstance(data, dict) and 'data' in data and 'usersByDevice' in data['data']:
        user_data = data['data']['usersByDevice']
        for user_entry in user_data:
            brand = user_entry['brand']
            transaction_count = user_entry['count']
            percentage = user_entry['percentage']
            row = {
                'States': state,
                'Years': year,
                'Quarter': quarter,
                'Brands': brand,
                'Transaction_count': transaction_count,
                'Percentage': percentage
            }
            rows.append(row)
    return rows

def extract_map_transaction(data, state, year, quarter):
    rows = []
    if isinstance(data, dict) and 'data' in data and 'hoverDataList' in data['data']:
        hover_data = data['data']['hoverDataList']
        for entry in hover_data:
            district = entry['name']
            metrics = entry['metric']
            for metric in metrics:
                if metric['type'] == 'TOTAL':
                    transaction_count = metric['count']
                    transaction_amount = metric['amount']
                    row = {
                        'States': state,
                        'Years': year,
                        'Quarter': quarter,
                        'Districts': district,
                        'Transaction_count': transaction_count,
                        'Transaction_amount': transaction_amount
                    }
                    rows.append(row)
    return rows

def extract_map_user(data, state, year, quarter):
    rows = []
    if isinstance(data, dict) and 'data' in data and 'hoverData' in data['data']:
        user_data = data['data']['hoverData']
        for district, metrics in user_data.items():
            registered_users = metrics.get('registeredUsers', 0)
            app_opens = metrics.get('appOpens', 0)
            row = {
                'States': state,
                'Years': year,
                'Quarter': quarter,
                'Districts': district,
                'RegisteredUsers': registered_users,
                'AppOpens': app_opens
            }
            rows.append(row)
    return rows

def extract_top_transaction(data, state, year, quarter):
    rows = []
    if isinstance(data, dict) and 'data' in data and 'pincodes' in data['data']:
        transaction_data = data['data']['pincodes']
        for transaction_entry in transaction_data:
            pincode = transaction_entry.get('pincode', 0)
            transaction_count = transaction_entry.get('count', 0)
            transaction_amount = transaction_entry.get('amount', 0)
            row = {
                'States': state,
                'Years': year,
                'Quarter': quarter,
                'Pincodes': pincode,
                'Transaction_count': transaction_count,
                'Transaction_amount': transaction_amount
            }
            rows.append(row)
    return rows

def extract_top_user(data, state, year, quarter):
    rows = []
    if isinstance(data, dict) and 'data' in data and 'pincodes' in data['data']:
        user_data = data['data']['pincodes']
        for user_entry in user_data:
            pincode = user_entry.get('pincode', 0)
            registered_users = user_entry.get('registeredUsers', 0)
            row = {
                'States': state,
                'Years': year,
                'Quarter': quarter,
                'Pincodes': pincode,
                'RegisteredUsers': registered_users
            }
            rows.append(row)
    return rows

# Function to process directories and insert data
def process_directory(base_path, table_name, columns, data_extractor, mycursor, mydb):
    for root, dirs, files in os.walk(base_path):
        for file in files:
            if file.endswith(".json"):
                file_path = os.path.join(root, file)
                insert_data_from_file(file_path, table_name, columns, data_extractor, mycursor, mydb)

# Calling function to create tables
create_tables(mycursor, mydb)

# Insert data for each table
process_directory("C:/path/to/aggregated/transaction", "Aggregated_transaction", ["States", "Years", "Quarter", "Transaction_type", "Transaction_count", "Transaction_amount"], extract_aggregated_transaction, mycursor, mydb)
process_directory("C:/path/to/aggregated/user", "Aggregated_user", ["States", "Years", "Quarter", "Brands", "Transaction_count", "Percentage"], extract_aggregated_user, mycursor, mydb)
process_directory("C:/path/to/map/transaction", "Map_transaction", ["States", "Years", "Quarter", "Districts", "Transaction_count", "Transaction_amount"], extract_map_transaction, mycursor, mydb)
process_directory("C:/path/to/map/user", "Map_user", ["States", "Years", "Quarter", "Districts", "RegisteredUsers", "AppOpens"], extract_map_user, mycursor, mydb)
process_directory("C:/path/to/top/transaction", "Top_transaction", ["States", "Years", "Quarter", "Pincodes", "Transaction_count", "Transaction_amount"], extract_top_transaction, mycursor, mydb)
process_directory("C:/path/to/top/user", "Top_user", ["States", "Years", "Quarter", "Pincodes", "RegisteredUsers"], extract_top_user, mycursor, mydb)

# Title
st.title("📱 PHONEPE PULSE DATA VISUALIZATION ")

# Streamlit sidebar menu
SELECT = st.sidebar.selectbox(
    "📱 PHONEPE PULSE MENU",
    ["🏠 Home", "📊 Top Charts - Transaction", "📈 Top Charts - Users", "📅 View Tables", "📊 Data Visualization", "❓ Queries"]
)

# Home page

if SELECT == "🏠 Home":
    st.subheader("Welcome to PhonePe Pulse Data Visualization")
    col1,col2 = st.columns([1,3])
    col1.image(Image.open("C:/Users/Vishaali Naagaarjun/Downloads/pelogo.jpeg"),width = 200)
    with col2:
        st.subheader("PhonePe is an Indian digital payments and financial technology company headquartered in Bengaluru, Karnataka, India. PhonePe was founded in December 2015, by Sameer Nigam, Rahul Chari and Burzin Engineer. The PhonePe app, based on the Unified Payments Interface (UPI), went live in August 2016. It is owned by Flipkart, a subsidiary of Walmart.")
        st.download_button("DOWNLOAD THE APP NOW", "https://www.phonepe.com/app-download/")


# Top Charts - Transaction
if SELECT == "📊 Top Charts - Transaction":
    st.subheader("Top Charts - Transaction")
    st.write("Visualizing top transaction data...")
    
    # Pie chart for aggregated transaction data
    st.subheader("Aggregated Transaction Data - Pie Chart")
    query = "SELECT Transaction_type, SUM(Transaction_count) as Total_Transactions FROM Aggregated_transaction GROUP BY Transaction_type"
    df = pd.read_sql(query, mydb)
    fig = px.pie(df, values='Total_Transactions', names='Transaction_type', title='Aggregated Transaction Data')
    st.plotly_chart(fig)
    
    # Scatter plot for map transaction data
    st.subheader("Map Transaction Data - Scatter Plot")
    query = "SELECT Districts, Transaction_count, Transaction_amount FROM Map_transaction"
    df = pd.read_sql(query, mydb)
    fig = px.scatter(df, x='Transaction_count', y='Transaction_amount', color='Districts', title='Map Transaction Data')
    st.plotly_chart(fig)

    # Bar graph for top transaction data
    st.subheader("Top Transaction Data - Bar Graph")
    query = "SELECT Pincodes, SUM(Transaction_count) as Total_Transactions FROM Top_transaction GROUP BY Pincodes"
    df = pd.read_sql(query, mydb)
    fig = px.bar(df, x='Pincodes', y='Total_Transactions', title='Top Transaction Data')
    st.plotly_chart(fig)

# Top Charts - Users
if SELECT == "📈 Top Charts - Users":
    st.subheader("Top Charts - Users")
    st.write("Visualizing top users data...")
    
    # Pie chart for aggregated user data
    st.subheader("Aggregated User Data - Pie Chart")
    query = "SELECT Brands, SUM(Transaction_count) as Total_Users FROM Aggregated_user GROUP BY Brands"
    df = pd.read_sql(query, mydb)
    fig = px.pie(df, values='Total_Users', names='Brands', title='Aggregated User Data')
    st.plotly_chart(fig)
    
    # Scatter plot for map user data
    st.subheader("Map User Data - Scatter Plot")
    query = "SELECT Districts, RegisteredUsers, AppOpens FROM Map_user"
    df = pd.read_sql(query, mydb)
    fig = px.scatter(df, x='RegisteredUsers', y='AppOpens', color='Districts', title='Map User Data')
    st.plotly_chart(fig)

    # Bar graph for top user data
    st.subheader("Top User Data - Bar Graph")
    query = "SELECT Pincodes, SUM(RegisteredUsers) as Total_Users FROM Top_user GROUP BY Pincodes"
    df = pd.read_sql(query, mydb)
    fig = px.bar(df, x='Pincodes', y='Total_Users', title='Top User Data')
    st.plotly_chart(fig)


# Function to view all tables
def view_tables(mycursor):
    try:
        tables = ['Aggregated_transaction', 'Aggregated_user', 'Map_transaction', 'Map_user', 'Top_transaction', 'Top_user']
        for table in tables:
            query = f"SELECT * FROM {table}"
            mycursor.execute(query)
            result = mycursor.fetchall()
            st.write(f"## {table} Table")
            if result:
                df = pd.DataFrame(result, columns=[desc[0] for desc in mycursor.description])
                st.write(df)
            else:
                st.write("Table is empty.")
    except mysql.connector.Error as err:
        st.error(f"Error: {err}")

# Main function
if SELECT == '📅 View Tables':
    view_tables(mycursor)


# Connect to the database
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Arjunkutty22@",
    database="phonepe_pulse"
)
mycursor = mydb.cursor()


# Data Visualization Page
if SELECT == "📊 Data Visualization":
    st.title("Data Visualization")

    # Map Visualization
    st.subheader("Map Visualization")

    # Fetch data from MySQL database
    connection = get_db_connection()
    if connection:
        query = """
        SELECT States, 
               SUM(Transaction_count) AS TotalTransactionCount, 
               SUM(Transaction_amount) AS TotalTransactionAmount
        FROM Map_transaction
        GROUP BY States
        """
        df_map_user = pd.read_sql(query, connection)
        connection.close()
        
        st.subheader("Total Transaction Count and Total Transaction Amount State-wise")
        st.dataframe(df_map_user)
        
       

        # Display total transaction count and total transaction amount
        total_transaction_count = df_map_user['TotalTransactionCount'].sum()
        total_transaction_amount = df_map_user['TotalTransactionAmount'].sum()

        st.write(f"Total Transaction Count: {total_transaction_count}")
        st.write(f"Total Transaction Amount: {total_transaction_amount}")

        # Fetch the GeoJSON data
        geojson_url = "https://raw.githubusercontent.com/geohacker/india/master/state/india_telengana.geojson"
        response = requests.get(geojson_url)
        state_geojson = response.json()

        # Create Folium Map with a purple theme
        m = folium.Map(location=[20.5937, 78.9629], zoom_start=5, tiles='cartodbpositron')

        # Add GeoJSON to the map
        folium.GeoJson(state_geojson, name="geojson").add_to(m)

        # Add data points using centroids with purple color
        for feature in state_geojson['features']:
            state_name = feature['properties'].get('st_nm') or feature['properties'].get('ST_NM')
            if state_name in df_map_user['States'].values:
                state_data = df_map_user[df_map_user['States'] == state_name].iloc[0]
                total_transaction_count = state_data['TotalTransactionCount']
                total_transaction_amount = state_data['TotalTransactionAmount']

                centroid = shape(feature['geometry']).centroid
                lat, lon = centroid.y, centroid.x

                popup_text = f"State: {state_name}<br>" \
                             f"Total Transaction Count: {total_transaction_count}<br>" \
                             f"Total Transaction Amount: {total_transaction_amount}"
                
                folium.CircleMarker(
                    location=[lat, lon],
                    radius=5,
                    popup=popup_text,
                    color="#800080",  # Purple color
                    fill=True,
                    fill_color="#800080"  # Purple color
                ).add_to(m)

        # Display the map
        folium_static(m)
    
# Queries
if SELECT == "❓ Queries":
    st.subheader("Custom SQL Queries")

    query_options = {
        "Show total transactions for each transaction type": "SELECT Transaction_type, SUM(Transaction_count) as Total_Transactions FROM Aggregated_transaction GROUP BY Transaction_type",
        "Show total transaction amount by district": "SELECT Districts, SUM(Transaction_amount) as Total_Transaction_Amount FROM Map_transaction GROUP BY Districts",
        "Show total registered users by brand": "SELECT Brands, SUM(Transaction_count) as Total_Users FROM Aggregated_user GROUP BY Brands",
        "Show total registered users and app opens by district": "SELECT Districts, SUM(RegisteredUsers) as Total_Users, SUM(AppOpens) as Total_AppOpens FROM Map_user GROUP BY Districts",
        "Show total transactions by pincode": "SELECT Pincodes, SUM(Transaction_count) as Total_Transactions FROM Top_transaction GROUP BY Pincodes",
        "Show total registered users by pincode": "SELECT Pincodes, SUM(RegisteredUsers) as Total_Users FROM Top_user GROUP BY Pincodes",
        "Show average transaction amount by transaction type": "SELECT Transaction_type, AVG(Transaction_amount) as Avg_Transaction_Amount FROM Aggregated_transaction GROUP BY Transaction_type",
        "Show total app opens by state": "SELECT State, SUM(AppOpens) as Total_AppOpens FROM Map_user GROUP BY State",
        "Show total transaction count and amount by district": "SELECT Districts, SUM(Transaction_count) as Total_Transaction_Count, SUM(Transaction_amount) as Total_Transaction_Amount FROM Map_transaction GROUP BY Districts",
        "Show total registered users and app opens by state": "SELECT State, SUM(RegisteredUsers) as Total_Users, SUM(AppOpens) as Total_AppOpens FROM Map_user GROUP BY State"
    }

    selected_query = st.selectbox("Select a query", list(query_options.keys()))

    if st.button("Execute Query"):
        if selected_query:
            query = query_options[selected_query]
            try:
                df = pd.read_sql(query, mydb)
                st.write(df)
                st.write("Query executed successfully.")
                
                # Determine the type of chart to display based on the columns in the result
                if 'Transaction_type' in df.columns or 'Brands' in df.columns:
                    fig = px.pie(df, values=df.columns[1], names=df.columns[0], title=selected_query)
                elif 'Districts' in df.columns or 'Pincodes' in df.columns or 'States' in df.columns:
                    fig = px.bar(df, x=df.columns[0], y=df.columns[1], title=selected_query)
                else:
                    fig = px.scatter(df, x=df.columns[1], y=df.columns[2], color=df.columns[0], title=selected_query)
                
                st.plotly_chart(fig)
                
            except Exception as e:
                st.write(f"Error executing query: {e}")

# Close the cursor and connection
mycursor.close()
mydb.close()

