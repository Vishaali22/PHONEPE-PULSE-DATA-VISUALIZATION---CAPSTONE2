# PHONEPE-PULSE-DATA-VISUALIZATION---CAPSTONE2
PHONEPE PULSE DATA VISUALIZATION - CAPSTONE2

Overview
This project aims to visualize data extracted from PhonePe Pulse using various visualization techniques. PhonePe Pulse is a platform that provides insights into digital transactions and user behaviors on the PhonePe app.

Technologies Used
Python: The primary programming language used for scripting and data processing.
Streamlit: Used for building the web-based user interface for data visualization.
MySQL: Database management system used to store and retrieve PhonePe Pulse data.
Pandas: Library for data manipulation and analysis, used to work with data retrieved from the database.
Plotly Express: Library for creating interactive visualizations, used for charts and graphs.
GeoPandas: Library for working with geospatial data, used for handling geographical information such as maps.
Folium: Library for creating interactive maps, used for visualizing geographical data.

Features
Home Page: Provides an overview of the project and its objectives.
Data Visualization Page: Displays various visualizations of PhonePe Pulse data, including charts, graphs, and maps.
Map Visualization: Shows geographical distribution of transaction data using an interactive map.
State-wise Total Transaction Information: Displays a table with state-wise total transaction count and total transaction amount.

How to Run
Clone this repository to your local machine.
Ensure you have Python installed. You can download it from here.
Install the required Python libraries using pip:

pip install streamlit mysql-connector-python pandas plotly geopandas folium

Set up a MySQL database and import the PhonePe Pulse data.

Update the database connection details in the Python script (pecap2.py) to connect to your MySQL database.

Run the Streamlit application:
arduino
Copy code
streamlit run pecap2.py
References
Streamlit Documentation
MySQL Documentation
Pandas Documentation
Plotly Express Documentation
GeoPandas Documentation
Folium Documentation
Contributors
Vishaali RJ
