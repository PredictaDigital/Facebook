import requests
import pyodbc
from datetime import datetime

# Define the base URL
GRAPH_API_VERSION = 'v19.0'
page_id ='109818058023623'
endpoint = f"https://graph.facebook.com/{GRAPH_API_VERSION}/{page_id}/insights"

# Define parameters
params = {
    'access_token': 'EAAFe1V0tVGYBO2ppZCRlw9mLT9ZCLvVUnSdueZBzpnq1IzVGF5O4f3i37DWtJ6f3gTTcNPgBZC5qpZCewwnxqSdlgzNkCpNmLvpj8meUF8UGxpcIUK5EMkJZB9FY4yliWrh6V7IPwYZAQZBBlidYXCHor8ZA1lBbUcSAEGqNZAjI7ZAz5VOPGDKwCkmWZCs81PXZAfIjfRAOk8dTqd02mJiMWecW8TssHGGI93iz3I4DNGbQZD',
    'since': int(datetime(2024, 12, 1).timestamp()),  # Convert since date to Unix timestamp
    'until': int(datetime(2025, 1, 19).timestamp()),  # Convert until date to Unix timestamp
    'metric': 'page_fans_city',
}

# Replace '{page_id}' in the base URL with the actual page ID
# Connect to SQL Server
server = 'Predicta.Database.Windows.Net'
database = 'Predicta'
username = 'PredictaAdmin'
password = 'Yhf^43*&^FHHytf'
db_table = 'dbo.FB_Followers_By_City_KS'

conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')
cursor = conn.cursor()

# Make the API request
response = requests.get(endpoint, params=params)
data = response.json() # Extract data from the response
print(data)
# Extract the insights data
insights_data = data.get('data', [])

# Loop through each entry in the insights data and insert it into the SQL table
for entry in insights_data:
    metric_name = entry.get('name', '')
    values = entry.get('values', [])

    # Loop through the values and insert them into the SQL table
    for value in values:
        value_data = value.get('value', {})

# Loop through the value data (city-value pairs) and insert them into the SQL table
    for city, value in value_data.items():
    # Define the SQL query to insert the data into the table
        insert_query = f"INSERT INTO {db_table} (PageID, MetricName, City, Value) VALUES (?, ?, ?, ?)"

        # Execute the SQL query
        cursor.execute(insert_query, (page_id, metric_name, city, value))
        conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()
