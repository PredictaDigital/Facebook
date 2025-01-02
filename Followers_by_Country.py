import requests
import pyodbc
from datetime import datetime


# Define the base URL
endpoint = "https://graph.facebook.com/v19.0/109818058023623/insights"

# Define parameters
params = {
    'access_token': 'EAAWD1BHSts0BOxGS36AifZBsdX4BOMs7tkGsEkyU5sDgBV6scZC02gu0UK2jZAgUJ2CPPwRFgi1aZAz9FMablQdJNSDQ4zmIIwcpOAKcXlV8Be7KZBUqZApjmkgXsH1dCj20AX9rBLDw76jWXziI2MaBDngoWV4E4VmNZB9DQTBG322WGhIyjZCvqZAcd2pLs8RslKIpmBjD5NjUaVWWji76xKuUZD',
    'since': int(datetime(2024, 3, 14).timestamp()),  # Convert since date to Unix timestamp
    'until': int(datetime(2024, 3, 19).timestamp()),  # Convert until date to Unix timestamp
    'metric': 'page_fans_city',
    'page_id':'109818058023623'
}

# Replace '{page_id}' in the base URL with the actual page ID
# url = base_url.format(page_id='page_id')
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=Predicta.Database.Windows.Net;DATABASE=Predicta;UID=PredictaAdmin;PWD=Yhf^43*&^FHHytf')
cursor = conn.cursor()

# Make the API request
response = requests.get(endpoint, params=params)
data = response.json() # Extract data from the response

# Extract the insights data
insights_data = data.get('data', [])
# Loop through each entry in the insights data and insert it into the SQL table
for entry in insights_data:
     metric_name = entry.get('name', '')
     values = entry.get('values', [])

    # Loop through the values and insert them into the SQL table
for value in values:
    value_data = value.get('value', {})
# print(value_data)

# Loop through the value data (city-value pairs) and insert them into the SQL table
for city, value in value_data.items():
# Define the SQL query to insert the data into the table
    insert_query = ("INSERT INTO dbo.FB_Followers_By_City_KS (PageID, MetricName, City, Value)"
                            "VALUES (?, ?, ?, ?)")
    # Execute the SQL query
    cursor.execute(insert_query, ("109818058023623", metric_name, city, value))
    conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()

