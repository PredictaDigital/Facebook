import requests
import pyodbc
from datetime import datetime

# Define the base URL
endpoint = "https://graph.facebook.com/v19.0/109818058023623/insights"

# Define parameters
params = {
    'access_token': 'EAAWD1BHSts0BO7luaqWyMZBpfXLs2HRTmZAmUZCrVlDwt7fCDI02SvFTTh8XuV97sD1P52ZCv4iom2bO8TdS9Xa2bptd7QXhh2QzInsDA553MlCMeNC6vIRY3UHRE7sLvJejiKiWYXhGWbktVlw7ZBjzThUjlcWCezITZAbRtKq1JVIRUld7U2Mu57hpnAi4sSFfRvgXgXwHNeT5RLCvToRMsZD',
    'since': int(datetime(2024, 3, 14).timestamp()),  # Convert since date to Unix timestamp
    'until': int(datetime(2024, 3, 19).timestamp()),  # Convert until date to Unix timestamp
    'metric': 'page_fans_gender_age',
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

# Initialize lists to store values for each column
genders = []
ages = []
values = []

# Iterate over the dictionary items
for key, value in value_data.items():
    # Split the key into gender and age parts
    gender, age = key.split('.')
    # Append values to respective lists
    genders.append(gender)
    ages.append(age)
    values.append(value)

# Create a dictionary for the new DataFrame
new_data = {'Gender': genders, 'Age': ages, 'Value': values}
# print(new_data)

# Loop through the value data (city-value pairs) and insert them into the SQL table
for gender, age, value in zip(genders, ages, values):
# Define the SQL query to insert the data into the table
    insert_query = ("INSERT INTO dbo.FB_Followers_By_Gender_Age_KS (PageID, MetricName, Gender, Age, Value)"
                            "VALUES (?, ?, ?, ?, ?)")
    # Execute the SQL query
    cursor.execute(insert_query, ("109818058023623", metric_name, gender, age, value))
    conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()

