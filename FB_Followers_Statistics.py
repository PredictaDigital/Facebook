import requests
import pyodbc

# from datetime import datetime, timedelta

# Facebook API endpoint
GRAPH_API_VERSION = 'v21.0'  # Specify the desired Graph API version
PAGE_ID = '109818058023623'
ACCESS_TOKEN = 'EAAFe1V0tVGYBO5LBLFeqZAZAvoxhDTo8G6QzICXf9f6sIst4ZA2oBZAV9HxEpLtpJaRJAEcup2Fm5pDWZAP0OVzh1FOvwcboSv0eH72F1orVeTqcYPoYZAKXiRgsBWFpfSfx7Lr7hGGfgJHdQdKA6NgMHiP5RH982zN2UZCm0eWoVgma79tGDn18eZBeHZAG9jkjxIHvgFsz26fZCE0jMZD'
all_data = []
# until_date = datetime.now().date()
# since_date = until_date - timedelta(days=4)
since_date = '2024-11-22'
until_date = '2025-01-22'
# (datetime.now().date())
endpoint = f"https://graph.facebook.com/{GRAPH_API_VERSION}/{PAGE_ID}/insights"

# Define the parameters for the request
params = {
    'access_token': ACCESS_TOKEN,
    'metric': 'page_follows',
    'since': since_date,
    'until': until_date,
    'period': 'day'
}
response = requests.get(endpoint, params=params)

# print(analytics_data)
# Connect to SQL Server

server = 'Predicta.Database.Windows.Net'
database = 'Predicta'
username = 'PredictaAdmin'
password = 'Yhf^43*&^FHHytf'
db_table = 'dbo.FB_Followers_Statistics_KS'

conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')
cursor = conn.cursor()

analytics_data = response.json()
# print(analytics_data)
# Extract data from the JSON and insert into SQL table
for metric_data in analytics_data['data']:
    metric_name = metric_data['name']
    for value in metric_data['values']:
        end_time = value['end_time']
        page_follows = value['value']
        # Construct and execute SQL insert statement
        insert_query = f"INSERT INTO {db_table} (End_Time,Page_Follows) VALUES (?, ?)"
        cursor.execute(insert_query, (end_time, page_follows))
conn.commit()
# Close the cursor and connection
cursor.close()
conn.close()
