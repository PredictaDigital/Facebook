import requests
import pyodbc

# from datetime import datetime, timedelta

# Facebook API endpoint
GRAPH_API_VERSION = 'v19.0'  # Specify the desired Graph API version
PAGE_ID = '109818058023623'
ACCESS_TOKEN = 'EAAFe1V0tVGYBOzqjcDxeZCdhiRpaa9urBNuKhs7G2sougyk3Acuj0exQe1r52W0YGkOwjYtuZC10OfwF6GX3rEJ7JM9R5jeZBWfzEdpkz2E53JIGWKfhOhFkkDzCkSQxBoIS69lEZCv0JqH4hHblkXQsOsHiNXyXZB82HMCATAp5RZB1pJcJmAaZCBEUGX6fOYZCkU69PHW6wCzoEZAAZD'
all_data = []
# until_date = datetime.now().date()
# since_date = until_date - timedelta(days=4)
since_date = '2024-04-06'
until_date = '2024-05-26'
# (datetime.now().date())
endpoint = f"https://graph.facebook.com/v19.0/109818058023623/insights"

# Define the parameters for the request
params = {
    'access_token': ACCESS_TOKEN,
    'metric': 'page_follows',
    'since': since_date,
    'until': until_date,
    'period': 'day'
}
response = requests.get(endpoint, params=params)

analytics_data = response.json()
# print(analytics_data)
# Connect to SQL Server
connection_string = 'DRIVER={SQL Server};SERVER=Predicta.Database.Windows.Net;DATABASE=Predicta;UID=PredictaAdmin;PWD=Yhf^43*&^FHHytf'
conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

# Extract data from the JSON and insert into SQL table
for metric_data in analytics_data['data']:
    metric_name = metric_data['name']
    for value in metric_data['values']:
        end_time = value['end_time']
        page_follows = value['value']
        # Construct and execute SQL insert statement
        insert_query = f"INSERT INTO dbo.FB_Followers_Statistics_KS (End_Time,Page_Follows) VALUES (?, ?)"
        cursor.execute(insert_query, (end_time, page_follows))
conn.commit()
# Close the cursor and connection
cursor.close()
conn.close()