import requests
import pyodbc
from datetime import datetime, timedelta

# Facebook API endpoint
GRAPH_API_VERSION = 'v19.0'  # Specify the desired Graph API version
PAGE_ID = '109818058023623'
ACCESS_TOKEN = 'EAAWD1BHSts0BO2FlGFTQKmwmdGJ7AXSjKwubt3xQqQWXNj6jZBv3aHRYZBJU2kZB2ADubm9x5Sw1sdnZBvjtrcX7gConkVZCrZBInZBZBcXwdtOyc0f2NvsgPby4ykftT2VmQ33ZAiT0BueoUf46p62g0jR1CnfS2D1BFZA7Ma5ZAo6Q5rE9xVxjrXlTNsMO1G6YFZBulP2ZB2TDPbZBke9vmlDXkqIuZAyjLPcor6CLjBNjQ0ZD'
all_data = []
# until_date = datetime.now().date()
# since_date = until_date - timedelta(days=4)
since_date = '2020-01-01'
until_date = '2023-12-19'
(datetime.now().date())
endpoint = f"https://graph.facebook.com/v19.0/109818058023623/posts"

# Define the parameters for the request
params = {
    'access_token': ACCESS_TOKEN,
    'fields':  'created_time,id,full_picture,icon,instagram_eligibility,is_hidden,is_instagram_eligible,is_published,'
               'message,permalink_url,promotion_status,status_type,timeline_visibility,updated_time,promotable_id,'
               'is_expired,is_eligible_for_promotion,is_popular,is_spherical',
    'since': since_date,
    'until': until_date
}
response = requests.get(endpoint, params=params)

analytics_data = response.json()
print(analytics_data)
#----------------------------------------------------------------------------------------------
# Connect to SQL Server
connection_string = 'DRIVER={SQL Server};SERVER=Predicta.Database.Windows.Net;DATABASE=Predicta;UID=PredictaAdmin;PWD=Yhf^43*&^FHHytf'
conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

# Extract data from the JSON and insert into SQL table
for value in analytics_data['data']:
    created_time = value['created_time']
    id = value['id']
    full_picture = value['full_picture']
    icon = value['icon']
    instagram_eligibility = value['instagram_eligibility']
    is_hidden = value['is_hidden']
    is_instagram_eligible = value['is_instagram_eligible']
    is_published = value['is_published']
    message = value['message']
    permalink_url = value['permalink_url']
    promotion_status = value['promotion_status']
    status_type = value['status_type']
    timeline_visibility = value['timeline_visibility']
    updated_time = value['updated_time']
    promotable_id = value['promotable_id']
    is_expired = value['is_expired']
    is_eligible_for_promotion = value['is_eligible_for_promotion']
    is_popular = value['is_popular']
    is_spherical = value['is_spherical']
    insert_query = ("INSERT INTO dbo.FB_Posts_Insights_KS (created_time,id,full_picture,icon,instagram_eligibility,"
                    "is_hidden,is_instagram_eligible,is_published,message,permalink_url,promotion_status,status_type"
                    ",timeline_visibility,updated_time,promotable_id,is_expired,is_eligible_for_promotion,is_popular"
                    ",is_spherical) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
    cursor.execute(insert_query, (created_time,id,full_picture,icon,instagram_eligibility,is_hidden,
                        is_instagram_eligible,is_published,message,permalink_url,promotion_status,status_type,
                        timeline_visibility,updated_time,promotable_id,is_expired,is_eligible_for_promotion,is_popular,is_spherical))
conn.commit()
# Close the cursor and connection
cursor.close()
conn.close()
