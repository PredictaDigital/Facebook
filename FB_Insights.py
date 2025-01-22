import requests
import json
import pyodbc
from datetime import datetime, timedelta

# Facebook API endpoint
GRAPH_API_VERSION = 'v20.0'  # Specify the desired Graph API version
PAGE_ID = '109818058023623'
ACCESS_TOKEN = 'EAAFe1V0tVGYBOwdFsduM7xWqv6AHYkkG0efZBrsB4jg9OgjTZAe6euHLDpCi5oUmMeNL6b9XSfP3EcoZCnPR1qDUgrLadZCRrCUf2DPns8lBk2FVfHbh6e8a7zXkxgx0nTyQu03lqnVdAckouBBNrxZB0sFY4LbVVbIvXkHTrqtjsX6zQCwgpaf2X9v6UjV2434ZArba3plEMXGC1HPuzKR8iieTt73PaVbDxmFUAZD'
all_data = []
# until_date = datetime.now().date()
# since_date = until_date - timedelta(days=4)
#Limit is 93 days
since_date = '2024-01-22'
until_date = '2025-06-22'
# (datetime.now().date())
endpoint = f"https://graph.facebook.com/{GRAPH_API_VERSION}/{PAGE_ID}/insights"

# Define the parameters for the request
params = {
    'access_token': ACCESS_TOKEN,
    'metric': 'page_fans,page_fan_adds,page_fan_removes,page_views_total,page_video_views,page_video_view_time,'
              'page_video_views_paid,page_video_views_organic,page_video_views_click_to_play,'
              'page_posts_impressions,page_posts_impressions_paid,page_posts_impressions_organic,page_posts_impressions_viral,'
              'page_posts_impressions_nonviral,page_impressions,page_impressions_unique,page_impressions_paid,'
              'page_impressions_viral,page_impressions_nonviral,'
              'page_post_engagements,page_actions_post_reactions_like_total,page_actions_post_reactions_anger_total,'
              'page_actions_post_reactions_wow_total,page_actions_post_reactions_haha_total, page_actions_post_reactions_love_total,'
              'page_actions_post_reactions_sorry_total',
    'since': since_date,
    'until': until_date,
    'period': 'day'
}
response = requests.get(endpoint, params=params)

analytics_data = response.json()
# print (analytics_data)
list_type_data = analytics_data.get('data')
# print(list_type_data)
#----------------------------------------------------------------------------------------------
# Establish a connection to your SQL Server database
server = 'Predicta.Database.Windows.Net'
database = 'Predicta'
username = 'PredictaAdmin'
password = 'Yhf^43*&^FHHytf'

conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')
cursor = conn.cursor()

result_data = {}

for item in list_type_data:
    metric_name = item.get("name")
    for value in item.get("values"):
        if value.get('end_time') in result_data:
            result_data[value.get('end_time')].update({metric_name: value.get('value')})
        else:
            result_data[value.get('end_time')] = {metric_name: value.get('value')}

# print(result_data)
for end_time, metrics in result_data.items():
    # Constructing the INSERT query
    query = f'''
        INSERT INTO dbo.FB_Insights_KS (end_time, page_fans, page_fan_adds, page_fan_removes,
                                     page_views_total, page_video_views, page_video_view_time,page_video_views_paid,
                                     page_video_views_organic,page_video_views_click_to_play,
                                     page_posts_impressions,page_posts_impressions_paid,page_posts_impressions_organic,
                                     page_posts_impressions_viral,page_posts_impressions_nonviral,
                                     page_impressions, page_impressions_unique,page_impressions_paid, page_impressions_viral,
                                     page_impressions_nonviral, page_post_engagements,
                                     page_actions_post_reactions_like_total, page_actions_post_reactions_anger_total,
                                     page_actions_post_reactions_wow_total, page_actions_post_reactions_haha_total,
                                     page_actions_post_reactions_love_total, page_actions_post_reactions_sorry_total)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''
    # Executing the INSERT query
    cursor.execute(query, (end_time,metrics['page_fans'], metrics['page_fan_adds'],
                           metrics['page_fan_removes'], metrics['page_views_total'], metrics['page_video_views'],
                           metrics['page_video_view_time'],metrics['page_video_views_paid'],metrics['page_video_views_organic'],
                           metrics['page_video_views_click_to_play'],metrics['page_posts_impressions'],
                           metrics['page_posts_impressions_paid'],metrics['page_posts_impressions_organic'],metrics['page_posts_impressions_viral'],
                           metrics['page_posts_impressions_nonviral'],metrics['page_impressions'],
                           metrics['page_impressions_unique'],metrics['page_impressions_paid'], metrics['page_impressions_viral'],
                           metrics['page_impressions_nonviral'],metrics['page_post_engagements'], metrics['page_actions_post_reactions_like_total'],
                           metrics['page_actions_post_reactions_anger_total'], metrics['page_actions_post_reactions_wow_total'],
                           metrics['page_actions_post_reactions_haha_total'], metrics['page_actions_post_reactions_love_total'],
                           metrics['page_actions_post_reactions_sorry_total']))

conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()

