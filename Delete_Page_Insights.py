
import requests
import json
import pyodbc

# Facebook API endpoint
GRAPH_API_VERSION = 'v19.0'  # Specify the desired Graph API version
PAGE_ID = '109818058023623'
ACCESS_TOKEN = 'EAAFe1V0tVGYBO3VsQsg7BZBlljEmJbRDfZA5cCmmOHLxQZAJr6wsEzP73FZB3tayY1xiTbiDcIRp87ClJfy2f8ZArwlITxVbvXln4SDbJcikZBGeVRLhZC6XjRZCcQNLSD86xTI6CoADpGf29W8hcp9vZCKULHherqkNFrdfLO8liTcMaLubYrKBuH1MxwvvuqfQrrk8wKZCBM1TQJUSTj0Le91IUXT3oKG9B8UZCiDLQ8ZD'
endpoint = f"https://graph.facebook.com/v19.0/109818058023623/posts"

# Define the parameters for the request
params = {
    'access_token': ACCESS_TOKEN,
    'fields': 'created_time,id,full_picture,icon,message,permalink_url,promotable_id,timeline_visibility,status_type,'
              'promotion_status,is_hidden,is_published,is_instagram_eligible,updated_time,'
              'insights.metric(post_impressions,post_clicks,post_engaged_fan,post_reactions_like_total,'
              'post_reactions_love_total,post_reactions_wow_total,post_reactions_haha_total,post_reactions_sorry_total,'
              'post_reactions_anger_total,post_negative_feedback,post_impressions_fan,post_engaged_users,'
              'post_impressions_paid,post_impressions_organic,post_video_views,post_impressions_viral,post_impressions_nonviral,'
              'post_video_views_organic,post_video_views_paid,post_video_avg_time_watched)',

    'period': 'lifetime'
}
response = requests.get(endpoint, params=params)

analytics_data = response.json()
list_type_data = analytics_data.get('data')
print(list_type_data)
#------------------------------------------------------------------------------------------------------
# Establish a connection to your SQL Server database
server = 'Predicta.Database.Windows.Net'
database = 'Predicta'
username = 'PredictaAdmin'
password = 'Yhf^43*&^FHHytf'

conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')
cursor = conn.cursor()

result_data = {}
for item in list_type_data:
    insights = item.get('insights', {}).get('data', [])
    post_impressions = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'post_impressions'), 0)
    post_clicks = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'post_clicks'), 0)
    post_engaged_fan = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'post_engaged_fan'), 0)
    post_reactions_like_total = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'post_reactions_like_total'), 0)
    post_reactions_love_total = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'post_reactions_love_total'), 0)
    post_reactions_wow_total = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'post_reactions_wow_total'), 0)
    post_reactions_haha_total = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'post_reactions_haha_total'), 0)
    post_reactions_sorry_total = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'post_reactions_sorry_total'), 0)
    post_reactions_anger_total = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'post_reactions_anger_total'), 0)
    post_negative_feedback = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'post_negative_feedback'), 0)
    post_impressions_fan = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'post_impressions_fan'), 0)
    post_engaged_users = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'post_engaged_users'), 0)
    post_impressions_paid = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'post_impressions_paid'), 0)
    post_impressions_organic = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'post_impressions_organic'), 0)
    post_video_views = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'post_video_views'), 0)
    post_impressions_viral = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'post_impressions_viral'), 0)
    post_impressions_nonviral = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'post_impressions_nonviral'), 0)
    post_video_views_organic = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'post_video_views_organic'), 0)
    post_video_views_paid = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'post_video_views_paid'), 0)
    post_video_avg_time_watched = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'post_video_avg_time_watched'), 0)
    query = f'''
        INSERT INTO dbo.FB_Post_Insights_KS (created_time,id,full_picture,icon,message,permalink_url,
                promotable_id,timeline_visibility,status_type,promotion_status,is_hidden,is_published,
                is_instagram_eligible,updated_time,post_impressions,post_clicks,post_engaged_fan,post_reactions_like_total,
                post_reactions_love_total,post_reactions_wow_total,post_reactions_haha_total,post_reactions_sorry_total,
                post_reactions_anger_total,post_negative_feedback,post_impressions_fan,post_engaged_users,
                post_impressions_paid,post_impressions_organic,post_video_views,post_impressions_viral,post_impressions_nonviral,
                post_video_views_organic,post_video_views_paid,post_video_avg_time_watched)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
    cursor.execute(query, (item.get("created_time"),item.get("id"),item.get("full_picture"),item.get("icon"),
                           item.get("message"),item.get("permalink_url"),item.get("promotable_id"),item.get("timeline_visibility"),
                           item.get("status_type"),item.get("promotion_status"),item.get("is_hidden"),item.get("is_published"),
                           item.get("is_instagram_eligible"),item.get("updated_time"),post_impressions,post_clicks,
                           post_engaged_fan,post_reactions_like_total,post_reactions_love_total,post_reactions_wow_total,
                           post_reactions_haha_total,post_reactions_sorry_total,post_reactions_anger_total,
                           post_negative_feedback,post_impressions_fan,post_engaged_users,post_impressions_paid,
                           post_impressions_organic,post_video_views,post_impressions_viral,post_impressions_nonviral,
                           post_video_views_organic,post_video_views_paid,post_video_avg_time_watched))
conn.commit()
conn.close()
