
import requests
import json
import pyodbc

# Facebook API endpoint
GRAPH_API_VERSION = 'v22.0'  # Specify the desired Graph API version
PAGE_ID = '109818058023623'
ACCESS_TOKEN = 'EAAFe1V0tVGYBOznKXAzG2FvUnqSeZBit8IHFvtySVp6lg8Sb5G3I80eBW1ZCqSb3gGMuyHzq7ntQFUaI0j8ZANy0AeZAXFzPrR3Ypves01A4awRRzUjt5zBRfM5vE1KRA0helcuPSkuchqs9RZC0tcguzFBi1pScFNoVG85GJZBWtbkiqU50lTp24AHfUC6jrXXHGPLV4y2XZADrFZAwELYdjQwlPTXO13mu6CcZBW8IZD'
endpoint = f"https://graph.facebook.com/{GRAPH_API_VERSION}/{PAGE_ID}/posts"

# Define the parameters for the request
params = {
    'access_token': ACCESS_TOKEN,
    'fields': 'created_time,id,full_picture,icon,message,permalink_url,promotable_id,timeline_visibility,status_type,'
              'promotion_status,is_hidden,is_published,is_instagram_eligible,updated_time,'
              'insights.metric(post_impressions,post_clicks,post_reactions_like_total,post_video_views,'
              'post_reactions_love_total,post_reactions_wow_total,post_reactions_haha_total,post_reactions_sorry_total,'
              'post_reactions_anger_total,post_impressions_fan,post_impressions_paid,post_impressions_organic'
              ',post_impressions_viral,post_impressions_nonviral,'
              'post_video_views_organic,post_video_views_paid,post_video_avg_time_watched)',
# post_engaged_fan,post_negative_feedback,post_engaged_users,
    'period': 'lifetime'
}

# Function to fetch data with pagination
def fetch_all_posts(endpoint, params):
    all_posts = []
    while True:
        response = requests.get(endpoint, params=params)
        data = response.json()

        # Add the current batch of posts to the list
        all_posts.extend(data.get('data', []))

        # Check if there's a next page
        next_page = data.get('paging', {}).get('next')
        if not next_page:
            break  # Exit loop if no more pages

        # Update the endpoint and parameters for the next page
        endpoint = next_page
        params = {}  # No need to pass the original parameters for the next page

    return all_posts


# Fetch all posts
all_posts = fetch_all_posts(endpoint, params)

#------------------------------------------------------------------------------------------------------
# Establish a connection to your SQL Server database
server = 'Predicta.Database.Windows.Net'
database = 'Predicta'
username = 'PredictaAdmin'
password = 'Yhf^43*&^FHHytf'
db_table = 'dbo.FB_Post_Insights_KS'
conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')
cursor = conn.cursor()

# Truncate the table
cursor.execute(f'TRUNCATE TABLE {db_table}')

result_data = {}
for item in all_posts:
    insights = item.get('insights', {}).get('data', [])
    post_impressions = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'post_impressions'), 0)
    post_clicks = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'post_clicks'), 0)
    post_reactions_like_total = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'post_reactions_like_total'), 0)
    post_reactions_love_total = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'post_reactions_love_total'), 0)
    post_reactions_wow_total = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'post_reactions_wow_total'), 0)
    post_reactions_haha_total = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'post_reactions_haha_total'), 0)
    post_reactions_sorry_total = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'post_reactions_sorry_total'), 0)
    post_reactions_anger_total = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'post_reactions_anger_total'), 0)
    post_impressions_fan = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'post_impressions_fan'), 0)
    post_impressions_paid = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'post_impressions_paid'), 0)
    post_impressions_organic = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'post_impressions_organic'), 0)
    post_video_views = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'post_video_views'), 0)
    post_impressions_viral = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'post_impressions_viral'), 0)
    post_impressions_nonviral = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'post_impressions_nonviral'), 0)
    post_video_views_organic = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'post_video_views_organic'), 0)
    post_video_views_paid = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'post_video_views_paid'), 0)
    post_video_avg_time_watched = next((insight['values'][0]['value'] for insight in insights if insight['name'] == 'post_video_avg_time_watched'), 0)
    query = f'''
        INSERT INTO {db_table} (created_time,id,full_picture,icon,message,permalink_url,
                promotable_id,timeline_visibility,status_type,promotion_status,is_hidden,is_published,
                is_instagram_eligible,updated_time,post_impressions,post_clicks,post_reactions_like_total,
                post_reactions_love_total,post_reactions_wow_total,post_reactions_haha_total,post_reactions_sorry_total,
                post_reactions_anger_total,post_impressions_fan, post_impressions_paid,post_impressions_organic,
                post_video_views,post_impressions_viral,post_impressions_nonviral,post_video_views_organic,
                post_video_views_paid,post_video_avg_time_watched)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
    cursor.execute(query, (item.get("created_time"),item.get("id"),item.get("full_picture"),item.get("icon"),
                           item.get("message"),item.get("permalink_url"),item.get("promotable_id"),item.get("timeline_visibility"),
                           item.get("status_type"),item.get("promotion_status"),item.get("is_hidden"),item.get("is_published"),
                           item.get("is_instagram_eligible"),item.get("updated_time"),post_impressions,post_clicks,
                           post_reactions_like_total,post_reactions_love_total,post_reactions_wow_total,
                           post_reactions_haha_total,post_reactions_sorry_total,post_reactions_anger_total,
                           post_impressions_fan,post_impressions_paid, post_impressions_organic,post_video_views,
                           post_impressions_viral,post_impressions_nonviral,post_video_views_organic,post_video_views_paid,
                           post_video_avg_time_watched))
conn.commit()
conn.close()
