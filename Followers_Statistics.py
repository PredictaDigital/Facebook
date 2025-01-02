from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
import datetime
import pyodbc

# Set up API connection
app_id = '385745461007462'
app_secret = '39838a1fd0b95af866f368acc81f77b0'
access_token = 'EAAFe1V0tVGYBOy1ODGqRqX3G48V5wAeywZC20lwC0ZAFiWRbGWdvsBJDmyCey3fsKYb8lAwxe4YZALDMZCiANBZA3cLyvWNX45l60F3xYkf9oZCYCbmTZBo5ZAEy0c2SbUTjULc4XOpZABr9lMZBzZBdH7iyu1YvTirsqUAUMNJs1l3ZCC5ZCZBnYZBEzLSOrbZADIJCmjvM0WLRi3gH0u91ok8ZD'
today = datetime.datetime.today().strftime('%Y-%m-%d')

FacebookAdsApi.init(app_id, app_secret, access_token)

# Define the Ad Account ID as 'act_<ad_account_id>'
ad_account_id = 'act_145093901032221'

# Get the Ad Account
ad_account = AdAccount(ad_account_id)

# Define fields you want to retrieve
fields = [ 'name','objective','status','id','account_id','created_time','updated_time','buying_type',
           'start_time','stop_time','effective_status']

# Define metrics you want to retrieve
metrics = ['clicks','cpc','cpm','ctr','impressions','reach','spend','frequency',]

# Define the time range for insights
time_range = {
    'since': '2021-01-01',
    'until': today,

}
params = {
    'time_interval':1
}
# Get Campaigns
campaigns = ad_account.get_campaigns(fields=fields, params=params)

#-----------------------------------------------------------------------------------------------------
# Establish a connection to your SQL Server database
server = 'Predicta.Database.Windows.Net'
database = 'Predicta'
username = 'PredictaAdmin'
password = 'Yhf^43*&^FHHytf'

conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')
cursor = conn.cursor()

# Truncate the table
cursor.execute('TRUNCATE TABLE dbo.Facebook_Campaign_Insights_KS')

# Extract campaign data
for campaign in campaigns:
    Campaign_name = campaign['name']
    Objective = campaign['objective']
    Status = campaign['status']
    Campaign_ID  =  campaign['id']
    Account_ID = campaign['account_id']
    Created_Time = campaign['created_time']
    Updated_Time = campaign['updated_time']
    Start_Time = campaign['start_time']
    try:
        Stop_Time = campaign['stop_time']
    except KeyError:
        Stop_Time = today
    # Stop_Time = campaign['stop_time']
    Buying_type= campaign['buying_type']
    Effective_status= campaign['effective_status']
    # print(campaign)
    # Get insights for the campaign
    insights = campaign.get_insights(fields=metrics, params={'time_range': time_range})
    for insight in insights:
        Clicks = insight['clicks']
        try:CPC = insight['cpc']
        except KeyError:CPC = 0

        try:CPM = insight['cpm']
        except KeyError:CPM = 0

        try:CTR = insight['ctr']
        except KeyError:CTR = 0

        try:Frequency = insight['frequency']
        except KeyError:Frequency = 0

        try:Impressions = insight['impressions']
        except KeyError:Impressions = 0

        try:Reach = insight['reach']
        except KeyError:Reach = 0

        try: Spend = insight['spend']
        except KeyError: Spend = 0

    query = f'''
        INSERT INTO dbo.Facebook_Campaign_Insights_KS (Campaign_name,Objective,Status,Campaign_ID,Account_ID,
                    Created_Time,Updated_Time,Start_time,Stop_time,Buying_type,Effective_status,Clicks,CPC,CPM,CTR,Frequency,Impressions,Reach,Spend)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
    cursor.execute(query, Campaign_name,Objective,Status,Campaign_ID,Account_ID,Created_Time,Updated_Time,
                   Start_Time,Stop_Time,Buying_type,Effective_status,Clicks,CPC,CPM,CTR,Frequency,Impressions,Reach,Spend)
conn.commit()
cursor.close()
conn.close()