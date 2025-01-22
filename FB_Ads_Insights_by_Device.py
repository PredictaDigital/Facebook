from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
import datetime
import pyodbc
# Set up your app credentials
app_id = '385745461007462'
app_secret = '39838a1fd0b95af866f368acc81f77b0'
api_version='v20.0'
access_token = 'EAAFe1V0tVGYBOx1L62SL3JMTOoh29n4XbKh9Ac98vJmjV9eTW7ZAAzlcIuBZAnwidsbuhTZCFvgBH454NEF8HQK2GujrNzEb1xSJcnguHQPJ6ITodY2nuk8o76n2DNJI1Mfm5CjzS8DeCunOZA9zWiZAcUZB12BtQY05XqSrsBavnRambikqPGw2B5cakxVvWqh6Vl7hDD'
today = datetime.datetime.today().strftime('%Y-%m-%d')
ad_account_id = 'act_145093901032221'

# Initialize the FacebookAdsApi with your app credentials
FacebookAdsApi.init(app_id, app_secret, access_token)

# Get the AdAccount object
ad_account = AdAccount(ad_account_id)

# Define the fields and parameters for ad insights
fields = ['ad_id','ad_name','adset_id','adset_name','campaign_id','campaign_name','account_id','account_name',
    'reach','impressions','clicks','spend','frequency','account_currency','buying_type','unique_ctr','ctr','cpc',
    'cpm','cpp','objective','created_time','updated_time' ]
params = {
    'time_range': {'since': '2022-01-01', 'until': today},
    #limit is 37 months
    'level': 'ad',
    'breakdowns': ['impression_device','device_platform'],
    'time_increment': 1
}

# Retrieve ad insights
insights = ad_account.get_insights(fields=fields, params=params)
# print(insights)
#-----------------------------------------------------------------------------------------------------
# Establish a connection to your SQL Server database
server = 'Predicta.Database.Windows.Net'
database = 'Predicta'
username = 'PredictaAdmin'
password = 'Yhf^43*&^FHHytf'
db_table = 'dbo.FB_Ads_Insights_by_Device_KS'

conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')
cursor = conn.cursor()

# Truncate the table
# cursor.execute(f'TRUNCATE TABLE {db_table}')

for insight in insights:
    account_id = insight['account_id']
    account_name = insight['account_name']
    ad_id  =  insight['ad_id']
    ad_name = insight['ad_name']
    adset_id = insight['adset_id']
    adset_name = insight['adset_name']
    campaign_id = insight['campaign_id']
    campaign_name = insight['campaign_name']
    buying_type = insight['buying_type']
    created_time = insight['created_time']
    updated_time = insight['updated_time']
    account_currency = insight['account_currency']
    try: clicks = insight['clicks']
    except KeyError:clicks = 0
    try: cpc  =  insight['cpc']
    except KeyError: cpc = 0
    try: cpm = insight['cpm']
    except KeyError: cpm = 0
    try: cpp = insight['cpp']
    except KeyError: cpp = 0
    try: ctr = insight['ctr']
    except KeyError: ctr = 0
    try: objective = insight['objective']
    except KeyError:objective = 0
    try: impressions = insight['impressions']
    except KeyError:impressions = 0
    try: reach = insight['reach']
    except KeyError:reach = 0
    try: spend = insight['spend']
    except KeyError:spend = 0
    try: frequency = insight['frequency']
    except KeyError:frequency = 0
    try: unique_ctr = insight['unique_ctr']
    except KeyError:unique_ctr = 0
    try: impression_device = insight['impression_device']
    except KeyError:impression_device = 0
    try: device_platform = insight['device_platform']
    except KeyError:device_platform = 0
    query = f'''
        INSERT INTO {db_table} (account_id,account_name,ad_id,ad_name,adset_id,adset_name,campaign_id
      ,campaign_name,impression_device,device_platform,buying_type,created_time,updated_time,objective,account_currency,clicks,cpc,cpm,cpp,ctr,impressions
      ,reach,spend,frequency,unique_ctr, date_start, date_stop)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?)'''
    cursor.execute(query,account_id,account_name,ad_id,ad_name,adset_id,adset_name,campaign_id,campaign_name,
                   impression_device,device_platform,buying_type,created_time,updated_time,objective,account_currency,clicks,cpc,cpm,cpp,ctr,
                   impressions,reach,spend,frequency,unique_ctr,insight['date_start'], insight['date_stop'])
conn.commit()
cursor.close()
conn.close()
