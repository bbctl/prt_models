import config # Username & Password Sharepoint Authentication Vars Exist Here
import requests # Pulls Live Sharepoint Data
import pandas as pd # Converts Sharepoint & CMS Invoice Detail Files to Pandas DataFrames 
from requests_ntlm import HttpNtlmAuth # Authorize Pulling Live Sharepoint Data
from shareplum import Site # Module to bring in Sharepoint

def mySharepoint():

    cred = HttpNtlmAuth(config.USERNAME,config.PASSWORD)
    site = Site('https://applications.level3.com/sites/ProactiveRehabTracking', auth=cred)

    sp_list = site.List('Proactive Rehab Tracking List') # Get the List by Name
    data = sp_list.GetListItems('All Items') # Called 'AllItems' In Sharepoint

    df = pd.DataFrame(data[0:]) # Brings in 'live' Sharepoint to DataFrame 
    return df

all_items = mySharepoint()

if __name__ == '__main__':
    mySharepoint()