import pandas as pd


# Network Drive URIs
west = pd.read_csv(r'//pkdw0705/Network_Investment/Network_ReportCaster/Status_Reports/Status_Report_West_PORTIZ.csv', engine='python')
south = pd.read_excel(r'//pkdw0705/Network_Investment/Network_ReportCaster/Status_Reports/Status_Report_South_PORTIZ.xlsx', sheet_name='Sheet1', engine='openpyxl', skiprows=3)
north = pd.read_excel(r'//pkdw0705/Network_Investment/Network_ReportCaster/Status_Reports/Status_Report_North_PORTIZ.xlsx', sheet_name='Sheet1', engine='openpyxl', skiprows=3)

defaultdate = 0 # Fill NaN w/ 0 to Convert To_DataTime()
south = south.fillna(value=defaultdate)
north = north.fillna(value=defaultdate)
west = west.fillna(value=defaultdate)

north = north[[
    'Project',
    'Actual Constr Complete Dte',
    'FW Created Dte',
    'System Teco Dte',
    'System Close Dte',
    'Eng Start Dte',
    'Eng Complete Dte'
    ]]

south = south[[
    'Project',
    'Actual Constr Complete Dte',
    'FW Created Dte',
    'System Teco Dte',
    'System Close Dte',
    'Eng Start Dte',
    'Eng Complete Dte'
    ]]

west = west[[
    'Project',
    'Actual Constr Complete Dte',
    'FW Created Dte',
    'System Teco Dte',
    'System Close Dte',
    'Eng Start Dte',
    'Eng Complete Dte'
    ]]

# Convert Date Columns From Integers To Desired Datetime Format (mm/dd/yyyy)
south['Actual Constr Complete Dte'] = pd.to_datetime(south['Actual Constr Complete Dte'], errors='coerce')
south['FW Created Dte'] = pd.to_datetime(south['FW Created Dte'], errors='coerce')
south['System Teco Dte'] = pd.to_datetime(south['System Teco Dte'], errors='coerce')
south['System Close Dte'] = pd.to_datetime(south['System Close Dte'], errors='coerce')
south['Eng Start Dte'] = pd.to_datetime(south['Eng Start Dte'], errors='coerce')
south['Eng Complete Dte'] = pd.to_datetime(south['Eng Complete Dte'], errors='coerce')
south['Region'] = 'SOUTH'

# Convert Date Columns From Integers To Desired Datetime Format (mm/dd/yyyy)
west['Actual Constr Complete Dte'] = pd.to_datetime(west['Actual Constr Complete Dte'], format='%m%d%Y', errors='coerce')
west['FW Created Dte'] = pd.to_datetime(west['FW Created Dte'], format='%m%d%Y', errors='coerce')
west['System Teco Dte'] = pd.to_datetime(west['System Teco Dte'], format='%m%d%Y', errors='coerce')
west['System Close Dte'] = pd.to_datetime(west['System Close Dte'], format='%m%d%Y', errors='coerce')
west['Eng Start Dte'] = pd.to_datetime(west['Eng Start Dte'], format='%m%d%Y', errors='coerce')
west['Eng Complete Dte'] = pd.to_datetime(west['Eng Complete Dte'], format='%m%d%Y', errors='coerce')
west['Region'] = 'WEST'

# Convert Date Columns From Integers To Desired Datetime Format (mm/dd/yyyy)
north['Actual Constr Complete Dte'] = pd.to_datetime(north['Actual Constr Complete Dte'], errors='coerce')
north['FW Created Dte'] = pd.to_datetime(north['FW Created Dte'], errors='coerce')
north['System Teco Dte'] = pd.to_datetime(north['System Teco Dte'], errors='coerce')
north['System Close Dte'] = pd.to_datetime(north['System Close Dte'], errors='coerce')
north['Eng Start Dte'] = pd.to_datetime(north['Eng Start Dte'], errors='coerce')
north['Eng Complete Dte'] = pd.to_datetime(north['Eng Complete Dte'], errors='coerce')
north['Region'] = 'NORTH'

capital_buckets = pd.concat([north, south, west], join='inner') # Join them!

#return capital_buckets

#capital_buckets = capitalbuckets()
capital_buckets.to_csv('C:/Users/AC75737/CenturyLink/Parquet Files - General/data/cms_capital_file/capital_buckets.csv')

#if __name__ == '__main__':
#    capitalbuckets()
