<center>![alt text](https://i.ibb.co/YT5mXdV/lumen.jpg "Lumen")</center>
# Proactive Rehab Tracking
## PowerBI Back-End
### Utilizing Python v3.8.5

#### Created on: 12SEP2020

## Purpose

> The purpose of this Application is to facilitate the backend processing/"pre-processing" of Sharepoint data to identify Transformation projects, and their respective status.

## Structure

> The application fetches the 'live' Sharepoint data on a timeloop (interval=seconds), which is fed into multiple scripts, representing their respective filtered ''applied filters'' 

## Flow

> This data is then synced to a OneDrive folder, that is Synced to a OneDrive Teams Folder - which is monitored by PowerBI Pro Workspace for changes, and thus updated in the app at the selected interval (OneDrive Teams Folder default monitors on each hour without any additional scheduled refreshes).

## Running

1. Create config.py file in root project directory

> Example:

    USERNAME='[domain (i.e CTL)]\\[AC12345]' 
    PASSWORD='[YOUR_PASSWORD]'

    drive_letter = r'C:/' 
    folder_name = r'Path/To/OneDrive/Folder/'
    folder_name_historic = r'Path/To/OneDrive/Folder/historic'
    folder_to_save_files = drive_letter + folder_name
    historic_folder_to_save_files = drive_letter + folder_name_historic

2. Create Python Virtual Environment

i. Go to project root / folder in terminal

ii. 
> $ python -m pip install -U pip

iii.
>$ python -m pip install -r requirements.txt

3. Run File

>$ Python powerConn.py
