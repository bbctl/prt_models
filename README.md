![alt text](https://i.ibb.co/YT5mXdV/lumen.jpg "Lumen")
# Proactive Rehab Tracking
## PowerBI Back-End
### Utilizing Python v3.8.5


#### Created by: 
#### Created on: 12SEP2020
#### Manager: 


## Purpose

> Insert Purpose Here

## Structure

> Breakdown Structure Here

## Flow

> Explain Back-End Workflow here

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
ii. > $ python -m pip install -U pip
iii. > $ python -m pip install -r requirements.txt

3. Run File

i. $ Python powerConn.py
