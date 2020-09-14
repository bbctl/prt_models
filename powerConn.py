import time
import os
import sys
import config 
import pandas as pd 
from sharepy import all_items
from financeapprovalfilters import finance_approval
from characterizationfilters import characterization
from pendinglnicostingfilters import pending_lni
from constructionfilters import construction
from constructioninprogressfilters import con_ip
from completependingpaymentfilters import pending_payment
from submittovendor import submit_to_vendor
from timeloop import Timeloop
from datetime import timedelta
from datetime import datetime

pd.options.mode.chained_assignment = None  # default='warn'

tl = Timeloop() # Jobs will run on below schedules as long as this script runs uninterrupted in Bash

@tl.job(interval=timedelta(seconds=60)) # Runs every 5 Minutes
def pending_characterization_update():

    system_time = datetime.now().strftime("%Y-%m-%d_%I-%M-%S_%p") # Declared Within f() to refresh time

    try:
        print(f'Attempting to Print Pending Characterization .CSV {system_time}..')
        df = characterization
        df['Created'] = pd.to_datetime(df.Created) # This is the date column we're grouping by
        x = df.groupby([pd.Grouper(key='Created', freq='43200min'),'Ops Region']) # 43200mins in 30 days
        y = x['ID'].count()
        df = pd.DataFrame(columns=['0-30', '30-60', '60-90', '90', 'Total'])
        df = df.append({'0-30': y[-1].sum(), '30-60': y[-2].sum(), '60-90': y[-3].sum(), '90': y[:-3].sum(), 'Total': y.sum(), 'Updated_On': system_time}, ignore_index=True)
        dfObj = pd.concat([df, characterization], axis=1)
        dfObj.to_csv(config.folder_to_save_files + '/1_pending_characterization.csv')

    except OSError as err:
        print("OS error: {0}".format(err))
    except ValueError:
        print("Could not convert data to an integer.")
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise

@tl.job(interval=timedelta(seconds=60)) # Runs every 5 Minutes
def pending_lni_costing_update():

    system_time = datetime.now().strftime("%Y-%m-%d_%I-%M-%S_%p") # Declared Within f() to refresh time

    try:
        print(f'Attempting to Print Pending LNI Costing .CSV {system_time}..')
        df = characterization
        df['Created'] = pd.to_datetime(df.Created) # This is the date column we're grouping by
        x = df.groupby([pd.Grouper(key='Created', freq='43200min'),'Ops Region']) # 43200mins in 30 days
        y = x['ID'].count()
        df = pd.DataFrame(columns=['0-30', '30-60', '60-90', '90', 'Total'])
        df = df.append({'0-30': y[-1].sum(), '30-60': y[-2].sum(), '60-90': y[-3].sum(), '90': y[:-3].sum(), 'Total': y.sum(), 'Updated_On': system_time}, ignore_index=True)
        dfObj = pd.concat([df, characterization], axis=1)
        dfObj.to_csv(config.folder_to_save_files + '/3_pending_lni_costing.csv')

    except OSError as err:
        print("OS error: {0}".format(err))
    except ValueError:
        print("Could not convert data to an integer.")
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise


@tl.job(interval=timedelta(seconds=60)) # Runs every 5 Minutes
def finance_approval_update():

    system_time = datetime.now().strftime("%Y-%m-%d_%I-%M-%S_%p") # Declared Within f() to refresh time

    try:
        print(f'Attempting to Print Finance Approval .CSV {system_time}..')
        df = finance_approval
        df['Created'] = pd.to_datetime(df.Created) # This is the date column we're grouping by
        x = df.groupby(pd.Grouper(key='Created', freq='43200min')) # 43200mins in 30 days
        y = x['ID'].count()
        df = pd.DataFrame(columns=['0-30', '30-60', '60-90', '90', 'Total', 'Updated_On'])
        df = df.append({'0-30': y[-1].sum(), '30-60': y[-2].sum(), '60-90': y[-3].sum(), '90': y[:-3].sum(), 'Total': y.sum(), 'Updated_On': system_time}, ignore_index=True)
        dfObj = pd.concat([df, finance_approval], axis=1)
        dfObj.to_csv(config.folder_to_save_files + '/3_pending_finance_approval.csv')
    except OSError as err:
        print("OS error: {0}".format(err))
    except ValueError:
        print("Could not convert data to an integer.")
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise
    

@tl.job(interval=timedelta(seconds=60)) # Runs every 5 Minutes
def ready_to_submit_to_vendor_update():

    system_time = datetime.now().strftime("%Y-%m-%d_%I-%M-%S_%p") # Declared Within f() to refresh time

    try:
        print(f'Attempting to Print Ready to Submit to Vendor .CSV {system_time}..')
        df = submit_to_vendor
        df['Created'] = pd.to_datetime(df.Created) # This is the date column we're grouping by
        x = df.groupby(pd.Grouper(key='Created', freq='43200min')) # 43200mins in 30 days
        y = x['ID'].count()
        df = pd.DataFrame(columns=['0-30', '30-60', '60-90', '90', 'Total'])
        df = df.append({'0-30': y[-1].sum(), '30-60': y[-2].sum(), '60-90': y[-3].sum(), '90': y[:-3].sum(), 'Total': y.sum(), 'Updated_On': system_time}, ignore_index=True)
        dfObj = pd.concat([df, submit_to_vendor], axis=1)
        dfObj.to_csv(config.folder_to_save_files + '/4_ready_to_submit_to_vendor.csv')
    except OSError as err:
        print("OS error: {0}".format(err))
    except ValueError:
        print("Could not convert data to an integer.")
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise


@tl.job(interval=timedelta(seconds=60)) # Runs every 5 Minutes
def pending_construction_update():

    system_time = datetime.now().strftime("%Y-%m-%d_%I-%M-%S_%p") # Declared Within f() to refresh time

    try:        
        print(f'Attempting to Print Construction .CSV {system_time}..')
        df = construction
        df['Created'] = pd.to_datetime(df.Created) # This is the date column we're grouping by
        x = df.groupby(pd.Grouper(key='Created', freq='43200min')) # 43200mins in 30 days
        y = x['ID'].count()
        df = pd.DataFrame(columns=['0-30', '30-60', '60-90', '90', 'Total'])
        df = df.append({'0-30': y[-1].sum(), '30-60': y[-2].sum(), '60-90': y[-3].sum(), '90': y[:-3].sum(), 'Total': y.sum(), 'Updated_On': system_time}, ignore_index=True)
        dfObj = pd.concat([df, construction], axis=1)
        dfObj.to_csv(config.folder_to_save_files + '/5_pending_construction.csv')
    except OSError as err:
        print("OS error: {0}".format(err))
    except ValueError:
        print("Could not convert data to an integer.")
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise

@tl.job(interval=timedelta(seconds=60)) # Runs every 5 Minutes
def construction_in_progress_update():

    system_time = datetime.now().strftime("%Y-%m-%d_%I-%M-%S_%p") # Declared Within f() to refresh time

    try:
        print(f'Attempting to Print Construction In Progresss .CSV {system_time}..')
        df = con_ip
        df['Created'] = pd.to_datetime(df.Created) # This is the date column we're grouping by
        x = df.groupby(pd.Grouper(key='Created', freq='43200min')) # 43200mins in 30 days
        y = x['ID'].count()
        df = pd.DataFrame(columns=['0-30', '30-60', '60-90', '90', 'Total'])
        df = df.append({'0-30': y[-1].sum(), '30-60': y[-2].sum(), '60-90': y[-3].sum(), '90': y[:-3].sum(), 'Total': y.sum(), 'Updated_On': system_time}, ignore_index=True)
        dfObj = pd.concat([df, con_ip], axis=1)
        dfObj.to_csv(config.folder_to_save_files + '/6_construction_in_progress.csv')
    except OSError as err:
        print("OS error: {0}".format(err))
    except ValueError:
        print("Could not convert data to an integer.")
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise

@tl.job(interval=timedelta(seconds=60)) # Runs every 5 Minutes
def complete_pending_payment_update():

    system_time = datetime.now().strftime("%Y-%m-%d_%I-%M-%S_%p") # Declared Within f() to refresh time

    try:
        print(f'Attempting to Print Complete Pending Payment .CSV {system_time}..')
        df = pending_payment
        df['Created'] = pd.to_datetime(df.Created) # This is the date column we're grouping by
        x = df.groupby(pd.Grouper(key='Created', freq='43200min')) # 43200mins in 30 days
        y = x['ID'].count()
        df = pd.DataFrame(columns=['0-30', '30-60', '60-90', '90', 'Total'])
        df = df.append({'0-30': y[-1].sum(), '30-60': y[-2].sum(), '60-90': y[-3].sum(), '90': y[:-3].sum(), 'Total': y.sum(), 'Updated_On': system_time}, ignore_index=True)
        dfObj = pd.concat([df, pending_payment], axis=1)
        dfObj.to_csv(config.folder_to_save_files + '/7_complete_pending_payment.csv')
    except OSError as err:
        print("OS error: {0}".format(err))
    except ValueError:
        print("Could not convert data to an integer.")
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise

@tl.job(interval=timedelta(seconds=60)) # Runs every 5 Minutes
def sharepoint_latest():

    system_time = datetime.now().strftime("%Y-%m-%d_%I-%M-%S_%p") # Declared Within f() to refresh time

    try:
        print(f'Attempting to Print Sharepoint Historic snapshot .CSV of {system_time}..')
        df = all_items
        df['Created'] = pd.to_datetime(df.Created) # This is the date column we're grouping by
        x = df.groupby(pd.Grouper(key='Created', freq='43200min')) # 43200mins in 30 days
        y = x['ID'].count()
        df = pd.DataFrame(columns=['0-30', '30-60', '60-90', '90', 'Total'])
        df = df.append({'0-30': y[-1].sum(), '30-60': y[-2].sum(), '60-90': y[-3].sum(), '90': y[:-3].sum(), 'Total': y.sum(), 'Updated_On': system_time}, ignore_index=True)
        dfObj = pd.concat([df, all_items], axis=1)
        dfObj.to_csv(config.folder_to_save_files + '/0_sharepoint.csv')
    except OSError as err:
        print("OS error: {0}".format(err))
    except ValueError:
        print("Could not convert data to an integer.")
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise

@tl.job(interval=timedelta(seconds=60)) # Runs every 5 Minutes
def sharepoint_backup():

    system_time = datetime.now().strftime("%Y-%m-%d_%I-%M-%S_%p") # Declared Within f() to refresh time

    try:
        print(f'Attempting to Print Sharepoint Historic snapshot .CSV of {system_time}..')
        all_items.to_csv(config.historic_folder_to_save_files + '/sharepoint_backup/sharepoint' + system_time + '.csv')
    except OSError as err:
        print("OS error: {0}".format(err))
    except ValueError:
        print("Could not convert data to an integer.")
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise

@tl.job(interval=timedelta(seconds=60)) # Runs every 5 Minutes
def pending_characterization_historic():

    system_time = datetime.now().strftime("%Y-%m-%d_%I-%M-%S_%p") # Declared Within f() to refresh time

    try:
        print(f'Attempting to Print Pending Finance Approval Historic .CSV {system_time}..')
        finance_approval.to_csv(config.historic_folder_to_save_files + '/finance_approval/pending_finance_approval_' + system_time + '.csv')
    except OSError as err:
        print("OS error: {0}".format(err))
    except ValueError:
        print("Could not convert data to an integer.")
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise

@tl.job(interval=timedelta(seconds=60)) # Runs every 5 Minutes
def pending_charactarization_to_historic():

    system_time = datetime.now().strftime("%Y-%m-%d_%I-%M-%S_%p") # Declared Within f() to refresh time

    try:
        print(f'Attempting to Print Pending Characterization Historic .CSV {system_time}..')
        characterization.to_csv(config.historic_folder_to_save_files + '/characterization/pending_characterization_' + system_time + '.csv')
    except OSError as err:
        print("OS error: {0}".format(err))
    except ValueError:
        print("Could not convert data to an integer.")
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise
    
@tl.job(interval=timedelta(seconds=60)) # Runs every 5 Minutes
def construction_to_historic():

    system_time = datetime.now().strftime("%Y-%m-%d_%I-%M-%S_%p") # Declared Within f() to refresh time

    try:
        print(f'Attempting to Print Pending Construction Historic .CSV {system_time}..')
        construction.to_csv(config.historic_folder_to_save_files + '/construction/pending_construction_' + system_time + '.csv')
    except OSError as err:
        print("OS error: {0}".format(err))
    except ValueError:
        print("Could not convert data to an integer.")
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise

@tl.job(interval=timedelta(seconds=60)) # Runs every 5 Minutes
def submit_to_vendor_historic():

    system_time = datetime.now().strftime("%Y-%m-%d_%I-%M-%S_%p") # Declared Within f() to refresh time

    try:
        print(f'Attempting to Print Ready to Submit to Vendor Historic .CSV {system_time}..')
        submit_to_vendor.to_csv(config.historic_folder_to_save_files + '/submit_to_vendor/ready_to_submit_to_vendor_' + system_time + '.csv')
    except OSError as err:
        print("OS error: {0}".format(err))
    except ValueError:
        print("Could not convert data to an integer.")
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise

@tl.job(interval=timedelta(seconds=60)) # Runs every 5 Minutes
def construction_in_progress_historic():

    system_time = datetime.now().strftime("%Y-%m-%d_%I-%M-%S_%p") # Declared Within f() to refresh time

    try:
        print(f'Attempting to Print Construction In Progress Historic .CSV {system_time}..')
        con_ip.to_csv(config.historic_folder_to_save_files + '/construction_in_progress/construction_in_progress_' + system_time + '.csv')
    except OSError as err:
        print("OS error: {0}".format(err))
    except ValueError:
        print("Could not convert data to an integer.")
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise

@tl.job(interval=timedelta(seconds=60)) # Runs every 5 Minutes
def complete_pending_payment_historic():

    system_time = datetime.now().strftime("%Y-%m-%d_%I-%M-%S_%p") # Declared Within f() to refresh time

    try:
        print(f'Attempting to Print Complete Pending Payment Historic .CSV {system_time}..')
        pending_payment.to_csv(config.historic_folder_to_save_files + '/complete_pending_payment/complete_pending_payment_' + system_time + '.csv')
    except OSError as err:
        print("OS error: {0}".format(err))
    except ValueError:
        print("Could not convert data to an integer.")
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise

if __name__ == '__main__':
    tl.start(block=True)
