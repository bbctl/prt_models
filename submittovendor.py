from sharepy import all_items
import pandas as pd

def submittovendor():

    submit_to_vendor = all_items
    submit_to_vendor = submit_to_vendor[
        (submit_to_vendor['MRO Approval Date'].notna()) &
        (submit_to_vendor['Finance Approval'].notna()) &
        (submit_to_vendor['CMS Project #/SAP Invoice ID'].isna())
        ]
    return submit_to_vendor

submit_to_vendor = submittovendor()

if __name__ == '__main__':
    submittovendor()