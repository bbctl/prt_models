from sharepy import all_items
import pandas as pd
import numpy as np

def pendingMroApproval():

    pending_mro = all_items
    pending_mro = pending_mro[
        (pending_mro['MRO Approval Date'].isnull()) &
        (pending_mro['MRO EMAIL sent'] == 'Email Notification Sent')
        ]
    return pending_mro

pending_mro = pendingMroApproval()

print(pending_mro)

if __name__ == '__main__':
    pendingMroApproval()
   