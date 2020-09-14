from sharepy import all_items
import pandas as pd
import numpy as np

def pendingLniCosting():

    pending_lni = all_items
    pending_lni = pending_lni[
        (pending_lni['MRO Approval Date'].notna()) &
        (pending_lni['Estimated Expense Repair $'].isnull())
        ]
    return pending_lni

pending_lni = pendingLniCosting()

if __name__ == '__main__':
    pendingLniCosting()
   