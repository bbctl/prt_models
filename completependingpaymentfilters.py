from sharepy import all_items
import pandas as pd
import numpy as np

def completePendingPayment():

    pending_payment = all_items
    pending_payment = pending_payment[
        (pending_payment['Resolved Date'].notna()) &
        (pending_payment['Invoice Paid'].isnull())
        ]
    return pending_payment

pending_payment = completePendingPayment()

if __name__ == '__main__':
    completePendingPayment()
   