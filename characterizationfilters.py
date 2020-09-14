from sharepy import all_items
import pandas as pd

def pendingCharacterization():

    pending_char = all_items
    characterization = pending_char[
        (pending_char['MRO Approval Date'].isna()) &
        (pending_char['Savings Estimate'] != 0) 
        ]
    return characterization

characterization = pendingCharacterization()

if __name__ == '__main__':
    pendingCharacterization()