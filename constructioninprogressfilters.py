from sharepy import all_items
import pandas as pd
import numpy as np

def constructionInProgress():

    con_ip = all_items
    con_ip = con_ip[
        (con_ip['Resolved Date'].isnull()) &
        (con_ip['Design Complete'].notna())
        ]
    return con_ip

con_ip = constructionInProgress()

if __name__ == '__main__':
    constructionInProgress()
   