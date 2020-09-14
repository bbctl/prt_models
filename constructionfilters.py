from sharepy import all_items
import pandas as pd

def constructionFilters():

    ready_for_construction = all_items
    construction = ready_for_construction[
        (ready_for_construction['Design Complete'].isna()) &
        (ready_for_construction['CMS Project #/SAP Invoice ID'].notna())
        ]
    return construction

construction = constructionFilters()

if __name__ == '__main__':
    constructionFilters()