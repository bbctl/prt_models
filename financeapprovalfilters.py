from sharepy import all_items
import pandas as pd
import numpy as np

def financeApprovalPrep():

    finance_approval = all_items
    finance_approval['Estimated Expense Repair $'] = finance_approval['Estimated Expense Repair $'].replace('/W', '') # Remove all non alphanumeric chars
    finance_approval['Estimated Expense Repair $'] = pd.to_numeric(finance_approval['Estimated Expense Repair $'], errors='coerce').astype(float) # force to float for operand comparison
    finance_approval['Payback Years'] = finance_approval['Payback Years'].astype(str)
    finance_approval['Payback Years'] = finance_approval['Payback Years'].str.slice(7,11)
    finance_approval['Payback Years'] = pd.to_numeric(finance_approval['Payback Years'], errors='coerce').astype(float)
    finance_approval = finance_approval[
        (finance_approval['Current Status'] != 'Closed') &
        (finance_approval['Current Status'] != 'Not Economical') &
        (finance_approval['Project Type'] == 'Transformation') &
        (finance_approval['Finance Approval'].isnull()) &
        (finance_approval['MRO Approval Date'].notna()) &
        (finance_approval['Estimated Expense Repair $'].notna()) &
        (finance_approval['Estimated Expense Repair $'].astype(float) >= 1000) &
        (finance_approval['Resolved Date'].isnull()) &
	    (finance_approval['Payback Years'].notna()) &
        (finance_approval['Payback Years'].astype(float) <= 5) &
        (finance_approval['Payback Years'] != 0)
        ]
    return finance_approval

finance_approval = financeApprovalPrep()

def financeApprovalCounts():
    
    finance_approval = financeApprovalPrep()
    finance_approval_counts = finance_approval
    finance_approval_counts['Estimated Expense Repair $ SUM'] = finance_approval_counts['Estimated Expense Repair $'].sum()
    finance_approval_counts['Estimated Expense Repair $ TOTAL'] = finance_approval_counts['Estimated Expense Repair $'].count()

    return finance_approval_counts


finance_approval_counts = financeApprovalCounts()

if __name__ == '__main__':
    financeApprovalPrep()
    financeApprovalCounts()
    