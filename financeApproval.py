finance_approval = df

# Format Currency
finance_approval['Estimated Expense Repair $'] = finance_approval['Estimated Expense Repair $'].replace('/W', '') # Remove all non alphanumeric chars
finance_approval['Estimated Expense Repair $'] = pd.to_numeric(finance_approval['Estimated Expense Repair $'], errors='coerce').astype(float) # force to float for operand comparison
#finance_approval['Payback Years'] = finance_approval['Payback Years'].replace('1,503.5', '1500.5') # Remove all non alphanumeric chars
#finance_approval = finance_approval[finance_approval['Payback Years'].notna()]
finance_approval['Payback Years'] = pd.to_numeric(finance_approval['Payback Years'], errors='coerce').astype(float)