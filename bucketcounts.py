import pandas as pd


def pendingCharacterization():

    df = pd.read_csv('C:/Users/AC75737/CenturyLink/Parquet Files - General/data/transformation_buckets/1_pending_characterization.csv', encoding='utf-8', engine='python')
    return df

df = pendingCharacterization()

df['Created'] = pd.to_datetime(df.Created) # This is the date column we're grouping by
x = df.groupby(pd.Grouper(key='Created', freq='43200min')) # 43200mins in 30 days
y = x['ID'].count()
# print(f'Pending Characterization: \n < 30 Days: {y[-1].sum()} \n 30-60 Days: {y[-2].sum()} \n 60-90 Days: {y[-3].sum()} \n > 90 Days: {y[:-3].sum()} \n     Total: {y.sum()}')

dfObj = pd.DataFrame(columns=['0-30', '30-60', '60-90', '90', 'Total'])
dfObj = dfObj.append({'0-30': y[-1].sum(), '30-60': y[-2].sum(), '60-90': y[-3].sum(), '90': y[:-3].sum(), 'Total': y.sum()}, ignore_index=True)
dfObj.to_csv('C:/Users/AC75737/CenturyLink/Parquet Files - General/data/transformation_buckets/bucket_counts/1_cts_pending_characterization.csv')
print(f'Step 1 \n    Pending Characterization: \n {dfObj}')

def financeApprovalCount():

    df = pd.read_csv('C:/Users/AC75737/CenturyLink/Parquet Files - General/data/transformation_buckets/3_pending_finance_approval.csv', encoding='utf-8', engine='python')
    return df

df = financeApprovalCount()

df['Created'] = pd.to_datetime(df.Created) # This is the date column we're grouping by
x = df.groupby(pd.Grouper(key='Created', freq='43200min')) # 43200mins in 30 days
y = x['ID'].count()
# print(f'Ready for Finance Approval: \n < 30 Days: {y[-1].sum()} \n 30-60 Days: {y[-2].sum()} \n 60-90 Days: 0 \n > 90 Days: 0 \n Total: {y.sum()}')

dfObj = pd.DataFrame(columns=['0-30', '30-60', 'Total'])
dfObj = dfObj.append({'0-30': y[-1].sum(), '30-60': y[-2].sum(), 'Total': y.sum()}, ignore_index=True)

dfObj.to_csv('C:/Users/AC75737/CenturyLink/Parquet Files - General/data/transformation_buckets/bucket_counts/3_cts_pending_finance_approval.csv')
print(f'Step 3 \n    Pending Finance Approval: \n {dfObj}')

def submitToVendor():

    df = pd.read_csv('C:/Users/AC75737/CenturyLink/Parquet Files - General/data/transformation_buckets/4_ready_to_submit_to_vendor.csv', encoding='utf-8', engine='python')
    return df

df = submitToVendor()

df['Created'] = pd.to_datetime(df.Created) # This is the date column we're grouping by
x = df.groupby(pd.Grouper(key='Created', freq='43200min')) # 43200mins in 30 days
y = x['ID'].count()
# print(f'Ready to Submit to Vendor: \n < 30 Days: {y[-1].sum()} \n 30-60 Days: {y[-2].sum()} \n 60-90 Days: {y[-3].sum()} \n > 90 Days: {y[:-3].sum()} \n     Total: {y.sum()}')

dfObj = pd.DataFrame(columns=['0-30', '30-60', '60-90', '90', 'Total'])
dfObj = dfObj.append({'0-30': y[-1].sum(), '30-60': y[-2].sum(), '60-90': y[-3].sum(), '90': y[:-3].sum(), 'Total': y.sum()}, ignore_index=True)

dfObj.to_csv('C:/Users/AC75737/CenturyLink/Parquet Files - General/data/transformation_buckets/bucket_counts/4_cts_ready_to_submit_to_vendor.csv')
print(f'Step 4 \n    Ready to Submit to Vendor: \n {dfObj}')


def readyForConstruction():

    df = pd.read_csv('C:/Users/AC75737/CenturyLink/Parquet Files - General/data/transformation_buckets/5_pending_construction.csv', encoding='utf-8', engine='python')
    return df

df = readyForConstruction()

df['Created'] = pd.to_datetime(df.Created) # This is the date column we're grouping by
x = df.groupby(pd.Grouper(key='Created', freq='43200min')) # 43200mins in 30 days
y = x['ID'].count()

# print(f'Ready for Construction: \n < 30 Days: {y[-1].sum()} \n 30-60 Days: {y[-2].sum()} \n 60-90 Days: {y[-3].sum()} \n > 90 Days: {y[:-3].sum()} \n     Total: {y.sum()}')

dfObj = pd.DataFrame(columns=['0-30', '30-60', '60-90', '90', 'Total'])
dfObj = dfObj.append({'0-30': y[-1].sum(), '30-60': y[-2].sum(), '60-90': y[-3].sum(), '90': y[:-3].sum(), 'Total': y.sum()}, ignore_index=True)

dfObj.to_csv('C:/Users/AC75737/CenturyLink/Parquet Files - General/data/transformation_buckets/bucket_counts/5_cts_pending_construction.csv')
print(f'Step 5 \n    Ready for Construction: \n {dfObj}')

def constructionInProgress():

    df = pd.read_csv('C:/Users/AC75737/CenturyLink/Parquet Files - General/data/transformation_buckets/6_construction_in_progress.csv', encoding='utf-8', engine='python')
    return df

df = constructionInProgress()

df['Created'] = pd.to_datetime(df.Created) # This is the date column we're grouping by
x = df.groupby(pd.Grouper(key='Created', freq='43200min')) # 43200mins in 30 days
y = x['ID'].count()

#print(f'Construction In Progress: \n < 30 Days: {y[-1].sum()} \n 30-60 Days: {y[-2].sum()} \n 60-90 Days: {y[-3].sum()} \n > 90 Days: {y[:-3].sum()} \n     Total: {y.sum()}')

dfObj = pd.DataFrame(columns=['0-30', '30-60', '60-90', '90', 'Total'])
dfObj = dfObj.append({'0-30': y[-1].sum(), '30-60': y[-2].sum(), '60-90': y[-3].sum(), '90': y[:-3].sum(), 'Total': y.sum()}, ignore_index=True)
dfObj.to_csv('C:/Users/AC75737/CenturyLink/Parquet Files - General/data/transformation_buckets/bucket_counts/6_cts_construction_in_progress.csv')
print(f'Step 6 \n    Construction In Progress: \n {dfObj}')

def completePendingPayment():

    df = pd.read_csv('C:/Users/AC75737/CenturyLink/Parquet Files - General/data/transformation_buckets/7_complete_pending_payment.csv', encoding='utf-8', engine='python')
    return df

df = completePendingPayment()

df['Created'] = pd.to_datetime(df.Created) # This is the date column we're grouping by
x = df.groupby(pd.Grouper(key='Created', freq='43200min')) # 43200mins in 30 days
y = x['ID'].count()

#print(f'Complete Pending Payment: \n < 30 Days: {y[-1].sum()} \n 30-60 Days: {y[-2].sum()} \n 60-90 Days: {y[-3].sum()} \n > 90 Days: {y[:-3].sum()} \n     Total: {y.sum()}')

dfObj = pd.DataFrame(columns=['0-30', '30-60', '60-90', '90', 'Total'])
dfObj = dfObj.append({'0-30': y[-1].sum(), '30-60': y[-2].sum(), '60-90': y[-3].sum(), '90': y[:-3].sum(), 'Total': y.sum()}, ignore_index=True)
dfObj.to_csv('C:/Users/AC75737/CenturyLink/Parquet Files - General/data/transformation_buckets/bucket_counts/7_cts_complete_pending_payment.csv')

print(f'Step 7 \n    Pending Payment: \n {dfObj}')
