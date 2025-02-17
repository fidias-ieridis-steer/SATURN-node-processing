import pandas as pd
import numpy as np
from utilities import *



dat_file = 'Two_pairs_DAT.DAT'
dat_file = 'SE_B19_TS1_Ass_net_v021_m_v07c_I6_Dat.DAT'
dat = process_DAT_file(dat_file=dat_file,skiprows=0)


dat['link'] = dat['NOD_label'] + '-' + dat['ANODE']

# for testing purposes
# dat = dat[:-1]

excel_path = r'LinkChanges.xlsx'

link_changes = pd.read_excel(excel_path)
link_changes['link'] = link_changes['A Node'].astype('str') + '-' + link_changes['B Node'].astype('str')

merged_df = pd.merge(dat,link_changes,'left')
merged_df['A Node'] = merged_df['A Node'].fillna(0).astype('int')
merged_df['B Node'] = merged_df['B Node'].fillna(0).astype('int')

merged_df['Distance'] = merged_df['Distance'].fillna(0).astype('int')
merged_df['Distance'] = merged_df['Distance'].astype('str')

merged_df['Speed'] = merged_df['Speed'].fillna(0).astype('int')
merged_df['Speed'] = merged_df['Speed'].astype('str')

merged_df['SFC'] = merged_df['SFC'].fillna(0).astype('int')
merged_df['SFC'] = merged_df['SFC'].astype('str')


# speed cols 17-19
merged_df['S-17'] = merged_df['Speed'].map(lambda x:x[-3] if len(x)>2 else '')#np.nan)
merged_df['S-18'] = merged_df['Speed'].map(lambda x:x[-2] if len(x)>1 else '')#np.nan)
merged_df['S-19'] = merged_df['Speed'].map(lambda x:x[-1])


#distance cols 20-24
merged_df['D-20'] = merged_df['Distance'].map(lambda x:x[-5] if len(x)>4 else '')#np.nan)
merged_df['D-21'] = merged_df['Distance'].map(lambda x:x[-4] if len(x)>3 else '')#np.nan)
merged_df['D-22'] = merged_df['Distance'].map(lambda x:x[-3] if len(x)>2 else '')#np.nan)
merged_df['D-23'] = merged_df['Distance'].map(lambda x:x[-2] if len(x)>1 else '')#np.nan)
merged_df['D-24'] = merged_df['Distance'].map(lambda x:x[-1])

#SFC cols on type2b (with * on type2) cols 42-44 - if no type2b row exists- then create one
merged_df['C-42'] = merged_df['SFC'].map(lambda x:x[-3] if len(x)>2 else '')#np.nan)
merged_df['C-43'] = merged_df['SFC'].map(lambda x:x[-2] if len(x)>1 else '')#np.nan)
merged_df['C-44'] = merged_df['SFC'].map(lambda x:x[-1])



# apply the link_changes inputs
merged_df[17] = merged_df['S-17']
merged_df[18] = merged_df['S-18']
merged_df[19] = merged_df['S-19']

merged_df[20] = merged_df['D-20']
merged_df[21] = merged_df['D-21']
merged_df[22] = merged_df['D-22']
merged_df[23] = merged_df['D-23']
merged_df[24] = merged_df['D-24']

# check if type2b row exists
merged_df['type2b-exists'] = merged_df.apply(lambda row:1 if row['type2b'] == 'type2b' else 0,axis=1)
# merged_df['type2b-exists-on-next-row'] = merged_df['type2b-exists'] .shift(-1)


# the following would work only when type2b row already exists for that specific link. Might need to develop further
# for cases where a new type2b row needs to be added
merged_df[42] = merged_df.apply(lambda row:row['C-42'] if row['type2b'] == 'type2b' else row[42],axis=1)
merged_df[43] = merged_df.apply(lambda row:row['C-43'] if row['type2b'] == 'type2b' else row[43],axis=1)
merged_df[44] = merged_df.apply(lambda row:row['C-44'] if row['type2b'] == 'type2b' else row[44],axis=1)

helper_df = merged_df.groupby('link').sum('type2b-exists')
helper_df = helper_df[helper_df['A Node'] !=0]
helper_df = helper_df.reset_index()
helper_df['add-row'] = helper_df.apply(lambda row:(1-row['type2b-exists']),axis=1)
helper_df = helper_df[['link','add-row']]

output = pd.merge(merged_df,helper_df)




# Iterate and create a new DataFrame
new_rows = []
for i, row in output.iterrows():
    new_rows.append(row)
    if row['add-row'] == 1:
        # Create a new row with all values as 1
        new_row = row.copy()
        for i in range(65):
            # print(i)
            if i in [42,43,44]:
                new_row[i] = new_row['C-' + str(i)]
            else:
                new_row[i] = ''
        new_rows.append(new_row)

# Reconstruct the DataFrame
result_df = pd.DataFrame(new_rows).reset_index(drop=True)