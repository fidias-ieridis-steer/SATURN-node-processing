import pandas as pd
import numpy as np
from utilities import *

excel_path = r"\\sdgworld.net\data\London\Projects\247\2\27\01\Model\01 - Coding\ToTest\LinkID_Updates.xlsx"
excel_path = r"C:\Fidias\Coding-related\Python\SATURN junctions processing\LinkID_Updates.xlsx"
excel_path = r"C:\Fidias\Coding-related\Python\SATURN junctions processing\LinkID_Updates v2.xlsx"

links = pd.read_excel(excel_path,'Data')
links = links.drop_duplicates()
cols = list(range(65))

df = process_DAT_file(dat_file='AM_Peak_Original.dat',skiprows=0)
df['NOD'] = df['NOD'].ffill()
df['link'] = df['NOD_label'] + '-' + df['ANODE']

# df['link'] = df['NOD'].astype('str') + '-' + df['ANODE'].astype('str')
links['old link'] = links['Old A'].astype('str') + '-' + links['Old B'].astype('str')
links['new link'] = links['New A'].astype('str') + '-' + links['New B'].astype('str')

# new_links = links[links['Old B'] == links['New B']]
# new_links = new_links.drop_duplicates()
new_links = links

links_not_in_dat = pd.merge(new_links['old link'],df['link'],left_on='old link',right_on='link',how='left')
links_not_in_dat['missing links'] = links_not_in_dat['link'].replace(np.NaN,0)
links_not_in_dat = links_not_in_dat[links_not_in_dat['missing links'] == 0]['old link']

modified_link = 'New A'
dfm = pd.merge(df,new_links[['old link','new link',modified_link]],how='left',left_on='link',right_on='old link')
dfm[modified_link] = dfm[modified_link].replace(np.NaN,0).astype('int').astype('str')

#New B cols 20-24
dfm['B-5'] = dfm[modified_link].map(lambda x:x[-5] if len(x)>4 else '')#np.nan)
dfm['B-6'] = dfm[modified_link].map(lambda x:x[-4] if len(x)>3 else '')#np.nan)
dfm['B-7'] = dfm[modified_link].map(lambda x:x[-3] if len(x)>2 else '')#np.nan)
dfm['B-8'] = dfm[modified_link].map(lambda x:x[-2] if len(x)>1 else '')#np.nan)
dfm['B-9'] = dfm[modified_link].map(lambda x:x[-1])


for i in range(5,10,1):
    # dfm[i] = dfm[f'B-{str(i)}'].map(lambda row:row if (row[modified_link] != '0') and (row['type2'] != 'type2') else row)
    # dfm[i] = dfm[f'B-{str(i)}'].apply(lambda row: row if (row[modified_link] != '0') and (row['type2'] != 'type2') else row)
    dfm[i] = dfm.apply(
    lambda row: row[f'B-{str(i)}'] if (row[modified_link] != '0') and (row['type2'] == 'type2') else row[i],
    axis=1
)

dfm[cols].to_csv('AM_Peak_modified.dat', sep=' ', index=False, header=False)
dfm[cols].apply(lambda row: ''.join(row.astype(str)), axis=1).to_csv("example.dat", index=False, header=False)

output2 = dfm[cols].applymap(lambda x: ' ' if x == '' else x)
# output2 = output2.drop(columns = ['NOD','ANODE', 'NOD_label_raw','NOD_label'])
format_spec2 = ['%s' for x in range(dfm[cols].shape[1])]
np.savetxt(f'AM_Peak_modified.dat', output2, fmt=format_spec2, delimiter='')
links_not_in_dat.to_csv('links_not_in_dat.csv')
# with open("AM_Peak_modified.dat", "w") as file:
#     for row in dfm[cols].itertuples(index=False):
#         file.write("".join(map(str, row)) + "\n")


value_counts = new_links['old link'].value_counts()
print(value_counts)
