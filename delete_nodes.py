import pandas as pd
import numpy as np
from utilities import *
import openpyxl

excel_path = r"/Users/fidiasieridis/Documents/GitHub/SATURN-node-processing/Nodes to change in swtm and sertm.xlsx"
links_d = pd.read_excel(excel_path,'SE to delete')['Node']
links_k = pd.read_excel(excel_path,'SW to keep')['NodeNo']

links_d,links_k = list(links_d),list(links_k)
# dat_file = 'SRTM2_Full.dat'
dat_file = 'SWTM_Full.dat'

format_spec = [(0,1),(1, 2), (2, 3) ,(3, 4) ,(4, 5) ,(5, 6) ,(6, 7) ,(7, 8) ,(8, 9) ,(9, 10) ,(10, 11) ,(11, 12) ,(12, 13) ,(13, 14) ,
(14, 15),(15, 16) ,(16, 17) ,(17, 18) ,(18, 19) ,(19, 20) ,(20, 21) ,(21, 22) ,(22, 23) ,(23, 24) ,(24, 25) ,(25, 26) ,
(26, 27) ,(27, 28) ,(28, 29) ,(29, 30) ,(30, 31) ,(31, 32) ,(32, 33) ,(33, 34) ,(34, 35) ,(35, 36) ,(36, 37) ,(37, 38) ,
(38, 39) ,(39, 40) ,(40, 41) ,(41, 42) ,(42, 43) ,(43, 44) ,(44, 45) ,(45, 46) ,
(46, 47) ,(47, 48) ,(48, 49) ,(49, 50) ,(50, 51) ,(51, 52) ,(52, 53) ,(53, 54) ,
(54, 55) ,(55, 56) ,(56, 57) ,(57, 58) ,(58, 59) ,(59, 60) ,(60, 61) ,(61, 62) ,
(62, 63) ,(63, 64) ,(64, 65)]# ,(65, 66) ,(66, 67) ,(67, 68) ,(68, 69) ,(69, 70) ,
# (70, 71) ,(71, 72) ,(72, 73) ,(73, 74) ,(74, 75) ]

dat = pd.read_fwf(dat_file, colspecs=format_spec, names=list(range(65)), skiprows=82,  dtype=str) # changed skiprows from 80 to 81
dat = dat.fillna('')

dat['NOD'] = dat[0].replace(pd.NA,'')+dat[1].replace(pd.NA,'')+dat[2].replace(pd.NA,'')+dat[3].replace(pd.NA,'')+dat[4].replace(pd.NA,'')
dat['ANODE'] = dat[5].replace(pd.NA,'')+dat[6].replace(pd.NA,'')+dat[7].replace(pd.NA,'')+dat[8].replace(pd.NA,'')+dat[9].replace(pd.NA,'')
# dat_signal_rows = dat.at[0,16]
# dat['NOD'] = dat.apply(lambda x:)
# if dat[0] == '*Delete':
#     dat[0] =


dat['NOD_label_raw'] = dat['NOD']
dat['NOD_label_raw'] = dat['NOD_label_raw'].replace('', pd.NA).ffill() # Fidias please think what if there is a TX in hte node
dat['NOD_label'] = dat['NOD_label_raw']

# dat = dat.applymap(lambda x: x.strip() if isinstance(x, str) else x)




dat['type1'] = dat['NOD'].map(isnode)
dat['type2'] = dat['ANODE'].map(isleg)
dat['ANODE'] = dat['ANODE'].replace('', pd.NA).ffill() # Fidias please think what if there is a TX in hte node

dat = replace_tx_with_last_number(dat,'NOD_label')
dat = adjust_ANODE_col(dat)


condition = dat[10]=="*"
dat.loc[condition.shift(1,fill_value=False),'type2b']='type2b'

dat_copy = dat.copy()

# links to keep
dat_keep = pd.DataFrame()
dat_file_links = list(dat['NOD_label'].unique())
links_to_check = [link for link in links_k if link in dat_file_links]
for link in links_to_check:
    if len(dat_copy[dat_copy['NOD_label'] == link])>0:
        print(len(dat_copy[dat_copy['NOD_label'] == link]))
        dat_keep = pd.merge([dat_keep,dat_copy[dat_copy['NOD_label'] == link]])


# links to delete
dat_filtered = dat.copy()
links_d_to_check = [link for link in links_d if link in dat_file_links]
for link in links_d_to_check:
        dat_filtered = dat_filtered[dat_filtered['NOD_label'] != link]

# alternatively
dat_filtered = dat.copy()
dat_filtered = dat_filtered[~dat_filtered['NOD_label'].isin(links_d)]