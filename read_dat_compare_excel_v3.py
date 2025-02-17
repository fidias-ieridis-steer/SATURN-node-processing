# -*- coding: utf-8 -*-
"""
Created on Tue Oct 15 10:50:19 2024

@author: MMa
"""

import pandas as pd
import numpy as np
from utilities import *


dat_file = 'AM_Peak_modified.dat'

format_spec = [(0,1),(1, 2), (2, 3) ,(3, 4) ,(4, 5) ,(5, 6) ,(6, 7) ,(7, 8) ,(8, 9) ,(9, 10) ,(10, 11) ,(11, 12) ,(12, 13) ,(13, 14) ,
(14, 15),(15, 16) ,(16, 17) ,(17, 18) ,(18, 19) ,(19, 20) ,(20, 21) ,(21, 22) ,(22, 23) ,(23, 24) ,(24, 25) ,(25, 26) ,
(26, 27) ,(27, 28) ,(28, 29) ,(29, 30) ,(30, 31) ,(31, 32) ,(32, 33) ,(33, 34) ,(34, 35) ,(35, 36) ,(36, 37) ,(37, 38) ,
(38, 39) ,(39, 40) ,(40, 41) ,(41, 42) ,(42, 43) ,(43, 44) ,(44, 45) ,(45, 46) ,
(46, 47) ,(47, 48) ,(48, 49) ,(49, 50) ,(50, 51) ,(51, 52) ,(52, 53) ,(53, 54) ,
(54, 55) ,(55, 56) ,(56, 57) ,(57, 58) ,(58, 59) ,(59, 60) ,(60, 61) ,(61, 62) ,
(62, 63) ,(63, 64) ,(64, 65)]# ,(65, 66) ,(66, 67) ,(67, 68) ,(68, 69) ,(69, 70) ,
# (70, 71) ,(71, 72) ,(72, 73) ,(73, 74) ,(74, 75) ]

dat = pd.read_fwf(dat_file, colspecs=format_spec, names=list(range(65)), skiprows=81,  dtype=str) # changed skiprows from 80 to 81
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



# read xlsx file
excel_file = 'Junction Coding Template V2.6 - For Testing_v2.xlsx'
excel_file = 'Junction Coding Template V2.6 - For Testing_v3.xlsx'
excel_file = 'Junction Coding Template V2.8.xlsx'

sheet_names = pd.ExcelFile(excel_file).sheet_names
sheet_names = sheet_names[2:]

# tab = excel_tab[4]

excel_tab = ['10379',
 '10885',
 '15559',
 '10887',
 '10891',
 '10384',
 '10309',
 '13217',
 '11187',
 '16553',
 '10518',
 '11183',
 '10623',
 '10900',
 '10400',
 '10391',
 '10398',
 '10951',
 '10491',
 '10401',
 '11065',
 '10562',
 '10558',
 '10559',
 '11047',
 '10519',
 '10389',
 '17024',
 '10901',
 '15128',
 '17027',
 '15131',
 '10397',
 '10914',
 '10402']

# tab = '10379'

outputs_only = pd.DataFrame()
for tab in excel_tab:
    #12/12/24
    # use_new = pd.read_excel(excel_file, sheet_name=tab, usecols='C', skiprows=3, nrows=1).iloc[0, 0]
    #10/01/25
    use_new = pd.read_excel(excel_file, sheet_name=tab, usecols='DM', skiprows=3, nrows=1).iloc[0, 0]
    if use_new == 'N':
        use_new = 'new'
    how_many_rows = pd.read_excel(excel_file, sheet_name = tab, usecols='AN', skiprows=6, nrows=1).iloc[0,0]
    # print(how_many_rows)

    new = pd.read_excel(excel_file, sheet_name = tab, usecols=('AO:DA'), dtype=str, skiprows=6, nrows=how_many_rows,
                          names=list(range(0,65)))


    EN_mat = pd.read_excel(excel_file, sheet_name = tab, usecols=('DM:FY'), dtype=str, skiprows=6, nrows=how_many_rows,
                           names=list(range(0,65)))

    shift_mat = pd.read_excel(excel_file, sheet_name = tab, usecols=('GL:IX'), dtype=str, skiprows=6, nrows=how_many_rows,
                           names=list(range(0,65)))

    # delete extra rows
    new = delete_extra_rows(new,EN_mat)
    EN_mat = align_EN_index(EN_mat,new)
    shift_mat = align_EN_index(shift_mat,new)

    new = new.reset_index(drop=True)
    EN_mat = EN_mat.reset_index(drop=True)
    shift_mat = shift_mat.reset_index(drop=True)

    new = new.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    new['NOD'] = new[0].replace(pd.NA,'')+new[1].replace(pd.NA,'')+new[2].replace(pd.NA,'')+new[3].replace(pd.NA,'')+new[4].replace(pd.NA,'')
    new['ANODE'] = new[5].replace(pd.NA,'')+new[6].replace(pd.NA,'')+new[7].replace(pd.NA,'')+new[8].replace(pd.NA,'')+new[9].replace(pd.NA,'')
    new_signal_rows = ''
    try:
        if (int(new.at[0, 14]) == 3) and (int(new.at[0, 19])>0):
            signal_check = True
        else:
            signal_check = False
    except:
        signal_check = False
    if signal_check:
        new_signal_rows = int(new.at[0, 19])


    new['type1'] = new['NOD'].map(isnode) #AttributeError: 'float' object has no attribute 'strip'
    new['type2'] = new['ANODE'].map(isleg)

    new['NOD_label_raw'] = new['NOD']
    new['NOD_label_raw'] = new['NOD_label_raw'].replace('',pd.NA).ffill()  # Fidias please think what if there is a TX in hte node
    new['NOD'] = new['NOD'].replace('',pd.NA).ffill()  # Fidias please think what if there is a TX in hte node
    new['NOD_label'] = new['NOD_label_raw']
    new = replace_tx_with_last_number(new, 'NOD_label')

    new['ANODE'] = new['ANODE'].replace('',pd.NA).ffill()
    new['ANODE'] = new['ANODE'].fillna(new['NOD'])
    new = adjust_ANODE_col(new)

    condition = new[10]=="*"
    new.loc[condition.shift(1,fill_value=False),'type2b']='type2b'

    occurrences = {}

    #add type column
    if new_signal_rows:
        for i in range(1,new_signal_rows+1):
            new['ANODE'].iloc[-i] = f'signal{new_signal_rows-i}'
    new['type'] = new['ANODE'].apply(classify_anode, args=(occurrences,tab))

    # new['type'] = new.apply(extract_type,axis=1)
    newnode = [a.strip() for a in list(new['NOD'].unique())]


    newnode = list(filter(lambda text: text.isdigit(), newnode))
    new = new.set_index(['ANODE','type'])
    EN_mat.index = new.index
    # old is missing a row
    if use_new != 'new':
        old = dat.loc[dat['NOD_label'].isin(newnode)].reset_index(drop=True)
        old_numeric_cols = old.columns
        # if len(old)>0:
        old_signal_rows = ''
        try:
            if int(old.iloc[0, 19])>0:
                signal_check = True
        except:
            signal_check = False
        # if old.iloc[0, 19] == '0' or not np.isnan(old.iloc[0, 19]):
        if signal_check:
            old_signal_rows =  int(old.iloc[0, 19])
        # old['ANODE'].iloc[-old_signal_rows:] = 'signal'
        if 'old_signal_rows' in locals():
            for i in range(1,old_signal_rows+1):
                old['ANODE'].iloc[-i] = f'signal{old_signal_rows-i}'


        # old numeric cols
        old_numeric_cols = [x for x in old.columns if isinstance(x, int)]
        # use shift_mat to shift to the right
        if len(old)>0:
            old[old_numeric_cols] = shift_old(old[old_numeric_cols], shift_mat)

        occurrences = {}
        old['type'] = old['ANODE'].apply(classify_anode, args=(occurrences,tab))

        # old['type'] = old.apply(extract_type,axis=1)
        # old.at[0,'ANODE'] = old.at[0,'NOD']
        old = old.set_index(['ANODE','type'])

        # filter EN_Mat based on expected signal rows and E/N
        if new_signal_rows or old_signal_rows:
            EN_mat = filter_EN_mat_signal_rows(EN_mat,new_signal_rows,old_signal_rows)

        #cut old to the right shape
        old = old.iloc[:,0:EN_mat.shape[1]] # I cut the old one slim, but idealy should be both 75 cols


        EN_mat = EN_mat_consistency_check(old,EN_mat)

    # create spliced
    new_numeric_cols = [x for x in new.columns if isinstance(x, int)]

    # added 12/12/2024
    if use_new == 'new':
        spliced = filter_EN(new, new, EN_mat)
    else:
        spliced = filter_EN(old, new, EN_mat)

    # add any missing signal rows to spliced
    if 'old_signal_rows' in locals():
        spliced_signal_rows = 0
        for i,j in spliced.index:
            if 'signal' in i:
                spliced_signal_rows += 1
        spliced = spliced.iloc[:-spliced_signal_rows]

        spliced = pd.concat([spliced,old.iloc[-old_signal_rows:,:]])


    try:
        output = pd.concat([old, new, spliced])
    except NameError:
        output = pd.concat([new,spliced])
    output = output.drop(columns=['type1','type2','type2b'])


    output = output.fillna('')
    output = output.drop(columns = ['NOD', 'NOD_label_raw','NOD_label'])

    # reformat output file for individual junction
    output2 = output.applymap(lambda x: ' ' if x == '' else x)
    format_spec2 = ['%s' for x in range(output.shape[1])]
    np.savetxt(f'output_{tab}.txt', output2, fmt=format_spec2, delimiter='')
    outputs_only = pd.concat([outputs_only,spliced])

# reformat the output file containing just the output/spliced coding
outputs_only2 = outputs_only.fillna('')
outputs_only2 = outputs_only2.applymap(lambda x: ' ' if x == '' else x)
format_spec_output2 = ['%s' for x in range(outputs_only.shape[1])]
# the output file named after the spreadsheet used
np.savetxt(f'{excel_file[:-5]}_all_outputs.txt', outputs_only2, fmt=format_spec_output2, delimiter='')
