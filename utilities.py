import pandas as pd

# need to add a rule to identify type2b rows after type2 rows
def isnode(s):
    if str(s)!='nan' and len(str(s).strip())>0:
        return 'type1'


def isleg(s):
    # if str(s)!='*Delete' or (str(s)!='nan' and len(str(s).strip())>2):
    #     return 'type2'
    if len(str(s).strip())>2:
        # print(str(s))
        return 'type2'


def filter_EN(old, new, EN_mat):
    spliced = EN_mat.copy()

    for r in EN_mat.index:
        for c in EN_mat.columns:
            # print(EN_mat.at[r, c])
            if EN_mat.at[r, c] == 'N':
                # print(f'{r},{c}')
                spliced.at[r, c] = new.at[r, c]
            else:
                # print(f'{r},{c}')
                spliced.at[r, c] = old.at[r, c]

    return spliced


def replace_tx_with_last_number(df, column_name):
    last_number = None

    for i in df.index:
        current_value = df.at[i, column_name]

        # Attempt to convert the current value to a numeric type, coerce errors to NaN
        current_value_numeric = pd.to_numeric(current_value, errors='coerce')

        # If conversion is successful (i.e., current_value_numeric is not NaN), update last_number
        if pd.notna(current_value_numeric) and current_value_numeric>0:
            last_number = current_value_numeric

        # If 'TX' is found, replace it with the last encountered number
        if isinstance(current_value, str) and (current_value[:2] == 'TX' or current_value[0] == '-'):
            # print(current_value[0])
            df.at[i, column_name] = str(last_number)

    return df
def adjust_ANODE_col(df):
    column_name = 'ANODE'
    last_number = None

    for i in range(len(df)):
        if i>=1:
           prev_node_val =  df.at[i-1, column_name]
        ANODE_value = df.at[i, column_name]
        nod_value = df.at[i, 'NOD']
        if isinstance(nod_value, str) and len(nod_value)>0 and (nod_value[:2] =='TX'
                or nod_value[0] == '-'):
            # print(nod_value)
            df.at[i, column_name] = str(prev_node_val)
        if len(ANODE_value)==1:
            df.at[i, column_name] = nod_value
    return df

def extract_type(row):
    if row['type2b'] == 'type2b':
        return 'type2b'
    if row['type2'] == 'type2':
        return 'type2'
    if row['type1'] == 'type1':
        return 'type1'

def delete_extra_rows(df,EN_mat):
    # Filter out rows where the first column has the value '*Delete'
    contain_delete_string = df.iloc[:, 0] == '*Delete'
    do_not_contain_delete_string = df.iloc[:, 0] != '*Delete'
    contain_E = EN_mat.iloc[:,0] == 'E'

    df_filtered = df[(contain_delete_string & contain_E) | do_not_contain_delete_string]
    return df_filtered

def align_EN_index(df,new):
    idx = new.index
    df_filtered = df.loc[idx]
    return df_filtered

# Function to classify based on length and occurrences
def classify_anode(value,occurrences,tab):
    # Increment the count of the current ANODE value in the occurrences dictionary
    if value not in occurrences:
        occurrences[value] = 0
    occurrences[value] += 1
    # print(value)
    # Apply classification rules
    if value == tab:
        return 'type1'
    elif 'signal' in value:
        return 'type3'
    elif occurrences[value] == 1:
        return 'type2'  # First occurrence

    else:
        return 'type2b'  # Second or more occurrences


def shift_old(old, shift_mat):
    # Iterate over rows
    for r in old.index:
        # Iterate over columns in reverse order
        for c in reversed(old.columns):
            s = shift_mat.at[r, c]
            if pd.notna(s):
                try:
                    shift = int(s)
                    old_col_index = c + shift  # Calculate old column index
                    # Ensure old column index is within bounds
                    if old_col_index < len(old.columns):
                        old.at[r, old.columns[old_col_index]] = old.at[r, c]
                        old.at[r, c] = ''#np.NaN
                except (ValueError, IndexError):
                    pass  # Handle invalid or out-of-bounds shifts

    return old

def EN_mat_consistency_check(old,EN_mat):

    EN_mat_check = EN_mat.copy()
    old_copy = old.copy()

    # EN_mat_index = EN_mat.index
    old_index = old.index
    # get which indices have value E on column 0
    EN_mat_check = EN_mat_check[[0]]
    # get which indices have value E on column 0
    EN_mat_check = EN_mat_check[EN_mat_check[0] == 'E']
    EN_mat_index = EN_mat_check.index
    idx_not_in_old = []
    for i,j in EN_mat_index:
        if j == 'type2b':
            if (i,j) not in old.index:
                print(i,j)
                idx_not_in_old.append((i,j))
    EN_mat = EN_mat.drop(index =idx_not_in_old )
    return EN_mat

def filter_EN_mat_signal_rows(EN_mat,new_signal_rows,old_signal_rows):
    try:
        using_old = EN_mat.loc[('signal0','type3'),0] == 'E'


    # EN_signal_rows = [r for r in EN_mat.index if 'type3' in r]

        if using_old:
            EN_mat_without_type_3 = EN_mat[EN_mat.index.get_level_values('type') != 'type3']
            EN_mat_type_3 = EN_mat[EN_mat.index.get_level_values('type') == 'type3'].head(old_signal_rows)
            EN_mat_out = pd.concat([EN_mat_without_type_3,EN_mat_type_3])
        else:
            EN_mat_without_type_3 = EN_mat[EN_mat.index.get_level_values('type') != 'type3']
            EN_mat_type_3 = EN_mat[EN_mat.index.get_level_values('type') == 'type3'].head(new_signal_rows)

    except:
        return EN_mat
    return EN_mat_out
# read dat

def process_DAT_file(dat_file='Two_pairs_DAT.DAT',skiprows=0):
    format_spec = [(0, 1), (1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 7), (7, 8), (8, 9), (9, 10), (10, 11), (11, 12),
                   (12, 13), (13, 14),
                   (14, 15), (15, 16), (16, 17), (17, 18), (18, 19), (19, 20), (20, 21), (21, 22), (22, 23), (23, 24),
                   (24, 25), (25, 26),
                   (26, 27), (27, 28), (28, 29), (29, 30), (30, 31), (31, 32), (32, 33), (33, 34), (34, 35), (35, 36),
                   (36, 37), (37, 38),
                   (38, 39), (39, 40), (40, 41), (41, 42), (42, 43), (43, 44), (44, 45), (45, 46),
                   (46, 47), (47, 48), (48, 49), (49, 50), (50, 51), (51, 52), (52, 53), (53, 54),
                   (54, 55), (55, 56), (56, 57), (57, 58), (58, 59), (59, 60), (60, 61), (61, 62),
                   (62, 63), (63, 64), (64, 65)]
    if skiprows == 1:
        dat = pd.read_fwf(dat_file, colspecs=format_spec, names=list(range(65)), skiprows=81,  dtype=str) # changed skiprows from 80 to 81
    if skiprows == 0:
        dat = pd.read_fwf(dat_file, colspecs=format_spec, names=list(range(65)),
                      dtype=str)  # changed skiprows from 80 to 81

    dat = dat.fillna('')

    dat['NOD'] = dat[0].replace(pd.NA, '') + dat[1].replace(pd.NA, '') + dat[2].replace(pd.NA, '') + dat[3].replace(
        pd.NA, '') + dat[4].replace(pd.NA, '')
    dat['ANODE'] = dat[5].replace(pd.NA, '') + dat[6].replace(pd.NA, '') + dat[7].replace(pd.NA, '') + dat[8].replace(
        pd.NA, '') + dat[9].replace(pd.NA, '')

    dat['NOD_label_raw'] = dat['NOD']
    dat['NOD_label_raw'] = dat['NOD_label_raw'].replace('',
                                                        pd.NA).ffill()  # Fidias please think what if there is a TX in hte node
    dat['NOD_label'] = dat['NOD_label_raw']

    # dat = dat.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    dat['type1'] = dat['NOD'].map(isnode)
    dat['type2'] = dat['ANODE'].map(isleg)
    dat['ANODE'] = dat['ANODE'].replace('', pd.NA).ffill()  # Fidias please think what if there is a TX in hte node

    dat = replace_tx_with_last_number(dat, 'NOD_label')
    dat = adjust_ANODE_col(dat)

    condition = dat[10] == "*"
    dat.loc[condition.shift(1, fill_value=False), 'type2b'] = 'type2b'
    return dat