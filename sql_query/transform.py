import numpy as np

def log1p_columns(df):
    cols = list(df)
    log_cols = ['Log' + i for i in cols]
    for c in cols:
        lc = 'Log' + c
        df[lc] = np.zeros(len(df))
        indx_pos = df.index[df[c]> 0].tolist()
        df.loc[indx_pos, lc] = np.log1p(df.loc[indx_pos, c])
    return df[log_cols]

def binary_columns(df):
    cols = list(df)
    new_cols = ['Has' + i for i in cols]
    for c in cols:
        bc = 'Has' + c
        df[bc] = (df[c] > 0) & (df[c].notnull())
    return df[new_cols]

def to_zero(df):
    cols = list(df)
    for c in cols:
        idx = df.index[(df[c]< 0) | (df[c].isnull())].tolist()
        df.loc[idx, c] = 0    
    return df



