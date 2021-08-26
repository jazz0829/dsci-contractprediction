import numpy as np
import pandas as pd

def creat_churn_indicator(df, end_date):
    churned = np.zeros(len(df))
    churned[(df.FullCancellationRequestDate <= end_date) | (df.LatestCommFinalDate <= end_date)] = 1
    return churned

def create_downgrade_indicator(df):
    downgraded = np.zeros(len(df))
    downgraded[df.DowngradeDate.notnull()] = 1
    return downgraded

def get_valid_comm_days(df, reference_date, window):
    window_start_date = reference_date - pd.DateOffset(days=window)
    valid_start_dates = df.FirstCommStartDate.apply(lambda x: max([x, window_start_date]))
    valid_days = (reference_date - valid_start_dates).dt.days + 1
    return valid_days

def get_valid_trial_days(df, reference_date, window):
    window_start_date = reference_date - pd.DateOffset(days=window)
    valid_start_dates = df.FirstTrialStartDate.apply(lambda x: max([x, window_start_date]))
    valid_days = np.zeros(len(df))
    idx_hadTrial = df.index[(df.LatestTrialFinalDate <= df.FirstCommStartDate) &
                            (df.LatestTrialFinalDate >= window_start_date)]
    valid_days[idx_hadTrial] = (df.loc[idx_hadTrial, 
             "LatestTrialFinalDate"] - valid_start_dates.loc[idx_hadTrial]).dt.days + 1   
    return valid_days