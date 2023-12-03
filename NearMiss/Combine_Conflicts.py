import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import glob
import ntpath
import csv
import datetime
import fnmatch
import os
import math

workspace=r"Z:\RTC_Multi-Modal\Virginia_Neil\Trajectories\conflict"
#workspace=r"Z:\Silver_Lake_Railroad\East\Trajectories\conflict"

fileType="summary_conflict.csv"
listed_df = []

for root, dirnames, filenames in os.walk(workspace):
    for filename in fnmatch.filter(filenames, '*'+fileType):
        file=os.path.join(root, filename)
        print(file)
        df = pd.read_csv(file)
        if len(df) != 0:
            listed_df.append(df)

df_out = pd.concat(listed_df)

#criteria1 = df_out['ObjectID'] == 1
#criteria2 = df_out['ObjectID_cf'] == 1
#criteria3 = df_out['directionDiff'] <= 15
#criteria4 = df_out['directionDiff'] >= -15

#df_out = df_out.drop(df_out[criteria1 & criteria2 & criteria3 & criteria4].index)

#df_out = df_out.loc[~(criteria1 & criteria2 & criteria3 & criteria4)]

df_out = df_out.query('(Class == 1 & Class_cf == 1 & (directionDiff > 15 | directionDiff < -15)) | (Class != 1 | Class_cf != 1)')

df_out.to_csv(r'Z:\RTC_Multi-Modal\Virginia_Neil\Trajectories\conflict\conflictSum_Integration_2.csv')

#df_out.to_csv(r'Z:\Virginia_SMP\Hills\Trajectories\conflict\conflictSum_Integration_2.csv')

#df_out['DateTime'] = pd.to_datetime(df_out['DateTime'])

#data_by_date = {date: group for date, group in df_out.groupby(df_out['DateTime'].dt.date)}

##for date, df in data_by_date.items():

    #df.to_csv(r'Z:\RTC_Multi-Modal\6th-University\Trajectories\conflict\conflictSum_Integration_2_Filter_' + str(date) + '.csv')
