from re import sub
import pandas as pd
import matplotlib.pyplot as plt
import datetime as dt
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import glob
import ntpath
import csv
import fnmatch
import os
import math
import arcpy
'''
df = pd.read_csv('Z:\Midtown_Study\Sites\Lawrence\ProcessedTrajs\speedVolume\AllZone_summary_speed.csv', usecols=['ObjectID','ZoneID','Year','Month','Day','Hour','Minute','Second'])

df = df.loc[(df['ZoneID'] == 4)]
'''

df = pd.read_csv(r"Z:\NSFSCCTrajectories\KeystoneAndMcCarran\New folder\nonV2V\Keystone_McCarran_GIS_nonV2V_cl.csv")
#dfdf = pd.read_csv(r"Z:\Virginia_SMP\Meadows_Marketplace\Analysis\Conflicts\Meadows_Marketplace_Conflicts_GIS_cl.csv")
#df = pd.read_csv(r"Z:\Silver_Lake_Railroad\West\Analysis\Conflicts\West_Conflicts_GIS_cl.csv")

df['DateTime'] = pd.to_datetime(df['DateTime_CR'])
df['Year'] = df.DateTime.dt.year
df['Month'] = df.DateTime.dt.month
df['Day'] = df.DateTime.dt.day
df['Hour'] = df.DateTime.dt.hour
df['Minute'] = df.DateTime.dt.minute
df['Second'] = df.DateTime.dt.second


conditions = [(df['Minute'] < 30), (df['Minute'] >= 30)]
values = ['00','30']
df['Minute_str'] = np.select(conditions,values)



conditions = [(df['Hour'] == 0), (df['Hour'] == 1), (df['Hour'] == 2), (df['Hour'] == 3), (df['Hour'] == 4), (df['Hour'] == 5), (df['Hour'] == 6), (df['Hour'] == 7), (df['Hour'] == 8), (df['Hour'] == 9), (df['Hour'] == 10), (df['Hour'] == 11), (df['Hour'] == 12), (df['Hour'] == 13), (df['Hour'] == 14), (df['Hour'] == 15), (df['Hour'] == 16), (df['Hour'] == 17), (df['Hour'] == 18), (df['Hour'] == 19),
             (df['Hour'] == 20), (df['Hour'] == 21), (df['Hour'] == 22), (df['Hour'] == 23)]
values = ['00','01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23']
df['Hour_str'] = np.select(conditions,values)


conditions = [(df['Day'] == 1), (df['Day'] == 2), (df['Day'] == 3), (df['Day'] == 4), (df['Day'] == 5), (df['Day'] == 6), (df['Day'] == 7), (df['Day'] == 8), (df['Day'] == 9), (df['Day'] == 10), (df['Day'] == 11), (df['Day'] == 12), (df['Day'] == 13), (df['Day'] == 14), (df['Day'] == 15), (df['Day'] == 16), (df['Day'] == 17), (df['Day'] == 18), (df['Day'] == 19),
             (df['Day'] == 20), (df['Day'] == 21), (df['Day'] == 22), (df['Day'] == 23), (df['Day'] == 24), (df['Day'] == 25), (df['Day'] == 26), (df['Day'] == 27), (df['Day'] == 28), (df['Day'] == 29), (df['Day'] == 30), (df['Day'] == 31)]
values = ['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31']
df['Day_str'] = np.select(conditions,values)


conditions = [(df['Month'] ==1),(df['Month'] ==2),(df['Month'] ==3),(df['Month'] ==4),(df['Month'] ==5),(df['Month'] ==6),(df['Month'] ==7),(df['Month'] ==8),(df['Month'] ==9),(df['Month'] ==10),(df['Month'] ==11),(df['Month'] ==12)]
values = ['01','02','03','04','05','06','07','08','09','10','11','12']
df['Month_str'] = np.select(conditions,values)


conditions = [(df['Year'] == 2021),(df['Year'] == 2022),(df['Year'] == 2023)]
values = ['2021','2022','2023']
df['Year_str'] = np.select(conditions,values)


path = r"Z:\RTC_Multi-Modal\Virginia_Neil\Trajectories"
#path = r"Z:\Marketing_Sites\US50_College\Trajectories"

dir_list = os.listdir(path)
df["File_Name"] = df['Year_str'] + '-' + df['Month_str'] + '-' + df['Day_str'] + '-' + \
                  df['Hour_str'] + '-' + df['Minute_str'] + '-' + '00_cl.csv'

listed_df = []
File_Names = df.File_Name.unique()
for f in File_Names:
    df_file = df.loc[(df['File_Name'] == f)]
    OID_NO = df_file['ObjectID_NO'].tolist()
    OID_CR = df_file['ObjectID_CR'].tolist()
    CID = df_file['OID_'].tolist()
    #CID = df_file['Conflict_ID'].tolist()
    Conflict_Angle = df_file['Conflict_Angle'].tolist()
    timeDiff = df_file['timeDiff'].tolist()
    #timeDiff = df_file['Time_Difference'].tolist()
    Distance = df_file['Distance'].tolist()
    Conflict_Type = df_file['Conflict_Type'].tolist()
    Conflict_Movements = df_file['Conflict_Movements'].tolist()
    print(OID_NO)
    print(OID_CR)
    print(len(Conflict_Movements))
    if f in dir_list:
        print(path + '\\' + f)
        df_traj = pd.read_csv(path + '\\' + f)
        for o_NO, o_CR, c, CA, TD, dist, CT,CM in zip(OID_NO,OID_CR,CID,Conflict_Angle,timeDiff,Distance,Conflict_Type,Conflict_Movements):
        #for o_NO, o_CR, c, CA, TD, CT,CM in zip(OID_NO,OID_CR,CID,Conflict_Angle,timeDiff,Conflict_Type,Conflict_Movements):

            df_traj_temp = df_traj.loc[(df_traj['ObjectID'] == o_NO) | (df_traj['ObjectID'] == o_CR)]
            print(c)
            print(df_traj_temp)
            df_traj_temp["C_ID"] = c
            df_traj_temp["Conflict_Angle"] = CA
            df_traj_temp["timeDiff"] = TD
            df_traj_temp["Distance"] = dist
            df_traj_temp["Conflict_Type"] = CT
            df_traj_temp["Conflict_Movements"] = CM
            listed_df.append(df_traj_temp)

    else:
        print(f + ' is not in directory list.')

df_out = pd.concat(listed_df,axis=0)
df_out.to_csv(r'Z:\NSFSCCTrajectories\KeystoneAndMcCarran\New folder\nonV2V\Keystone_McCarran_Conflicts_GIS_cl_Traj.csv')
arcpy.env.workspace = r"Z:\NSFSCCTrajectories\KeystoneAndMcCarran\KeystoneAndMcCarran.gdb"
arcpy.management.XYTableToPoint(r'Z:\NSFSCCTrajectories\KeystoneAndMcCarran\New folder\nonV2V\Keystone_McCarran_Conflicts_GIS_cl_Traj.csv',"Keystone_McCarran_Conflicts_GIS_cl_Traj","Longitude","Latitude")
#df_out.to_csv(r'Z:\Marketing_Sites\US50_Airport\Analysis\Conflicts\US50_College_Conflicts_GIS_cl_Traj.csv')
##arcpy.env.workspace = r"Z:\Flamingo_SMP\Bruce\Bruce.gdb"
#arcpy.management.XYTableToPoint(r'Z:\Marketing_Sites\US50_Airport\Analysis\Conflicts\US50_College_Conflicts_GIS_cl_Traj.csv',"US50_College_Conflicts_GIS_cl_Traj","Longitude","Latitude")
'''
arcpy.env.workspace = r"Z:\RTC_Multi-Modal\Sparks-Barring\Sparks-Barring.gdb"

df_out = pd.read_csv(r'Z:\RTC_Multi-Modal\Sparks-Barring\Analysis\Conflict\Sparks_Barring_Conflicts_GIS_cl_traj.csv')

Conflict_Movements = df_out.Conflict_Movements.unique()
for CM in Conflict_Movements:
    df_temp = df_out.loc[(df_out['Conflict_Movements'] == CM)]
    print(CM)
    df_temp.to_csv(r"Z:\RTC_Multi-Modal\Sparks-Barring\Analysis\Conflict\Sparks_Barring_Conflicts_GIS_cl_traj_" + CM + r".csv")
    arcpy.management.XYTableToPoint(r"Z:\RTC_Multi-Modal\Sparks-Barring\Analysis\Conflict\Sparks_Barring_Conflicts_GIS_cl_traj_" + CM + r".csv","Sparks_Barring_Conflicts_traj_" + CM,"Longitude","Latitude")
'''
#



'''
conditions = [(df['Hour'] == 0), (df['Hour'] == 1), (df['Hour'] == 2), (df['Hour'] == 3), (df['Hour'] == 4), (df['Hour'] == 5), (df['Hour'] == 6), (df['Hour'] == 7), (df['Hour'] == 8), (df['Hour'] == 9), (df['Hour'] == 10), (df['Hour'] == 11), (df['Hour'] == 12), (df['Hour'] == 13), (df['Hour'] == 14), (df['Hour'] == 15), (df['Hour'] == 16), (df['Hour'] == 17), (df['Hour'] == 18), (df['Hour'] == 19),
             (df['Hour'] == 20), (df['Hour'] == 21), (df['Hour'] == 22), (df['Hour'] == 23)]
values = ['0','1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23']
df['Hour_str'] = np.select(conditions,values)


conditions = [(df['Day'] == 1), (df['Day'] == 2), (df['Day'] == 3), (df['Day'] == 4), (df['Day'] == 5), (df['Day'] == 6), (df['Day'] == 7), (df['Day'] == 8), (df['Day'] == 9), (df['Day'] == 10), (df['Day'] == 11), (df['Day'] == 12), (df['Day'] == 13), (df['Day'] == 14), (df['Day'] == 15), (df['Day'] == 16), (df['Day'] == 17), (df['Day'] == 18), (df['Day'] == 19),
             (df['Day'] == 20), (df['Day'] == 21), (df['Day'] == 22), (df['Day'] == 23), (df['Day'] == 24), (df['Day'] == 25), (df['Day'] == 26), (df['Day'] == 27), (df['Day'] == 28), (df['Day'] == 29), (df['Day'] == 30), (df['Day'] == 31)]
values = ['1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31']
df['Day_str'] = np.select(conditions,values)


conditions = [(df['Month'] ==1),(df['Month'] ==2),(df['Month'] ==3),(df['Month'] ==4),(df['Month'] ==5),(df['Month'] ==6),(df['Month'] ==7),(df['Month'] ==8),(df['Month'] ==9),(df['Month'] ==10),(df['Month'] ==11),(df['Month'] ==12)]
values = ['1','2','3','4','5','6','7','8','9','10','11','12']
df['Month_str'] = np.select(conditions,values)


conditions = [(df['Year'] == 2021),(df['Year'] == 2022)]
values = ['2021','2022']
df['Year_str'] = np.select(conditions,values)
'''