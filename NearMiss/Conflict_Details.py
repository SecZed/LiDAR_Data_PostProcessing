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


df = pd.read_csv(r"Z:\NSFSCCTrajectories\KeystoneAndMcCarran\New folder\nonV2V\Keystone_McCarran_GIS_nonV2V.csv")
#df = pd.read_csv(r"Z:\Marketing_Sites\US50_College\Analysis\Conflicts\US50_College_Conflicts_GIS.csv")
#df = pd.read_csv(r"Z:\Silver_Lake_Railroad\West\Analysis\Conflicts\West_Conflicts_GIS.csv")


traj1 = '_NO'
traj2 = '_CR'
#df_thru = df.loc[((df['MoveAngle'] > 85) & (df['MoveAngle'] < 95)) & (df['Class'] != 2) & (df['Conflict_Movements'] == "WBLeft_EBThru")]
#df_thru_cf = df.loc[((df['MoveAngle_cf'] > 85) & (df['MoveAngle_cf'] < 95)) & (df['Class_cf'] != 2) & (df['Conflict_Movements'] == "WBLeft_EBThru")]
#df_thru = df.loc[((df['MoveAngle'] >= 0) & (df['MoveAngle'] <= 360)) & (df['Class'] == 1)]
#df_thru_cf = df.loc[((df['MoveAngle_cf'] >= 0) & (df['MoveAngle_cf'] <= 360)) & (df['Class_cf'] == 1)]

df_thru = df.loc[(((df['Conflict_Type'] == 'V2P') | (df['Conflict_Type'] == 'V2S') | (df['Conflict_Type'] == 'V2B')) & (df['Class'] == 1)) |  (((df['Conflict_Type'] == 'V2V') | (df['Conflict_Type'] == 'V2M')) & (df['MedianSpeed'] >= df['MedianSpeed_cf']))]
df_thru_cf = df.loc[(((df['Conflict_Type'] == 'V2P') | (df['Conflict_Type'] == 'V2S') | (df['Conflict_Type'] == 'V2B')) & (df['Class_cf'] == 1)) |  (((df['Conflict_Type'] == 'V2V') | (df['Conflict_Type'] == 'V2M')) & (df['MedianSpeed'] < df['MedianSpeed_cf']))]

df_thru.columns = ['OID_', 'Field1','ObjectIDs','Distance','Unnamed__0','index'+ traj2,'Timestamp'+ traj2, 'ObjectID' + traj2, 'FrameIndex' + traj2,'PredInd'+traj2, 'Coord_X' + traj2, 'Coord_Y' + traj2, 'Coord_Z' + traj2, 'Speed_X' + traj2, 'Speed_Y' + traj2, 'Speed' + traj2, 'Longitude' + traj2, 'Latitude' + traj2, 'Elevation' + traj2, 'Point_Cnt' + traj2, 'Dir_X_Bbox' + traj2, 
                    'Dir_Y_Bbox' + traj2, 'Height' + traj2, 'Length' + traj2, 'Width' + traj2, 'Area' + traj2, 'Dis' + traj2, 'Max_Length' + traj2, 'Class' + traj2, 'NewObjectID' + traj2, 'MoveAngle' + traj2,'DateTime' + traj2, 'AdjSpeed' + traj2, 'MedianSpeed' + traj2, 'Acceleration' + traj2, 'index'+ traj1, 'Timestamp'+ traj1,
                    'ObjectID' + traj1, 'FrameIndex' + traj1,'PredInd'+traj1, 'Coord_X' + traj1, 'Coord_Y' + traj1, 'Coord_Z' + traj1, 'Speed_X' + traj1, 'Speed_Y' + traj1, 'Speed' + traj1, 'Longitude' + traj1, 'Latitude' + traj1, 'Elevation' + traj1, 'Point_Cnt' + traj1, 'Dir_X_Bbox' + traj1, 
                    'Dir_Y_Bbox' + traj1, 'Height' + traj1, 'Length' + traj1, 'Width' + traj1, 'Area' + traj1, 'Dis' + traj1, 'Max_Length' + traj1, 'Class' + traj1,'NewObjectID' + traj1, 'MoveAngle' + traj1, 'DateTime' + traj1, 'AdjSpeed' + traj1, 'MedianSpeed' + traj1, 'Acceleration' + traj1, 
                    'timeDiff', 'directionDiff', 'distanceRange', 'Conflict_Type', 'trajAngleDifference', 'Conflict_Angle','Conflict_Movements'
]

df_thru_cf.columns = ['OID_', 'Field1','ObjectIDs','Distance','Unnamed__0','index'+ traj1,'Timestamp'+ traj1, 'ObjectID' + traj1, 'FrameIndex' + traj1,'PredInd'+traj1, 'Coord_X' + traj1, 'Coord_Y' + traj1, 'Coord_Z' + traj1, 'Speed_X' + traj1, 'Speed_Y' + traj1, 'Speed' + traj1, 'Longitude' + traj1, 'Latitude' + traj1, 'Elevation' + traj1, 'Point_Cnt' + traj1, 'Dir_X_Bbox' + traj1, 
                    'Dir_Y_Bbox' + traj1, 'Height' + traj1, 'Length' + traj1, 'Width' + traj1, 'Area' + traj1, 'Dis' + traj1, 'Max_Length' + traj1, 'Class' + traj1, 'NewObjectID' + traj1, 'MoveAngle' + traj1,'DateTime' + traj1, 'AdjSpeed' + traj1, 'MedianSpeed' + traj1, 'Acceleration' + traj1, 'index'+ traj2, 'Timestamp'+ traj2,
                    'ObjectID' + traj2, 'FrameIndex' + traj2,'PredInd'+traj2, 'Coord_X' + traj2, 'Coord_Y' + traj2, 'Coord_Z' + traj2, 'Speed_X' + traj2, 'Speed_Y' + traj2, 'Speed' + traj2, 'Longitude' + traj2, 'Latitude' + traj2, 'Elevation' + traj2, 'Point_Cnt' + traj2, 'Dir_X_Bbox' + traj2, 
                    'Dir_Y_Bbox' + traj2, 'Height' + traj2, 'Length' + traj2, 'Width' + traj2, 'Area' + traj2, 'Dis' + traj2, 'Max_Length' + traj2, 'Class' + traj2,'NewObjectID' + traj2,'MoveAngle' + traj2, 'DateTime' + traj2, 'AdjSpeed' + traj2, 'MedianSpeed' + traj2, 'Acceleration' + traj2, 
                    'timeDiff', 'directionDiff', 'distanceRange', 'Conflict_Type', 'trajAngleDifference', 'Conflict_Angle','Conflict_Movements'
]

#OID_	Field1	ObjectIDs	Distance	index	Timestamp	ObjectID_1	FrameIndex	PredInd	Coord_X	Coord_Y	Coord_Z	Speed_X	Speed_Y	Speed	Longitude	Latitude	Elevation	Point_Cnt	Dir_X_Bbox	Dir_Y_Bbox	Height	Length	Width	Area	Dis	Max_Length	Class	NewObjectID	MoveAngle	DateTime	AdjSpeed	MedianSpeed	Acceleration	index_cf	Timestamp_cf	ObjectID_cf	FrameIndex_cf	PredInd_cf	Coord_X_cf	Coord_Y_cf	Coord_Z_cf	Speed_X_cf	Speed_Y_cf	Speed_cf	Longitude_cf	Latitude_cf	Elevation_cf	Point_Cnt_cf	Dir_X_Bbox_cf	Dir_Y_Bbox_cf	Height_cf	Length_cf	Width_cf	Area_cf	Dis_cf	Max_Length_cf	Class_cf	NewObjectID_cf	MoveAngle_cf	DateTime_cf	AdjSpeed_cf	MedianSpeed_cf	Acceleration_cf	timeDiff	directionDiff	distanceRange	Conflict_Type	trajAngleDifference	Conflict_Angle	Conflict_Movements


df_out = pd.concat([df_thru, df_thru_cf])



conditions = [df_out['FrameIndex' + traj2] < df_out['FrameIndex' + traj1], df_out['FrameIndex' + traj2] > df_out['FrameIndex' + traj1]]
values = [1,0]
df_out['First' + traj2] = np.select(conditions,values)

#df_out.to_csv(r"Z:\Silver_Lake_Railroad\West\Analysis\Conflicts\West_Conflicts_GIS_cl.csv")

df_out.to_csv(r"Z:\NSFSCCTrajectories\KeystoneAndMcCarran\New folder\nonV2V\Keystone_McCarran_GIS_nonV2V_cl.csv")
#df_out.to_csv(r"Z:\Marketing_Sites\US50_College\Analysis\Conflicts\US50_College_Conflicts_GIS_cl.csv")


# '''
# workspace=r"Z:\HDR_LA\Round2\Sahara\Conflict"

# fileType="Conflicts.csv"
# listed_df = []

# for root, dirnames, filenames in os.walk(workspace):
#     for filename in fnmatch.filter(filenames, '*'+fileType):
#         file=os.path.join(root, filename)
#         print(file)
#         df = pd.read_csv(file)
#         if 'Conflict_Movement' in df.columns:
#             df_temp = df.loc[:,["Max_Length_NO","Max_Length_CR","Class_NO","Class_CR",'MoveAngle_NO','MoveAngle_CR','MedianSpeed_NO','MedianSpeed_CR','Acceleration_NO','Acceleration_CR','FrameIndex_NO','FrameIndex_CR','ObjectID_NO','ObjectID_CR','DateTime_NO','DateTime_CR','Longitude_NO','Longitude_CR','Latitude_NO','Latitude_CR','timeDiff','ObjectIDs','Conflict_Type','Conflict_Angle','Conflict_Movement','Conflict_Location','First_CR']]
#         elif 'Conflict_Movements' in df.columns:
#             df_temp = df.loc[:,["Max_Length_NO","Max_Length_CR","Class_NO","Class_CR",'MoveAngle_NO','MoveAngle_CR','MedianSpeed_NO','MedianSpeed_CR','Acceleration_NO','Acceleration_CR','FrameIndex_NO','FrameIndex_CR','ObjectID_NO','ObjectID_CR','DateTime_NO','DateTime_CR','Longitude_NO','Longitude_CR','Latitude_NO','Latitude_CR','timeDiff','ObjectIDs','Conflict_Type','Conflict_Angle','Conflict_Movements','Conflict_Location','First_CR']]
#         df_temp.columns = ["Max_Length_NO","Max_Length_CR","Class_NO","Class_CR",'MoveAngle_NO','MoveAngle_CR','MedianSpeed_NO','MedianSpeed_CR','Acceleration_NO','Acceleration_CR','FrameIndex_NO','FrameIndex_CR','ObjectID_NO','ObjectID_CR','DateTime_NO','DateTime_CR','Longitude_NO','Longitude_CR','Latitude_NO','Latitude_CR','timeDiff','ObjectIDs','Conflict_Type','Conflict_Angle','Conflict_Movement','Conflict_Location','First_CR']
#         df_temp['Site'] = filename[:-13]
#         df_temp['DateTime_NO'] = pd.to_datetime(df_temp['DateTime_NO'])
#         df_temp['DateTime_CR'] = pd.to_datetime(df_temp['DateTime_CR'])
#         df_temp['Day_of_Week'] = df_temp['DateTime_NO'].dt.day_name()
#         df_temp['Hour'] = df_temp['DateTime_NO'].dt.hour
#         df_temp['Day'] = df_temp['DateTime_NO'].dt.day
#         conditions = [(df_temp['Day'] == 6), (df_temp['Day'] == 8)]
#         values = ['Weekday','Weekend']
#         df_temp['Day_Type'] = np.select(conditions,values)   
#         df_temp.index = range(0, len(df_temp))
#         if len(df_temp) != 0:
#             listed_df.append(df_temp)

# df_out = pd.concat(listed_df)

# df_out.to_csv("Z:\HDR_LA\Round2\Sahara\Conflict\Sahara_Conflicts_2_cl.csv")




# weekdays = {'Arroyo':31,'California':13,'Cheney':21,'Larue':24,'Lawrence':21,'Liberty':13,'Martin':24,'Moran':13,'MtRose':31,'PlumbSouth':3,'Pueblo':27,'Regency':3,'Center':21,'Stewart':15,'Taylor':21,'Thoma':21,'Vassar':31,'Wells':3}
# weekends = {'Arroyo':29,'California':11,'Cheney':19,'Larue':22,'Lawrence':19,'Liberty':11,'Martin':22,'Moran':11,'MtRose':29,'PlumbSouth':5,'Pueblo':29,'Regency':5,'Center':19,'Stewart':11,'Taylor':19,'Thoma':19,'Vassar':30,'Wells':5}

# workspace=r"Z:\Midtown_Study\Network_Analysis\Conflict"

# fileType=".csv"
# listed_df = []

# for root, dirnames, filenames in os.walk(workspace):
#     for filename in fnmatch.filter(filenames, '*'+fileType):
#         file=os.path.join(root, filename)
#         print(file)
#         df = pd.read_csv(file)
#         if 'Conflict_Movement' in df.columns:
#             df_temp = df.loc[:,["Max_Length","Max_Length_cf","Class","Class_cf",'MoveAngle','MoveAngle_cf','MedianSpeed','MedianSpeed_cf','Acceleration','Acceleration_cf','FrameIndex','FrameIndex_cf','ObjectID','ObjectID_cf','DateTime','DateTime_cf','Longitude','Longitude_cf','Latitude','Latitude_cf','timeDiff','ObjectIDs','Conflict_Type','Conflict_Angle','Conflict_Movement']]
#         elif 'Conflict_Movements' in df.columns:
#             df_temp = df.loc[:,["Max_Length","Max_Length_cf","Class","Class_cf",'MoveAngle','MoveAngle_cf','MedianSpeed','MedianSpeed_cf','Acceleration','Acceleration_cf','FrameIndex','FrameIndex_cf','ObjectID','ObjectID_cf','DateTime','DateTime_cf','Longitude','Longitude_cf','Latitude','Latitude_cf','timeDiff','ObjectIDs','Conflict_Type','Conflict_Angle','Conflict_Movements']]
#         df_temp.columns = ["Max_Length","Max_Length_cf","Class","Class_cf",'MoveAngle','MoveAngle_cf','MedianSpeed','MedianSpeed_cf','Acceleration','Acceleration_cf','FrameIndex','FrameIndex_cf','ObjectID','ObjectID_cf','DateTime','DateTime_cf','Longitude','Longitude_cf','Latitude','Latitude_cf','timeDiff','ObjectIDs','Conflict_Type','Conflict_Angle','Conflict_Movement']
#         df_temp['Site'] = filename[:-13]
#         df_temp['DateTime'] = pd.to_datetime(df_temp['DateTime'])
#         df_temp['DateTime_cf'] = pd.to_datetime(df_temp['DateTime_cf'])
#         df_temp['Day_of_Week'] = df_temp['DateTime'].dt.day_name()
#         df_temp['Hour'] = df_temp['DateTime'].dt.hour
#         df_temp['Day'] = df_temp['DateTime'].dt.day
#         conditions = [(df_temp['Day'] == weekdays[filename[:-13]]), (df_temp['Day'] == weekends[filename[:-13]])]
#         values = ['Weekday','Weekend']
#         df_temp['Day_Type'] = np.select(conditions,values)   
#         df_temp.index = range(0, len(df_temp))
#         if len(df_temp) != 0:
#             listed_df.append(df_temp)

# df_out = pd.concat(listed_df)

# df_out.to_csv("Z:\Midtown_Study\Network_Analysis\Conflict\Conflicts_Midtown.csv")















# df_thru.columns = ['OID_', 'Field1','Timestamp'+ traj2, 'ObjectID' + traj2, 'FrameIndex' + traj2,'PredInd'+traj2, 'Coord_X' + traj2, 'Coord_Y' + traj2, 'Coord_Z' + traj2, 'Speed_X' + traj2, 'Speed_Y' + traj2, 'Speed' + traj2, 'Longitude' + traj2, 'Latitude' + traj2, 'Elevation' + traj2, 'Point_Cnt' + traj2, 'Dir_X_Bbox' + traj2, 
#                     'Dir_Y_Bbox' + traj2, 'Height' + traj2, 'Length' + traj2, 'Width' + traj2, 'Area' + traj2, 'Dis' + traj2, 'Max_Length' + traj2, 'Class' + traj2, 'MoveAngle' + traj2, 'DateTime' + traj2, 'AdjSpeed' + traj2, 'MedianSpeed' + traj2, 'Acceleration' + traj2, 'Timestamp'+ traj1,
#                     'ObjectID' + traj1, 'FrameIndex' + traj1,'PredInd'+traj1, 'Coord_X' + traj1, 'Coord_Y' + traj1, 'Coord_Z' + traj1, 'Speed_X' + traj1, 'Speed_Y' + traj1, 'Speed' + traj1, 'Longitude' + traj1, 'Latitude' + traj1, 'Elevation' + traj1, 'Point_Cnt' + traj1, 'Dir_X_Bbox' + traj1, 
#                     'Dir_Y_Bbox' + traj1, 'Height' + traj1, 'Length' + traj1, 'Width' + traj1, 'Area' + traj1, 'Dis' + traj1, 'Max_Length' + traj1, 'Class' + traj1, 'MoveAngle' + traj1, 'DateTime' + traj1, 'AdjSpeed' + traj1, 'MedianSpeed' + traj1, 'Acceleration' + traj1, 
#                     'timeDiff', 'directionDiff', 'distanceRange', 'ObjectIDs', 'Conflict_Type', 'trajAngleDifference', 'Conflict_Angle','Conflict_Movement','Conflict_Location'
# ]

# df_thru_cf.columns = ['OID_', 'Field1','Timestamp'+ traj1, 'ObjectID' + traj1, 'FrameIndex' + traj1,'PredInd'+traj1, 'Coord_X' + traj1, 'Coord_Y' + traj1, 'Coord_Z' + traj1, 'Speed_X' + traj1, 'Speed_Y' + traj1, 'Speed' + traj1, 'Longitude' + traj1, 'Latitude' + traj1, 'Elevation' + traj1, 'Point_Cnt' + traj1, 'Dir_X_Bbox' + traj1, 
#                     'Dir_Y_Bbox' + traj1, 'Height' + traj1, 'Length' + traj1, 'Width' + traj1, 'Area' + traj1, 'Dis' + traj1, 'Max_Length' + traj1, 'Class' + traj1, 'MoveAngle' + traj1, 'DateTime' + traj1, 'AdjSpeed' + traj1, 'MedianSpeed' + traj1, 'Acceleration' + traj1, 'Timestamp'+ traj2,
#                     'ObjectID' + traj2, 'FrameIndex' + traj2,'PredInd'+traj2, 'Coord_X' + traj2, 'Coord_Y' + traj2, 'Coord_Z' + traj2, 'Speed_X' + traj2, 'Speed_Y' + traj2, 'Speed' + traj2, 'Longitude' + traj2, 'Latitude' + traj2, 'Elevation' + traj2, 'Point_Cnt' + traj2, 'Dir_X_Bbox' + traj2, 
#                     'Dir_Y_Bbox' + traj2, 'Height' + traj2, 'Length' + traj2, 'Width' + traj2, 'Area' + traj2, 'Dis' + traj2, 'Max_Length' + traj2, 'Class' + traj2,'MoveAngle' + traj2, 'DateTime' + traj2, 'AdjSpeed' + traj2, 'MedianSpeed' + traj2, 'Acceleration' + traj2, 
#                     'timeDiff', 'directionDiff', 'distanceRange', 'ObjectIDs', 'Conflict_Type', 'trajAngleDifference', 'Conflict_Angle','Conflict_Movement','Conflict_Location'
# ]



# df_thru.columns = ['OID_', 'Field1', 'ObjectID' + traj2, 'FrameIndex' + traj2, 'Coord_X' + traj2, 'Coord_Y' + traj2, 'Coord_Z' + traj2, 'Speed_X' + traj2, 'Speed_Y' + traj2, 'Speed' + traj2, 'Longitude' + traj2, 'Latitude' + traj2, 'Elevation' + traj2, 'Point_Cnt' + traj2, 'Dir_X_Bbox' + traj2, 
#                     'Dir_Y_Bbox' + traj2, 'Height' + traj2, 'Length' + traj2, 'Width' + traj2, 'Area' + traj2, 'Dis' + traj2, 'Max_Length' + traj2, 'Class' + traj2, 'MoveAngle' + traj2, 'DateTime' + traj2, 'AdjSpeed' + traj2, 'MedianSpeed' + traj2, 'Acceleration' + traj2, 
#                     'ObjectID' + traj1, 'FrameIndex' + traj1, 'Coord_X' + traj1, 'Coord_Y' + traj1, 'Coord_Z' + traj1, 'Speed_X' + traj1, 'Speed_Y' + traj1, 'Speed' + traj1, 'Longitude' + traj1, 'Latitude' + traj1, 'Elevation' + traj1, 'Point_Cnt' + traj1, 'Dir_X_Bbox' + traj1, 
#                     'Dir_Y_Bbox' + traj1, 'Height' + traj1, 'Length' + traj1, 'Width' + traj1, 'Area' + traj1, 'Dis' + traj1, 'Max_Length' + traj1, 'Class' + traj1, 'MoveAngle' + traj1, 'DateTime' + traj1, 'AdjSpeed' + traj1, 'MedianSpeed' + traj1, 'Acceleration' + traj1, 
#                     'timeDiff', 'directionDiff', 'distanceRange', 'ObjectIDs', 'Conflict_Type', 'trajAngleDifference', 'Conflict_Angle','Conflict_Movement','Conflict_Location'
# ]

# df_thru_cf.columns = ['OID_', 'Field1', 'ObjectID' + traj1, 'FrameIndex' + traj1, 'Coord_X' + traj1, 'Coord_Y' + traj1, 'Coord_Z' + traj1, 'Speed_X' + traj1, 'Speed_Y' + traj1, 'Speed' + traj1, 'Longitude' + traj1, 'Latitude' + traj1, 'Elevation' + traj1, 'Point_Cnt' + traj1, 'Dir_X_Bbox' + traj1, 
#                     'Dir_Y_Bbox' + traj1, 'Height' + traj1, 'Length' + traj1, 'Width' + traj1, 'Area' + traj1, 'Dis' + traj1, 'Max_Length' + traj1, 'Class' + traj1, 'MoveAngle' + traj1, 'DateTime' + traj1, 'AdjSpeed' + traj1, 'MedianSpeed' + traj1, 'Acceleration' + traj1, 
#                     'ObjectID' + traj2, 'FrameIndex' + traj2, 'Coord_X' + traj2, 'Coord_Y' + traj2, 'Coord_Z' + traj2, 'Speed_X' + traj2, 'Speed_Y' + traj2, 'Speed' + traj2, 'Longitude' + traj2, 'Latitude' + traj2, 'Elevation' + traj2, 'Point_Cnt' + traj2, 'Dir_X_Bbox' + traj2, 
#                     'Dir_Y_Bbox' + traj2, 'Height' + traj2, 'Length' + traj2, 'Width' + traj2, 'Area' + traj2, 'Dis' + traj2, 'Max_Length' + traj2, 'Class' + traj2,'MoveAngle' + traj2, 'DateTime' + traj2, 'AdjSpeed' + traj2, 'MedianSpeed' + traj2, 'Acceleration' + traj2, 
#                     'timeDiff', 'directionDiff', 'distanceRange', 'ObjectIDs', 'Conflict_Type', 'trajAngleDifference', 'Conflict_Angle','Conflict_Movement','Conflict_Location'
# ]
# '''