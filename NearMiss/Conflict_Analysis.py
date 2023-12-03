import matplotlib.pyplot as plt
import pandas as pd
import numpy
import glob
import ntpath
import csv
import datetime
import fnmatch
import os
import math
import arcpy
#import UNRLiDARNoGISLib as UNRLib


##############################################   Data Cleaning   ################################################################


#df = pd.read_csv(r'Z:\Virginia_SMP\Hills\Trajectories\conflict\conflictSum_Integration_2.csv')
df = pd.read_csv(r'Z:\RTC_Multi-Modal\Virginia_Neil\Trajectories\conflict\conflictSum_Integration_2.csv')
#df_out = pd.concat(listed_df)
df_out  = df


df_out['timeDiff'] = df_out['timeDiff'].abs() / 10


lon1=numpy.radians(df_out["Longitude"])
lat1=numpy.radians(df_out["Latitude"])
lon2=numpy.radians(df_out["Longitude_cf"])
lat2=numpy.radians(df_out["Latitude_cf"])
R = 6371000
    #radius of the Earth meters, 6371 kim    
    #change in coordinates
dlon = lon2 - lon1
dlat = lat2 - lat1
    #Haversine formula
a = numpy.sin(dlat/2)**2 + numpy.cos(lat1) * numpy.cos(lat2) * numpy.sin(dlon / 2)**2     
c = 2 * numpy.arctan2(numpy.sqrt(a), numpy.sqrt(1 - a))
df_out['Distance'] = R*c* 3.28084


df_out['ObjectIDs'] = list(zip(df_out.ObjectID, df_out.ObjectID_cf))
df_out['ObjectIDs'] = [tuple(sorted(x)) for x in df_out['ObjectIDs']]

df_group = df_out.groupby('ObjectIDs',as_index=False)['Distance'].min()
df_merge = pd.merge(df_group,df_out,on = ['ObjectIDs','Distance'],how='left')
df_out = df_merge.drop_duplicates(['ObjectIDs'])

#df_group = df_out.groupby('ObjectIDs',as_index=False)['timeDiff'].min()
#df_merge = pd.merge(df_group,df_out,on = ['ObjectIDs','timeDiff'],how='left')
#df_out = df_merge.drop_duplicates(['ObjectIDs'])

#df.sort_values(['ObjectIDs','Distance'],ascending=True)
#df_out.drop_duplicates(subset = 'ObjectIDs',keep='first',inplace=True)

#df_out.sort_values('Distance', ascending=True).drop_duplicates('ObjectIDs').sort_index()

df_out['DateTime'] = pd.to_datetime(df_out['DateTime'])
df_out['DateTime_cf'] = pd.to_datetime(df_out['DateTime_cf'])

df_out['Height'] = df_out['Height']*3.28084
df_out['Height_cf'] = df_out['Height_cf']*3.28084
df_out['Width'] = df_out['Width']*3.28084
df_out['Width_cf'] = df_out['Width_cf']*3.28084
df_out['Length'] = df_out['Length']*3.28084
df_out['Length_cf'] = df_out['Length_cf']*3.28084
df_out['Max_Length'] = df_out['Max_Length']*3.28084
df_out['Max_Length_cf'] = df_out['Max_Length_cf']*3.28084
df_out['Area'] = df_out['Area']*10.764
df_out['Area_cf'] = df_out['Area_cf']*10.764

'''
conditions = [(df_out['Class'] == 1) & (df_out['Class_cf'] == 1), 
            (df_out['Class'] == 2) & (df_out['Class_cf'] == 2),
            (df_out['Class'] == 3) & (df_out['Class_cf'] == 3),
            ((df_out['Class'] == 1) & (df_out['Class_cf'] == 2)) | ((df_out['Class'] == 2) & (df_out['Class_cf'] == 1)),
            ((df_out['Class'] == 1) & (df_out['Class_cf'] == 3)) | ((df_out['Class'] == 3) & (df_out['Class_cf'] == 1)),
            ((df_out['Class'] == 2) & (df_out['Class_cf'] == 3)) | ((df_out['Class'] == 3) & (df_out['Class_cf'] == 2))]
values = ['V2V','P2P','B2B','V2P','V2B','B2P']
df_out['Conflict_Type'] = np.select(conditions,values)
'''

conditions = [(df_out['Class'] == 1) & (df_out['Class_cf'] == 1), 
            (df_out['Class'] == 2) & (df_out['Class_cf'] == 2),
            (df_out['Class'] == 3) & (df_out['Class_cf'] == 3),
            (df_out['Class'] == 4) & (df_out['Class_cf'] == 4),
            (df_out['Class'] == 5) & (df_out['Class_cf'] == 5),
            ((df_out['Class'] == 1) & (df_out['Class_cf'] == 2)) | ((df_out['Class'] == 2) & (df_out['Class_cf'] == 1)),
            ((df_out['Class'] == 1) & (df_out['Class_cf'] == 3)) | ((df_out['Class'] == 3) & (df_out['Class_cf'] == 1)),
            ((df_out['Class'] == 2) & (df_out['Class_cf'] == 3)) | ((df_out['Class'] == 3) & (df_out['Class_cf'] == 2)),
            ((df_out['Class'] == 1) & (df_out['Class_cf'] == 4)) | ((df_out['Class'] == 4) & (df_out['Class_cf'] == 1)),
            ((df_out['Class'] == 2) & (df_out['Class_cf'] == 4)) | ((df_out['Class'] == 4) & (df_out['Class_cf'] == 2)),
            ((df_out['Class'] == 4) & (df_out['Class_cf'] == 3)) | ((df_out['Class'] == 3) & (df_out['Class_cf'] == 4)),
            ((df_out['Class'] == 1) & (df_out['Class_cf'] == 5)) | ((df_out['Class'] == 5) & (df_out['Class_cf'] == 1)),
            ((df_out['Class'] == 2) & (df_out['Class_cf'] == 5)) | ((df_out['Class'] == 5) & (df_out['Class_cf'] == 2)),
            ((df_out['Class'] == 3) & (df_out['Class_cf'] == 5)) | ((df_out['Class'] == 5) & (df_out['Class_cf'] == 3)),
            ((df_out['Class'] == 4) & (df_out['Class_cf'] == 5)) | ((df_out['Class'] == 5) & (df_out['Class_cf'] == 4))]
values = ['V2V','P2P','B2B','S2S','M2M','V2P','V2B','B2P','V2S','S2P','B2S','V2M','M2P','M2B','M2S']
df_out['Conflict_Type'] = numpy.select(conditions,values)


df_out['trajAngleDifference'] = df_out['directionDiff'].abs()
conditions = [(df_out['trajAngleDifference'] >180), (df_out['trajAngleDifference'] <=180)]
values = [360 - df_out['trajAngleDifference'], df_out['trajAngleDifference']]
df_out['Conflict_Angle'] = numpy.select(conditions,values)


#df_out.to_csv(r'Z:\Virginia_SMP\Hills\Analysis\Conflicts\Hills_Conflicts.csv')
#df_out.to_csv(r'Z:\Virginia_SMP\Sierra_Manor\Analysis\Conflicts\Sierra_Manor_Conflicts.csv')
df_out.to_csv(r'Z:\RTC_Multi-Modal\Virginia_Neil\Analysis\Conflicts\Virginia_Neil_Conflicts.csv')


#arcpy.env.workspace = r"Z:\Virginia_SMP\Hills\Hills.gdb"
#arcpy.env.workspace = r"Z:\Virginia_SMP\Sierra_Manor\Sierra_Manor.gdb"
arcpy.env.workspace = r"Z:\RTC_Multi-Modal\Virginia_Neil\Virginia_Neil.gdb"


arcpy.management.XYTableToPoint(r'Z:\RTC_Multi-Modal\Virginia_Neil\Analysis\Conflicts\Virginia_Neil_Conflicts.csv',"Virginia_Neil_Conflicts","Longitude","Latitude")



##############################################   Data Summary   ################################################################
'''
df = pd.read_csv('Z:\Midtown_Study\Sites\Thoma\Analysis\Virginia_Crossing.csv')
#df_sliced = df.loc[(df['trajTimeDifference']<=2) & (df['trajDistance']<3) & (df['Conflict_Type'] == 'V2V')]
df['DateTime'] = pd.to_datetime(df['DateTime'])
df['DateTime_cf'] = pd.to_datetime(df['DateTime_cf'])

site = 'Thoma_Virginia_Crossing'

plt.hist(df['timeDiff']/10,label = 'Time Difference (sec)',color='#041E42')
plt.title('Distribution of Crossing Conflict Severity')
plt.xlabel("Time Difference (sec)")
plt.ylabel('Count of Conflicts')
my_file = site + '_TimeDiff.png'
plt.savefig(my_file)
plt.clf()

plt.plot_date(df['DateTime'],df['timeDiff']/10,marker = 'o', linestyle='',color='#041E42')
plt.title('Crossing Conflict Severity')
plt.xlabel("Date and Time")
plt.ylabel('Time Difference (sec)')
plt.xticks(rotation=30)
my_file = site + '_TimeDiff_DateTime.png'
plt.savefig(my_file)
plt.clf()

plt.plot(df['MedianSpeed'],df['MedianSpeed_cf'],marker = 'o', linestyle='',color='#041E42')
plt.title('Conflict Speeds')
plt.xlabel("Road User 1 Speed (MPH)")
plt.ylabel('Road User 2 Speed (MPH)')
my_file = site + '_Speeds.png'
plt.savefig(my_file)
plt.clf()

plt.plot(df['Acceleration'],df['Acceleration_cf'],marker = 'o', linestyle='', color='#041E42')
plt.title('Conflict Acceleration')
plt.xlabel("Road User 1 Acceleration (ft/s^2)")
plt.ylabel('Road User 2 Acceleration (ft/s^2)')
my_file = site + '_Acceleration.png'
plt.savefig(my_file)
plt.clf()

plt.plot(df['MoveAngle'],df['MoveAngle_cf'],marker = 'o', linestyle='',color='#041E42')
plt.title('Conflict Directions')
plt.xlabel("Road User 1 Direction (Degrees)")
plt.ylabel('Road User 2 Direction (Degrees)')
my_file = site + 'Direction.png'
plt.savefig(my_file)
plt.clf()

plt.hist(df['Conflict_Angle'],color='#041E42')
plt.title('Distribution of Conflict Angle')
plt.xlabel("Conflict Angle")
plt.ylabel('Count of Conflicts')
my_file = site + '_Angle.png'
plt.savefig(my_file)
plt.clf()

plt.plot(df['Max_Length']*3.28,df['Max_Length_cf']*3.28,marker = 'o', linestyle='',color='041E42') 
plt.title('Conflict Vehicle Size')
plt.xlabel("Road User 1 Length (FT)")
plt.ylabel('Road User 2 Length (FT)')
my_file = site + '_Size.png'
plt.savefig(my_file)
plt.clf()

'''







'''
plt.hist([df['trajTimeDifference'], df['trajDistance']],label = ['Time Difference (sec)', 'Distance (m)'],stacked=True)
plt.legend()
plt.title('Distribution of Crossing Conflict Severity')
plt.xlabel("Time Difference (sec) or Distance (m)")
plt.ylabel('Count of Conflicts')
plt.show()

plt.clf()
plt.scatter(df['trajTimeDifference'],df['trajDistance'])
plt.title('Scatterplot of Crossing Conflict Severity')
plt.xlabel("Time Difference (sec)")
plt.ylabel('Distance (m)')
plt.show()

plt.clf()
groups = df_sliced.groupby('Conflict_Type')
for name, group in groups:
    plt.plot(group['trajTimeDifference'],group['trajDistance'],marker = 'o',label = name,linestyle='')
plt.legend()
plt.xlabel("Time Difference (sec)")
plt.ylabel('Distance (m)')
plt.show()

plt.clf()
groups = df.groupby('Conflict_Type')
for name, group in groups:
    plt.plot(group['MedianSpeed'],group['MedianSpeed_cf'],marker = 'o',label = name, linestyle='') 
plt.legend()
plt.title('Conflict Speeds')
plt.xlabel("Road User 1 Speed (MPH)")
plt.ylabel('Road User 2 Speed (MPH)')
plt.show()

plt.clf()
groups = df.groupby('Conflict_Type')
for name, group in groups:
    plt.plot(group['MedianSpeed'],group['MedianSpeed_cf'],marker = 'o',label = name, linestyle='') 
plt.legend()
plt.title('Conflict Speeds')
plt.xlabel("Road User 1 Speed (MPH)")
plt.ylabel('Road User 2 Speed (MPH)')
plt.show()

plt.clf()
plt.hist(df['trajTimeDifference'].loc[df['trajTimeDifference'] <=1])
plt.title('Distribution of Crossing Distance Below 1 Second')
plt.xlabel("Distance (m)")
plt.ylabel('Count of Conflicts')
plt.show()

plt.clf()
plt.hist(df['trajTimeDifference'].loc[(df['trajTimeDifference'] > 1) & (df['trajTimeDifference'] <= 2)])
plt.title('Distribution of Crossing Distance Between 1 and 2 Seconds')
plt.xlabel("Distance (m)")
plt.ylabel('Count of Conflicts')
plt.show()

plt.clf()
plt.hist(df['trajTimeDifference'].loc[(df['trajTimeDifference'] > 2) & (df['trajTimeDifference'] <= 3)])
plt.title('Distribution of Crossing Distance Between 2 and 3 Seconds')
plt.xlabel("Distance (m)")
plt.ylabel('Count of Conflicts')
plt.show()

plt.clf()
plt.hist(df['trajTimeDifference'].loc[(df['trajTimeDifference'] > 3) & (df['trajTimeDifference'] <= 4)])
plt.title('Distribution of Crossing Distance Between 3 and 4 Seconds')
plt.xlabel("Distance (m)")
plt.ylabel('Count of Conflicts')
plt.show()

plt.clf()
plt.hist(df['trajTimeDifference'].loc[(df['trajTimeDifference'] > 4) & (df['trajTimeDifference'] <= 5)])
plt.title('Distribution of Crossing Distance Between 4 and 5 Seconds')
plt.xlabel("Distance (m)")
plt.ylabel('Count of Conflicts')
plt.show()


plt.clf()
plt.hist(df['Conflict_Angle'])
plt.title('Distribution of Conflict Angle')
plt.xlabel("Conflict Angle")
plt.ylabel('Count of Conflicts')
plt.show()

plt.clf()
plt.hist(df_sliced['Conflict_Angle'])
plt.title('Distribution of Conflict Angle')
plt.xlabel("Conflict Angle")
plt.ylabel('Count of Conflicts')
plt.show()


conflict_type = ['Vehicle-Vehicle','Vehicle-Pedestrian','Vehicle-Bicycle','Bicycle-Pedestrian']
type_count = [len(df[(df['Conflict_Type'] == 'V2V') & (df['trajDistance'] <= 2) & (df['trajTimeDifference'] <= 2)]), 
            len(df[(df['Conflict_Type'] == 'V2P') & (df['trajDistance'] <= 2) & (df['trajTimeDifference'] <= 2)]), 
            len(df[(df['Conflict_Type'] == 'V2B') & (df['trajDistance'] <= 2) & (df['trajTimeDifference'] <= 2)]), 
            len(df[(df['Conflict_Type'] == 'B2P') & (df['trajDistance'] <= 2) & (df['trajTimeDifference'] <= 2)])]
plt.clf()
#plt.bar(conflict_type,type_count)
#plt.yticks([1,25,50,75,100,125,150,175,200,225,250])
plt.pie(type_count,autopct='%1.1f%%',pctdistance=1.2,rotatelabels=True)
plt.legend(conflict_type)
plt.title('Breakdown of Conflicts Less Than 2s PET and 2m Distance')
plt.axis('equal')
plt.show()

type_count = [len(df[(df['Class'] == 1) & (df['Class_cf'] == 1)]), len(df[((df['Class'] == 1) & ((df['Class_cf'] == 2)) | (df['Class'] == 2) & (df['Class_cf'] == 1))]), len(df[((df['Class'] == 1) & ((df['Class_cf'] == 3)) | (df['Class'] == 3) & (df['Class_cf'] == 1))]), len(df[((df['Class'] == 2) & ((df['Class_cf'] ==3)) | (df['Class'] == 3) & (df['Class_cf'] == 2))])]
plt.clf()
#plt.bar(conflict_type,type_count)
#plt.yticks([1,25,50,75,100,125,150,175,200,225,250])
plt.pie(type_count,autopct='%1.1f%%',pctdistance=1.2,rotatelabels=True)
plt.legend(conflict_type)
plt.title('Breakdown of All Conflicts')
plt.axis('equal')
plt.show()

type_count = [len(df_sliced[(df_sliced['Class'] == 1) & (df_sliced['Class_cf'] == 1)]), len(df_sliced[((df_sliced['Class'] == 1) & ((df_sliced['Class_cf'] == 2)) | (df_sliced['Class'] == 2) & (df_sliced['Class_cf'] == 1))]), len(df_sliced[((df_sliced['Class'] == 1) & ((df_sliced['Class_cf'] == 3)) | (df_sliced['Class'] == 3) & (df_sliced['Class_cf'] == 1))]), len(df_sliced[((df_sliced['Class'] == 2) & ((df_sliced['Class_cf'] ==3)) | (df_sliced['Class'] == 3) & (df_sliced['Class_cf'] == 2))])]
plt.clf()
#plt.bar(conflict_type,type_count)
#plt.yticks([1,25,50,75,100,125,150,175,200,225,250])
plt.pie(type_count,autopct='%1.1f%%',pctdistance=1.2,rotatelabels=True)
plt.legend(conflict_type)
plt.title('Breakdown of All Conflicts')
plt.axis('equal')
plt.show()


plt.clf()
plt.scatter(df['trajTimeDifference'],df['Conflict_Angle'])
plt.ylim([0,360])
plt.show()


groups = df.groupby('Conflict_Type')
for name, group in groups:
    plt.plot(group['trajTimeDifference'],group['Conflict_Angle'],marker = 'o',label = name, linestyle='')
plt.legend()
plt.show()
 
plt.clf()
plt.hist(df['Conflict_Angle'].loc[df['Conflict_Type'] == 'V2P'])
plt.show()

plt.clf()
plt.scatter(df['DateTime'],df['trajTimeDifference'])
plt.show()
'''