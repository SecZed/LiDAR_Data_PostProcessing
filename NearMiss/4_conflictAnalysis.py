from tkinter import Frame
import pandas
import numpy
import datetime
import multiprocessing
from joblib import Parallel, delayed
import glob
import os
import UNRLiDARGISLib as UNRGISLib
import UNRLiDARNoGISLib as UNRLib
from math import sqrt
from os.path import exists
import time
import arcpy


# rootFolder=r"D:\HDR_LA"
# trajSubFolderName="Trajectories"
# #"Int",
# #"Roundabout_center","Arroyo","California1","Chenry","LARUE","Lawrence","Liberty","Martin","Mtrose"
# #"Chenry""Moran","PlumbNorth",,"PlumbNorth","Pueblo","Regency","Roundabout_NW","Steward","Taylor","Thoma","Vassr","Wells"
# trajFolderNameArray=["Charleston","Commerce","Meadows Valley View"]


rootFolder=r"D:\Kirkwood"
#"Arlington_5th","Keystone_5th","Ralston_5th","Virginia_2nd","Virginia_4th","Virginia_5th","Virginia_Commercial",
trajFolderNameArray=["Kirkwood"]
trajSubFolderName="Trajectories"

rootFolder=r"Z:\Data\CATERITS\LiDAR\Raw Data\MicroMobility"
trajFolderNameArray=["MillSTandVirginia","TruckeeWalkandVirginia","VirginiaAndCommercial","VirginiaAnd2nd","VIRGINIAAND4TH","VIRGINIAAND5TH","5thStandRalston","5thSTArlington","5thSTKeyStone"]
trajSubFolderName="Trajectories"

rootFolder=r"C:\ceehao\HDR_LA_SEC"
siteNameArray=["Alta","Main","North","SaharaCimarron","SCharleston","South","Valley View Blvd and Meadows Ln"]
subTrajectoryFolder="Trajectories"


# rootFolder=r"D:\ASWS\Pyramid_Sparks\Round3"
# trajSubFolderName="ProcessedTrajs"
# trajFolderNameArray=["int","north","south"]
#"Liberty",
#trajFolderNameArray=["Moran"]
rootFolder=r"Z:\Data\CATERITS\LiDAR\Raw Data\MicroMobility\Round2"
trajFolderNameArray=["2ndST","4th and Virginia","5th and Virginia"]
trajSubFolderName="Trajectories"

rootFolder=r"D:\Micromobility3"
trajFolderNameArray=["TrukeeRiver"]
#"2th","4th","5th","Arlington","CommercialVirginia",#"Keystone","Ralston"
trajSubFolderName="Trajectories"

rootFolder=r"D:\Micromobility2"
trajFolderNameArray=["5thSTArlington"]
trajSubFolderName="Trajectories"

rootFolder=r"D:\Micromobility3"
trajFolderNameArray=["Keystone"]
trajSubFolderName="Trajectories"

rootFolder=r"D:\Micromobility2"
trajFolderNameArray=["5thSTArlington28th"]
trajSubFolderName="Trajectories"

rootFolder=r"D:\Micromobility3"
trajFolderNameArray=["4th"]
trajSubFolderName="Trajectories"

rootFolder=r"D:\Micromobility2"
trajFolderNameArray=["VIRGINIAAND5TH"]
#siteNameArray=["VIRGINIAAND4TH"]
trajSubFolderName="Trajectories"

rootFolder=r"D:\Micromobility1"
trajFolderNameArray=["VirginiaAndCommercial"]
trajSubFolderName="Trajectories"

rootFolder=r"D:\Micromobility2"
trajFolderNameArray=["VirginiaAndCommercial"]
trajSubFolderName="Trajectories"

rootFolder=r"C:\Users\trevorwhitley\Documents\Virginia_SMP"
trajFolderNameArray=["North_NB_US395_Ramp","North_SB_US395_Ramp","Patriot"]
subTrajectoryFolder="Trajectories"

rootFolder=r"C:\Users\trevorwhitley\Documents\RTC_Multi-Modal"
trajFolderNameArray=["Barring_Goldy","Wells_6th"]
subTrajectoryFolder="Trajectories"


rootFolder=r"C:\Users\trevorwhitley\Documents\Rail"
trajFolderNameArray=["West"]

rootFolder=r"C:\Users\trevorwhitley\Documents\Silver_Lake_Railroad"
trajFolderNameArray=["East"]


rootFolder=r"C:\Users\trevorwhitley\Documents\RTC_Multi-Modal"
trajFolderNameArray=["Virginia_Neil","Virginia_Loneley","Sierra_Highlands_7th","McCarren_7th","Booth_Riverside"]
subTrajectoryFolder="Trajectories"

overwriteOutputFiles=True # or True
trajDataType='New'#'New' #or 'Old'
num_cores=20
boolParallel=True #or False
conflictCellSize=2 #meter

#define column names, not for operator's input

ObjectIDField='ObjectID'
FrameIndexField='FrameIndex'
LongitudeField='Longitude'
LatitudeField='Latitude'
LengthField='Length'
WidthField='Width'
HeightField='Height'
TypeField='Class'
CoordXField='Coord_X'
CoordYField='Coord_Y'
DirectionField='MoveAngle'
SpeedField='MedianSpeed'

if trajDataType=='Old':
    ObjectIDField='ObjectID'
    FrameIndexField='FrameIndex'
    LongitudeField='Longitude'
    LatitudeField='Latitude'
    LengthField='Object_Length'
    WidthField='Object_Width'
    HeightField='Object_Height'
    TypeField='ObjectType'
    CoordXField='Coord_x'
    CoordYField='Coord_y'
    DirectionField='MoveAngle'
    SpeedField='MedianSpeed'




def trajSummaryNoGIS(file="", minTrajLen=10, chunkSize=100000):
    #outputFile=file[:-4]+"_add.csv"
    outputSummaryFile=file[:-4]+"_summary.csv"
    if not overwriteOutputFiles and exists(outputSummaryFile):
            return
    reader=pandas.read_csv(file,chunksize=chunkSize)
    chunkCount=0
    for trajDF in reader:
        chunkCount=chunkCount+1
        trajDF=pandas.read_csv(file)
        objectIDList=trajDF.ObjectID.unique()
        newColumns=["Count","FirstFrame","LastFrame","MinType","MaxType","MeanType","MinLenth","MaxLength","MeanLength","MinWidth","MaxWidth","MeanWidth","FirstLon","FirstLat","LastLon","LastLat","TrajDis","TrajDir","TrajSpeed","DisIntersection","DateTime","StartDir"]
        #trajDF.reindex(columns=[*trajDF.columns.tolist(), *newColumns], fill_value=0)
        trajDF=pandas.concat([trajDF,pandas.DataFrame(columns=newColumns)],axis=1)

        #calculate base time based on file name
        dateAndTimeValues=UNRLib.extractDateAndTimefromFileName(file,"-")
        yearStr=dateAndTimeValues[0]
        monthStr=dateAndTimeValues[1]
        dayStr=dateAndTimeValues[2]
        hourStr=dateAndTimeValues[3]
        minuteStr=dateAndTimeValues[4]
        secondStr=dateAndTimeValues[5]
        baseDateTime=datetime.datetime(yearStr,monthStr,dayStr,hourStr,minuteStr,secondStr)
        
        for objectID in objectIDList:
            trajPoints=trajDF.loc[trajDF[ObjectIDField]==objectID]
            trajPoints=trajPoints.sort_values(by=[FrameIndexField])
            trajPointsLen=len(trajPoints.index)
            if trajPointsLen<minTrajLen:
                continue
            #summary of jobject type
            typeList=trajPoints[TypeField]
            typeValues=typeList
            minType=min(typeValues)
            maxType=max(typeValues)
            meanType=numpy.mean(typeValues)

            #summary of object length
            lengthList=trajPoints[LengthField]
            lengthValues=lengthList
            minLength=min(lengthValues)
            maxLength=max(lengthValues)
            meanLength=numpy.mean(lengthValues)

            #summary of object width
            widthList=trajPoints[WidthField]
            widthValues=widthList
            minWidth=min(widthValues)
            maxWidth=max(widthValues)
            meanWidth=numpy.mean(widthValues)

            #summary of longitude
            lonList=trajPoints[LongitudeField].astype(float)
            lonValues=lonList
            #print lonValues
            firstLon=lonValues.values[0]
            lastLon=lonValues.values[trajPointsLen-1]

            #summary of latitude
            latList=trajPoints[LatitudeField].astype(float)
            latValues=latList
            firstLat=latValues.values[0]
            lastLat=latValues.values[trajPointsLen-1]

            #summary of frameindex
            frameList=trajPoints[FrameIndexField]
            frameValues=frameList
            firstFrame=frameValues.values[0]
            if numpy.issubdtype(firstFrame, numpy.int64):
                firstFrame=firstFrame.item()
            lastFrame=frameValues.values[trajPointsLen-1]
            if numpy.issubdtype(lastFrame, numpy.int64):
                lastFrame=lastFrame.item()
            frameDiff=frameValues.values[trajPointsLen-1]-frameValues.values[0]
            timeDiffSec=frameDiff/10

            #start of trajectory's direction related to the intersection center
            # StartDirection=calculateGPSDirection(firstLon, firstLat,intersectionLon,intersectionLat)

            #calculate direction and speed of trajectory
            trajDirection=UNRLib.calculateGPSDirection(firstLon,firstLat,lastLon,lastLat)
            trajDistance=UNRLib.calculateGPSDistance(firstLon,firstLat,lastLon,lastLat)
            if trajDistance<10:
                trajDF.drop(trajDF[trajDF[ObjectIDField]==objectID].index, inplace = True) 
                continue
            trajSpeed=trajDistance/timeDiffSec #meter per second
            trajSpeed=trajSpeed*2.23694 #mile per hour

            #calculate time
            dateTime=baseDateTime+datetime.timedelta(seconds=firstFrame/10)





            trajDF.loc[trajDF[ObjectIDField]==objectID,["Count","FirstFrame","LastFrame","MinType","MaxType","MeanType","MinLenth","MaxLength","MeanLength","MinWidth","MaxWidth","MeanWidth","FirstLon","FirstLat","LastLon","LastLat","TrajDis","TrajDir","TrajSpeed","DisIntersection","DateTime","StartDir"]]=[trajPointsLen,firstFrame,lastFrame,minType,maxType,meanType,minLength,maxLength,meanLength,minWidth,maxWidth,meanWidth,firstLon,firstLat,lastLon,lastLat,trajDistance,trajDirection,trajSpeed,-1,dateTime,-1]
            #trajDF.loc[trajDF.ObjectID==objectID,["Count","FirstFrame","LastFrame","MinType","MaxType","MeanType","MinLenth","MaxLength","MeanLength","MinWidth","MaxWidth","MeanWidth","FirstLon","FirstLat","LastLon","LastLat","TrajDis","TrajDir","TrajSpeed","DisIntersection","DateTime","StartDir"]]=[trajPointsLen,firstFrame,lastFrame,minType,maxType,meanType,minLength,maxLength,meanLength,minWidth,maxWidth,meanWidth,firstLon,firstLat,lastLon,lastLat,trajDistance,trajDirection,trajSpeed,-1,dateTime,-1]

        # for i, row in trajDF.iterrows():
        #     lon=trajDF.at[i,LongitudeField]
        #     lat=trajDF.at[i,LatitudeField]
        #     frameIndex=trajDF.at[i,FrameIndexField]        
        #     disIntersection=calculateGPSDistance(lon,lat,intersectionLon,intersectionLat)
        #     if lat>intersectionLat:
        #         disIntersection=-disIntersection
        #     elif lat==intersectionLat and lon>intersectionLon:
        #         disIntersection=-disIntersection
        #     trajDF.at[i,"DisIntersection"]=disIntersection
        #     dateTime=baseDateTime+datetime.timedelta(seconds=frameIndex.item()/10)
        #     trajDF.at[i,"DateTime"]=dateTime
        trajDF=trajDF.drop_duplicates(subset = [ObjectIDField])
        if chunkCount==1: 
            trajDF.to_csv(outputSummaryFile,index = False, header=True)
        elif chunkCount>1:
            trajDF.to_csv(outputSummaryFile,mode='a',index = False, header=False)

#Taking the new cleaned trajects with speeds, acceleration, datatime and direction....
def newTrajSummaryNoGIS(file,outputSummaryFile, minTrajLen=10, chunkSize=100000):
    #outputFile=file[:-4]+"_add.csv"
    #outputSummaryFile=file[:-4]+"_summary.csv"
    if not overwriteOutputFiles and exists(outputSummaryFile):
            return
    reader=pandas.read_csv(file,chunksize=chunkSize)
    combinedTrajDF=pandas.DataFrame()
      
    for trajDF in reader:
        #trajDF=pandas.read_csv(file)
        objectIDList=trajDF.ObjectID.unique()
        newColumns=["Count","FirstFrame","LastFrame","TrajDis","TrajDir"]
        #trajDF.reindex(columns=[*trajDF.columns.tolist(), *newColumns], fill_value=0)
        #trajDF=pandas.concat([trajDF,pandas.DataFrame(columns=newColumns)],axis=1)
              
        for objectID in objectIDList:
            trajPoints=trajDF.loc[trajDF[ObjectIDField]==objectID]
            #print(trajPoints)
            trajPoints=trajPoints.sort_values(by=[FrameIndexField])
            trajPointsLen=len(trajPoints.index)
            if trajPointsLen<minTrajLen:
                continue
            
            
            #longitude and latitude
            firstLon=trajPoints[LongitudeField].iat[0]
            firstLat=trajPoints[LatitudeField].iat[0]
            lastLon=trajPoints[LongitudeField].iat[-1]
            lastLat=trajPoints[LatitudeField].iat[-1]            

            #summary of frameindex
            firstFrame=trajPoints[FrameIndexField].iat[0]
            lastFrame=trajPoints[FrameIndexField].iat[-1]             

            #start of trajectory's direction related to the intersection center
            # StartDirection=calculateGPSDirection(firstLon, firstLat,intersectionLon,intersectionLat)

            #calculate direction and speed of trajectory
            trajDirection=UNRLib.calculateGPSDirection(firstLon,firstLat,lastLon,lastLat)
            trajDistance=UNRLib.calculateGPSDistance(firstLon,firstLat,lastLon,lastLat)
            if trajDistance<5:
                continue
            #print(trajDF.loc[0,:])

            # print(pandas.DataFrame(columns=newColumns))
            tempOneRecordDF=trajPoints.head(1)
            tempOneRecordDF=pandas.concat([tempOneRecordDF,pandas.DataFrame(columns=newColumns)],axis=1)
            tempOneRecordDF.loc[:,newColumns]=[trajPointsLen,firstFrame,lastFrame,trajDistance,trajDirection]
            #print(tempOneRecordDF)

            if combinedTrajDF.empty:
                combinedTrajDF=tempOneRecordDF
            else:
                combinedTrajDF=combinedTrajDF.append(tempOneRecordDF)            
                       
    if not combinedTrajDF.empty:
        combinedTrajDF.to_csv(outputSummaryFile,index = False, header=True)
        


def conflictIdentification(trajFile="",summaryFile="",timePeriod=18000,step=100,minTrajLen=5,dirDiffThreshold=0,distanceThreshold=5,timeThreshold=1,smoothInterval=5):
    print("conflict analysis")
    print(trajFile)
    print(summaryFile)
    conflictTrajFile=trajFile[:-4]+"_conflict_traj.csv"
    conflictSummaryFile=summaryFile[:-4]+"_conflict.csv"
    if not overwriteOutputFiles and exists(conflictSummaryFile):
        return
    trajDF=pandas.read_csv(trajFile)
    summaryDF=pandas.read_csv(summaryFile)
    conflictTrajDF=pandas.DataFrame(columns=trajDF.columns)
    conflictSummaryDF=pandas.DataFrame(columns=summaryDF.columns)
    conflictSummaryDF.columns = [str(col) + '_cf' for col in conflictSummaryDF.columns]
    conflictSummaryDF=pandas.concat([pandas.DataFrame(columns=summaryDF.columns),conflictSummaryDF],axis=1)
    conflictSummaryDF=pandas.concat([conflictSummaryDF,pandas.DataFrame(columns=["distance","timeDifference","averageAngle","averageDirection1","averageDirection2","conflictTraj1Lon","conflictTraj1Lat","conflictTraj2Lon","conflictTraj2Lat","realtimeSpeed1","reapltimeSpeed2","timeFrame1","timeFrame2"])],axis=1)

    for i in range(0,(timePeriod-step),step):
        objects=summaryDF.loc[(summaryDF['FirstFrame']>=i)&(summaryDF['FirstFrame']<i+step)]
        for r_index,row in objects.iterrows():
            if row["Count"]<minTrajLen:
                continue            

            for r_index_second,row_second in objects.iterrows():
                if row_second["ObjectID"]==row["ObjectID"]:
                    continue
                if row_second["Count"]<minTrajLen:
                    continue
                directionDiff=abs(row_second["TrajDir"]-row["TrajDir"])                
                if directionDiff<dirDiffThreshold or directionDiff>360-dirDiffThreshold: #15 degree direction difference
                    continue                
                trajObject1=trajDF.loc[trajDF[ObjectIDField]==row["ObjectID"]]
                trajObject2=trajDF.loc[trajDF[ObjectIDField]==row_second["ObjectID"]]
                trajPointsLen1=len(trajObject1.index)
                trajPointsLen2=len(trajObject2.index)
                for i_1 in range(smoothInterval,trajPointsLen1):
                    previous_i_1=i_1-smoothInterval
                    current_X_1=trajObject1.loc[trajObject1.index[i_1],CoordXField]
                    current_Y_1=trajObject1.loc[trajObject1.index[i_1],CoordYField]
                    current_Lon_1=trajObject1.loc[trajObject1.index[i_1],LongitudeField]
                    current_Lat_1=trajObject1.loc[trajObject1.index[i_1],LatitudeField]
                    current_frameIndex_1=trajObject1.loc[trajObject1.index[i_1],FrameIndexField]
                    
                    previous_X_1=trajObject1.loc[trajObject1.index[previous_i_1],CoordXField]
                    previous_Y_1=trajObject1.loc[trajObject1.index[previous_i_1],CoordYField]
                    previous_Lon_1=trajObject1.loc[trajObject1.index[previous_i_1],LongitudeField]
                    previous_Lat_1=trajObject1.loc[trajObject1.index[previous_i_1],LatitudeField]
                    previous_frameIndex_1=trajObject1.loc[trajObject1.index[previous_i_1],FrameIndexField]
                    previous_distance_1=sqrt((current_X_1 - previous_X_1)**2+(current_Y_1 - previous_Y_1)**2)
                    previous_timediff_1=(current_frameIndex_1-previous_frameIndex_1)*0.1
                    direction_1=UNRLib.calculateGPSDirection(previous_Lon_1,previous_Lat_1,current_Lon_1,current_Lat_1)
                    speed_1=previous_distance_1/previous_timediff_1*2.23694 #convert meter per sec to mph

                    for i_2 in range(smoothInterval,trajPointsLen2):
                        conflictFound=False
                        previous_i_2=i_2-smoothInterval
                        current_X_2=trajObject2.loc[trajObject2.index[i_2],CoordXField]
                        current_Y_2=trajObject2.loc[trajObject2.index[i_2],CoordYField]
                        current_Lon_2=trajObject2.loc[trajObject2.index[i_2],LongitudeField]
                        current_Lat_2=trajObject2.loc[trajObject2.index[i_2],LatitudeField]
                        current_frameIndex_2=trajObject2.loc[trajObject2.index[i_2],FrameIndexField]
                        
                        previous_X_2=trajObject2.loc[trajObject2.index[previous_i_2],CoordXField]
                        previous_Y_2=trajObject2.loc[trajObject2.index[previous_i_2],CoordYField]
                        previous_Lon_2=trajObject2.loc[trajObject2.index[previous_i_2],LongitudeField]
                        previous_Lat_2=trajObject2.loc[trajObject2.index[previous_i_2],LatitudeField]
                        previous_frameIndex_2=trajObject2.loc[trajObject2.index[previous_i_2],FrameIndexField]
                        previous_distance_2=sqrt((current_X_2 - previous_X_2)**2+(current_Y_2 - previous_Y_2)**2)
                        previous_timediff_2=(current_frameIndex_2-previous_frameIndex_2)*0.1
                        direction_2=UNRLib.calculateGPSDirection(previous_Lon_2,previous_Lat_2,current_Lon_2,current_Lat_2)
                        speed_2=previous_distance_2/previous_timediff_2*2.23694 #convert meter per sec to mph

                        objectDistance=sqrt((current_X_2 - current_X_1)**2+(current_Y_2 - current_Y_1)**2)
                        timeDifference=abs(current_frameIndex_1-current_frameIndex_2)/10  
                        point_directionDiff=abs(direction_1-direction_2)                        
                        if point_directionDiff<dirDiffThreshold or point_directionDiff>(360-dirDiffThreshold): #15 degree direction difference
                            continue 

                        if objectDistance<=distanceThreshold and timeDifference<=timeThreshold:                            
                            conflictTrajDF=conflictTrajDF.append(trajObject1)
                            #conflictTrajDF=conflictTrajDF.append(trajObject2)
                            # additionalArray=[objectDistance,timeDifference,directionDiff,row["TrajDir"],row_second["TrajDir"],objectDirectionDifference,direction1,direction2,speed1,speed2,frameIndex1,frameIndex2]
                            additionalArray=[objectDistance,timeDifference,directionDiff,direction_1,direction_2,current_Lon_1,current_Lat_1,current_Lon_2,current_Lat_2,speed_1,speed_2,current_frameIndex_1,current_frameIndex_2]
                            integrateObjects=numpy.concatenate((numpy.array(row), numpy.array(row_second)), axis=None)
                            integrateObjects=numpy.concatenate((integrateObjects, additionalArray), axis=None)   
                            tempSeries = pandas.Series(integrateObjects, index = conflictSummaryDF.columns)
                            conflictSummaryDF = conflictSummaryDF.append(tempSeries, ignore_index=True)                              
                            
                            #  conflictSummaryDF.append(dict(zip(conflictSummaryDF.columns, integrateObjects)), ignore_index=True)
                            conflictFound=True

                            break
                        else:
                            continue
                    if conflictFound:
                        break
                    
    if len(conflictTrajDF.index)>0:
        conflictTrajDF.to_csv(conflictTrajFile,index=False,header=True)
    #mapCSVtoFeature(csvFileBaseName="",outputWorkspace="",featureClassOutput="",xFieldName=LongitudeField,yFieldName=LatitudeField,  sr=arcpy.SpatialReference(4326)):
    if len(conflictSummaryDF.index)>0:  
        conflictSummaryDF.to_csv(conflictSummaryFile,index=False,header=True)

#taking the new cleaned tracks with speeds, direction and dates
def newConflictIdentification(trajFile="",summaryFile="",timePeriod=18000,step=100,minTrajLen=5,dirDiffThreshold=0,distanceThreshold=5,timeThreshold=1,smoothInterval=5,chunkSize=200000):
    print("conflict analysis")
    print(trajFile)
    print(summaryFile)
    fileFolder=os.path.dirname(trajFile)
    fileBaseName=os.path.basename(trajFile)
    newFileFolder=fileFolder+"/conflict"
    conflictTrajFile=newFileFolder+"/"+fileBaseName[:-4]+"_conflict_traj.csv"    
    conflictSummaryFile=summaryFile[:-4]+"_conflict.csv"
    if not overwriteOutputFiles and exists(conflictSummaryFile):
        return
    if (not exists(trajFile)) or (not exists(summaryFile)):
        return
    reader=pandas.read_csv(trajFile,chunksize=chunkSize)
    for trajDF in reader:
        #trajDF=pandas.read_csv(trajFile)
        summaryDF=pandas.read_csv(summaryFile)
        columnsArray=numpy.concatenate((trajDF.columns,[str(col) + '_cf' for col in trajDF.columns],["trajDistance","trajTimeDifference","trajAngleDifference"]),axis=None)
        conflictTrajDF=pandas.DataFrame(columns=trajDF.columns)
        conflictSummaryDF=pandas.DataFrame(columns=columnsArray)
        # conflictSummaryDF.columns = [str(col) + '_cf' for col in trajDF.columns]
        # conflictSummaryDF=pandas.concat([pandas.DataFrame(columns=trajDF.columns),conflictSummaryDF],axis=1)
        # conflictSummaryDF=pandas.concat([conflictSummaryDF,pandas.DataFrame(columns=["distance","timeDifference","angleDifference"])],axis=1)

        for i in range(0,(timePeriod-step),step):
            objects=summaryDF.loc[(summaryDF['FirstFrame']>=i)&(summaryDF['FirstFrame']<i+step)]
            for r_index,row in objects.iterrows():
                if row["Count"]<minTrajLen:
                    continue            

                for r_index_second,row_second in objects.iterrows():
                    if row_second["ObjectID"]==row["ObjectID"]:
                        continue
                    if row_second["Count"]<minTrajLen:
                        continue
                    directionDiff=abs(row_second["TrajDir"]-row["TrajDir"])                
                    if directionDiff<dirDiffThreshold or directionDiff>360-dirDiffThreshold: #15 degree direction difference
                        continue                
                    trajObject1=trajDF.loc[trajDF[ObjectIDField]==row["ObjectID"]]
                    
                    trajPointsLen1=len(trajObject1.index)
                    
                    trajDistance=100000 #used to find the minimum distance between two trajectories
                    for i_1 in range(smoothInterval,trajPointsLen1):
                        #currentDF1=trajObject1.loc[trajObject1.index[i_1],:].to_frame().T
                        current_X_1=trajObject1.loc[trajObject1.index[i_1],CoordXField]
                        current_Y_1=trajObject1.loc[trajObject1.index[i_1],CoordYField]
                        #current_speed_1=trajObject1.loc[trajObject1.index[i_1],SpeedField]
                        current_frameIndex_1=trajObject1.loc[trajObject1.index[i_1],FrameIndexField]
                        current_direction_1=trajObject1.loc[trajObject1.index[i_1],DirectionField]
                        #convert meter per sec to mph
                        trajObject2=trajDF.loc[trajDF[ObjectIDField]==row_second["ObjectID"]]                    
                        trajObject2=trajObject2.loc[numpy.abs(trajObject2[FrameIndexField]-current_frameIndex_1)/10<=timeThreshold]
                        if len(trajObject2.index)<1:
                            continue
                        trajObject2=trajObject2.loc[(numpy.abs(trajObject2[DirectionField]-current_direction_1)>=dirDiffThreshold)&(numpy.abs(trajObject2[DirectionField]-current_direction_1)<=(360-dirDiffThreshold))]                    
                        #trajPointsLen2=len(trajObject2.index)
                        if len(trajObject2.index)<1:
                            continue
                        # print(trajObject2.shape)
                        # print(numpy.sqrt((trajObject2[CoordXField]-current_X_1)**2+(trajObject2[CoordYField]-current_Y_1)**2))
                        trajObject2=trajObject2.loc[numpy.sqrt((trajObject2[CoordXField]-current_X_1)**2+(trajObject2[CoordYField]-current_Y_1)**2)<=distanceThreshold]
                        if len(trajObject2.index)<1:
                            continue
                        tempDistanceDF=numpy.sqrt((trajObject2[CoordXField]-current_X_1)**2+(trajObject2[CoordYField]-current_Y_1)**2)
                        minIndex=tempDistanceDF.idxmin()
                        current_X_2=trajObject2.loc[minIndex,CoordXField]
                        current_Y_2=trajObject2.loc[minIndex,CoordYField]
                        #current_speed_2=trajObject2.loc[trajObject2.index[i_2],SpeedField]
                        current_frameIndex_2=trajObject2.loc[minIndex,FrameIndexField]
                        current_direction_2=trajObject2.loc[minIndex,DirectionField]        
                        objectDistance=sqrt((current_X_2 - current_X_1)**2+(current_Y_2 - current_Y_1)**2)
                        timeDifference=abs(current_frameIndex_1-current_frameIndex_2)/10  
                        point_directionDiff=abs(current_direction_1-current_direction_2)                                               
                        conflictTrajDF=conflictTrajDF.append(trajObject1)                            
                        additionalArray=numpy.array([objectDistance,timeDifference,point_directionDiff])
                        currentDF1Array=trajObject1.loc[trajObject1.index[i_1],:].to_numpy().T
                        currentDF2Array=trajObject2.loc[minIndex,:].to_numpy().T                 
                        # integrateObjects=numpy.concatenate((numpy.array(currentDF1Array), numpy.array(currentDF2Array)), axis=None)
                        # integrateObjects=numpy.concatenate((integrateObjects, additionalArray), axis=None) 
                        integrateObjects=numpy.concatenate((currentDF1Array,currentDF2Array,additionalArray),axis=None)
                        #conflictSummaryDF.reset_index(drop=True, inplace=True)
                        tempNewDF=pandas.DataFrame(numpy.array([integrateObjects]), columns=columnsArray)
                        conflictSummaryDF =conflictSummaryDF.append(tempNewDF,ignore_index=True) 
                        #conflictSummaryDF.append(pandas.DataFrame(integrateObjects, columns=conflictSummaryDF.columns))
                        #  conflictSummaryDF.append(dict(zip(conflictSummaryDF.columns, integrateObjects)), ignore_index=True)
                        conflictFound=True
                        break
                        
                    
        if len(conflictTrajDF.index)>0:
            if not os.path.isfile(conflictTrajFile):
                conflictTrajDF.to_csv(conflictTrajFile,index=False,header=True)
            else: # else it exists so append without writing the header
                conflictTrajDF.to_csv(conflictTrajFile,mode='a',index=False,header=True)            
        #mapCSVtoFeature(csvFileBaseName="",outputWorkspace="",featureClassOutput="",xFieldName=LongitudeField,yFieldName=LatitudeField,  sr=arcpy.SpatialReference(4326)):
        if len(conflictSummaryDF.index)>0:  
            if not os.path.isfile(conflictSummaryFile):
                conflictSummaryDF.to_csv(conflictSummaryFile,index=False,header=True)
            else: # else it exists so append without writing the header
                conflictSummaryDF.to_csv(conflictSummaryFile,mode='a',index=False,header=True)
            
def newConflictIdentification2(trajFile="",summaryFile="",dirDiffThreshold=0,cell_size=2,conflict_time_threshold=2,chunkSize=200000):
    print("conflict analysis - version new 2")
    print(trajFile)    
    #print(summaryFile)
    # if not "2022-05-21-17-30-00_cl" in trajFile:
    #     return
    fileFolder=os.path.dirname(trajFile)
    fileBaseName=os.path.basename(trajFile)
    newFileFolder=fileFolder+"/conflict"
    conflictTrajFile=newFileFolder+"/"+fileBaseName[:-4]+"_conflict_traj.csv"    
    conflictSummaryFile=summaryFile[:-4]+"_conflict.csv"
    if not overwriteOutputFiles and exists(conflictSummaryFile):
        return

    if overwriteOutputFiles and exists(conflictSummaryFile):
        os.remove(conflictSummaryFile)
    if overwriteOutputFiles and exists(conflictTrajFile):
        os.remove(conflictTrajFile)
    
    reader=pandas.read_csv(trajFile,chunksize=chunkSize)
    #summaryDF=pandas.read_csv(summaryFile)

    x_low_boud=-100 #meter
    x_high_bound=100 #meter
    y_low_bound=-100 #meter
    y_high_bound=100 #meter
    # cell_size=2 #meter
    # conflict_time_threshold=2 #second
    conflictSummaryDF=pandas.DataFrame()
    
    for trajDF in reader:
        for x_cell_low in numpy.arange(x_low_boud,x_high_bound,cell_size):
            x_cell_high=x_cell_low +cell_size
            tempCellTrajDF=trajDF.loc[trajDF[CoordXField].between(x_cell_low,x_cell_high,inclusive=True)]
            # print(len(tempCellTrajDF))
            # print(tempCellTrajDF)
            # print(tempCellTrajDF[CoordYField])
            if len(tempCellTrajDF)==0:                
                continue
            for y_cell_low in numpy.arange(y_low_bound,y_high_bound,cell_size):
                y_cell_high=y_cell_low+cell_size
                #print(cellTrajDF[CoordYField].between(y_cell_low,y_cell_high,inclusive=True))
                cellTrajDF=tempCellTrajDF.loc[tempCellTrajDF[CoordYField].between(y_cell_low,y_cell_high,inclusive=True)]
                # print(y_cell_low)
                # print(y_cell_high)
                # print(len(cellTrajDF))
                if len(cellTrajDF)==0:                    
                    continue
                cellTrajDF=cellTrajDF.drop_duplicates(ObjectIDField,keep='first')
                # print(len(cellTrajDF))
                # print(cellTrajDF[ObjectIDField])
                # print(numpy.abs(cellTrajDF[FrameIndexField]-cellTrajDF[FrameIndexField].shift()))
                # print((numpy.abs(cellTrajDF[FrameIndexField]-cellTrajDF[FrameIndexField].shift())<=(conflict_time_threshold*10)))
                # print(numpy.abs(cellTrajDF[DirectionField]-cellTrajDF[DirectionField].shift()))
                # print(numpy.abs(cellTrajDF[DirectionField]-cellTrajDF[DirectionField].shift())>=dirDiffThreshold)
                conflict_row_indexs=(numpy.abs(cellTrajDF[FrameIndexField]-cellTrajDF[FrameIndexField].shift())<=(conflict_time_threshold*10))&(numpy.abs(cellTrajDF[DirectionField]-cellTrajDF[DirectionField].shift())>=dirDiffThreshold)&(numpy.abs(cellTrajDF[DirectionField]-cellTrajDF[DirectionField].shift())<=(360-dirDiffThreshold))
                conflictTrajDF=cellTrajDF.loc[conflict_row_indexs] 
                if len(conflictTrajDF)==0 or len(conflictTrajDF)>100:
                    continue
                #print(conflictTrajDF)
                conflictTrajDF_Shift_ID=cellTrajDF[ObjectIDField].shift()
                conflictTrajDF_Shift_ID=conflictTrajDF_Shift_ID.loc[conflict_row_indexs]
                             
                conflictTrajDF2=cellTrajDF.loc[cellTrajDF[ObjectIDField].isin(conflictTrajDF_Shift_ID)]
                #print(conflictTrajDF2)
                timeDifference=conflictTrajDF2[FrameIndexField].to_numpy() -conflictTrajDF[FrameIndexField].to_numpy() 
                timeDifference=timeDifference.reshape(len(timeDifference),1)
                directionDifference=conflictTrajDF2[DirectionField].to_numpy() -conflictTrajDF[DirectionField].to_numpy() 
                directionDifference=directionDifference.reshape(len(directionDifference),1)
                newColumns=[str(col) + '_cf' for col in conflictTrajDF2.columns]
                # print (timeDifference)
                # print (directionDifference)
                conflictTrajDF2.columns=newColumns                
                integrateTimeDirection=numpy.concatenate((timeDifference,directionDifference),axis=1)
                #print(integrateTimeDirection)
                timeDirectionDiffDB=pandas.DataFrame(integrateTimeDirection,columns=["timeDiff","directionDiff"])
                timeDirectionDiffDB["distanceRange"]=cell_size
                tempConflictSummaryDF=pandas.concat([conflictTrajDF.reset_index(drop=True),conflictTrajDF2.reset_index(drop=True),timeDirectionDiffDB.reset_index(drop=True)],axis=1)

                if not tempConflictSummaryDF.empty:
                    if conflictSummaryDF.empty:
                        conflictSummaryDF=tempConflictSummaryDF
                    else:
                        conflictSummaryDF=pandas.concat([conflictSummaryDF,tempConflictSummaryDF])


                # # print(conflictTrajDF)
                # # print(conflictTrajDF2)
                # # print(timeDirectionDiffDB)
                # # print(conflictSummaryDF)
                # conflictObjectIDs=numpy.concatenate((conflictTrajDF[ObjectIDField].to_numpy(),conflictTrajDF2[ObjectIDField+'_cf'].to_numpy()),axis=None)
                # #print(conflictObjectIDs)
                # conflictTrajDF=trajDF.loc[trajDF[ObjectIDField].isin(conflictObjectIDs)]

                # if len(conflictTrajDF.index)>0:
                #     if not os.path.isfile(conflictTrajFile):
                #         conflictTrajDF.to_csv(conflictTrajFile,index=False,header=True,line_terminator = '\r')
                #     else: # else it exists so append without writing the header
                #         conflictTrajDF.to_csv(conflictTrajFile,mode='a',index=False,header=False,line_terminator = '\r')            
                # #mapCSVtoFeature(csvFileBaseName="",outputWorkspace="",featureClassOutput="",xFieldName=LongitudeField,yFieldName=LatitudeField,  sr=arcpy.SpatialReference(4326)):
                # if len(conflictSummaryDF.index)>0:  
                #     if not os.path.isfile(conflictSummaryFile):
                #         conflictSummaryDF.to_csv(conflictSummaryFile,index=False,header=True,line_terminator = '\r')
                #     else: # else it exists so append without writing the header
                #         conflictSummaryDF.to_csv(conflictSummaryFile,mode='a',index=False,header=False,line_terminator = '\r')
    
    if not conflictSummaryDF.empty:
        conflictSummaryDF.drop_duplicates([ObjectIDField,ObjectIDField+"_cf"],keep= 'first')
        conflictSummaryDF.to_csv(conflictSummaryFile,index=False,header=True)

        conflictObjectIDs=numpy.concatenate((conflictSummaryDF[ObjectIDField].to_numpy(),conflictSummaryDF[ObjectIDField+'_cf'].to_numpy()),axis=None)
        #print(conflictObjectIDs)
        conflictTrajDF=trajDF.loc[trajDF[ObjectIDField].isin(conflictObjectIDs)]
        conflictTrajDF.to_csv(conflictTrajFile,index=False,header=True,line_terminator = '\r')


def trajectorySummaryandConflicts(file=""):
    #filePath=r"C:\ceehao\workspacePyramid\conflict\2020-8-29-10-30-0-BF1-CL1-Traj(0-18000frames).csv"
    #outputFile=file[:-4]+"_add.csv"
    start_time=time.time()
    # try:
    fileFolder=os.path.dirname(file)
    fileBaseName=os.path.basename(file)
    newFileFolder=fileFolder+"/conflict"
    outputSummaryFile=newFileFolder+"/"+fileBaseName[:-4]+"_summary.csv"
    #newTrajSummaryNoGIS(file,outputSummaryFile=outputSummaryFile,minTrajLen=30)  

    # #temporarily call the old function using the old method for conflicts output
    # newTrajSummaryNoGIS(file,outputSummaryFile=outputSummaryFile,minTrajLen=20)  
    # newConflictIdentification(trajFile=file,summaryFile=outputSummaryFile,timePeriod=18000,dirDiffThreshold=30,step=100,distanceThreshold=5,timeThreshold=5, smoothInterval=5)
    # #end of the temporarily call of the old function
    
    newConflictIdentification2(trajFile=file,summaryFile=outputSummaryFile,dirDiffThreshold=0,cell_size=conflictCellSize,conflict_time_threshold=3)
    # except Exception as e:
    #     print(e)
    #     print ("!!Error occured when analyzing conflicts of "+file)
    print("--- %s seconds ---" % (time.time() - start_time))

def integrateConflictSummaryFiles(workspace,gdbWorkSpace):
    inputs=glob.glob(workspace+"/*_summary_conflict.csv")
    outputFile=workspace+"/conflictSum_Integration_"+str(conflictCellSize)+".csv"
    if not overwriteOutputFiles and exists(outputFile):
        return
    inputDF=pandas.DataFrame()
    for f in inputs:
        print(f)
        if not exists(f):
            continue
        tempInputDF=pandas.read_csv(f) 
        if tempInputDF.empty:
            continue
        if inputDF.empty:
            inputDF=tempInputDF
            #tempInputDF.to_csv(outputFile,index=False,header=True)
        else:
            #inputDF=pandas.concat([inputDF,tempInputDF],axis=0,ignore_index=True)
            inputDF=pandas.concat([inputDF,tempInputDF],axis=0,ignore_index=False)
            #tempInputDF.to_csv(outputFile,mode="a",index=False,header=False)
    if inputDF.empty:
        print("No conflict summary files/data!")
    else:        
        inputDF.to_csv(outputFile,index=False,header=True)
        UNRGISLib.mapCSVtoFeature(csvFileBaseName=outputFile,outputWorkspace=gdbWorkspace,featureClassOutput="conflictSum_Integration",xFieldName="Longitude",yFieldName="Latitude")
    
def integrateConflictSummaryFiles2(workspace,gdbWorkSpace):
    inputs=glob.glob(workspace+"/*_summary_conflict.csv")
    outputFile=workspace+"/conflictSum_Integration.csv"
    if not overwriteOutputFiles and exists(outputFile):
        return
    dataList=list()

    inputDF=pandas.DataFrame()
    columns=[]
    for f in inputs:
        print(f)
        if not exists(f):
            continue
        tempInputDF=pandas.read_csv(f)      
        if tempInputDF.empty:
            continue
        if len(columns)==0:
            columns=tempInputDF.columns
            print("assign columns ...")
        tempList=tempInputDF.values.tolist()
        dataList.extend(tempList)

    if len(dataList)==0 or len(columns)==0:
        print("No conflict summary files/data!")
        return
    else:
        inputDF=pandas.DataFrame(dataList,columns=columns)    
        inputDF.to_csv(outputFile,index=False,header=True)
        UNRGISLib.mapCSVtoFeature(csvFileBaseName=outputFile,outputWorkspace=gdbWorkspace,featureClassOutput="conflictSum_Integration",xFieldName="Longitude",yFieldName="Latitude")
    
def integrateConflictSummaryFiles3(workspace,gdbWorkSpace):
    inputs=glob.glob(workspace+"/*_summary_conflict.csv")
    outputFile=workspace+"/conflictSum_Integration_"+str(conflictCellSize).replace(".","_")+".csv"
    if not arcpy.Exists(gdbWorkspace):
        arcpy.CreateFileGDB_management(rootFolder+"/"+trajFolderName, trajFolderName+"_conflict.gdb") 
    if not overwriteOutputFiles and exists(outputFile):
        return
    if overwriteOutputFiles and exists(outputFile):
        os.remove(outputFile)
    
    tempInputs=inputs
    for f in tempInputs:
        print(f)
        if not exists(f):
            continue
        fileSize=os.path.getsize(f)
        if fileSize>10000000:
            print("remove large file of "+f)
            inputs.remove(f)
           

    print ("combine summary conflict files ...")
    inputDF=pandas.concat(map(pandas.read_csv,inputs))

    #the following code block integrating summary conflicts, but had memory error with many files, so was replaced.
    """inputDF=pandas.DataFrame()
    for f in inputs:
        print(f)
        if not exists(f):
            continue
        tempInputDF=pandas.read_csv(f) 
        
        if tempInputDF.empty:
            continue     
        else:
            if inputDF.empty:
                inputDF=tempInputDF
            else:
                inputDF=pandas.concat([inputDF,tempInputDF])
    """

            
            # if not os.path.isfile(outputFile):
            #     tempInputDF.to_csv(outputFile,index=False,header=True,line_terminator = '\r')
            # else:
            #     tempInputDF.to_csv(outputFile,mode='a',index=False,header=False,line_terminator = '\r')
    if not inputDF.empty:
        # inputDF.drop_duplicates([ObjectIDField,ObjectIDField+"_cf"],keep= 'first')
        inputDF.to_csv(outputFile,index=False,header=True)

    if os.path.isfile(outputFile):
        arcpy.env.workspace=gdbWorkspace
        arcpy.env.overwriteOutput=True  
        UNRGISLib.mapCSVtoFeature(csvFileBaseName=outputFile,outputWorkspace=gdbWorkSpace,featureClassOutput="conflictSum_Integration_"+str(conflictCellSize).replace(".","_"),xFieldName="Longitude",yFieldName="Latitude")

def mappingConflicts(trajFolderName):
    print("Sum output and mapping... ")   
    gdbWorkspace=rootFolder+"/"+trajFolderName+"/"+trajFolderName+"_conflict.gdb"
    workspace=rootFolder+"/"+trajFolderName+"/"+trajSubFolderName 
    if not arcpy.Exists(gdbWorkspace):
        arcpy.CreateFileGDB_management(rootFolder+"/"+trajFolderName, trajFolderName+"_conflict.gdb")
    inputs = glob.glob(workspace+"/conflict"+"/*_conflict_traj.csv") #all trajectory csvs in workspace   
    arcpy.env.workspace=gdbWorkspace
    arcpy.env.overwriteOutput=True    
    for f in inputs:
        # fileName=os.path.basename(f)
        # outputFeatureName=fileName[:-4]
        UNRGISLib.mapTrajectoriesFromCSV(f,gdbWorkspace,preStrFeatureName="conflict",overwriteTrajFeature=overwriteOutputFiles)
    #integrateConflictSummaryFiles3(workspace=workspace+"/conflict",gdbWorkSpace=gdbWorkspace)


for trajFolderName in trajFolderNameArray:
    gdbWorkspace=rootFolder+"/"+trajFolderName+"/"+trajFolderName+"_conflict.gdb"
    workspace=rootFolder+"/"+trajFolderName+"/"+trajSubFolderName
    print(workspace)
    print(gdbWorkspace)               
    conflictFolder=workspace+"/conflict"
    if not os.path.exists(conflictFolder):
        os.makedirs(conflictFolder)
    else:
        print(conflictFolder+'folder already exists')
    inputs = glob.glob(workspace+"/*_cl.csv")
    if boolParallel:
        results=Parallel(n_jobs=num_cores)(delayed(trajectorySummaryandConflicts)(f) for f in inputs)
    else:
        for f in inputs:
            trajectorySummaryandConflicts(f)
    #integrateConflictSummaryFiles3(workspace=workspace+"/conflict",gdbWorkSpace=gdbWorkspace)

#parallel processing caused GIS errors. mapping conflict trajectories are not needed.    
# boolParallel=False
# if boolParallel:
#     results=Parallel(n_jobs=num_cores)(delayed(mappingConflicts)(trajFolderName) for trajFolderName in trajFolderNameArray)
# else:
#     for trajFolderName in trajFolderNameArray:
#         mappingConflicts(trajFolderName)






        


