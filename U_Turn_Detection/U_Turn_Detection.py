import arcpy
import datetime,time
import pandas as pd
from tqdm import tqdm
import os

def U_Turn_Lane_To_Judgement(lane_from, lane_nb_leave, lane_sb_leave, lane_wb_leave, lane_eb_leave):
    if 'EB' in lane_from:
        return lane_wb_leave
    elif 'WB' in lane_from:
        return lane_eb_leave
    elif 'SB' in lane_from:
        return lane_nb_leave
    elif 'NB' in lane_from:
        return lane_sb_leave
    else:
        return 'Unknown'


def Direction_Judgement_U_Turn(lane_from):
    if 'EB' in lane_from:
        u_turn_direction = 'EB_to_WB'
    elif 'WB' in lane_from:
        u_turn_direction = 'WB_to_EB'
    elif 'SB' in lane_from:
        u_turn_direction = 'SB_to_NB'
    elif 'NB' in lane_from:
        u_turn_direction = 'NB_to_SB'
    else:
        u_turn_direction = 'Unknown'
    return u_turn_direction


def extract_vehicle_data(point_layer, polygon_zone1, polygon_zone2, site_name):
    # 允许覆盖输出
    arcpy.env.overwriteOutput = True

    # 选择与polygon_zone1相交的点
    arcpy.MakeFeatureLayer_management(point_layer, 'tempLayer1')
    arcpy.SelectLayerByLocation_management('tempLayer1', 'WITHIN', polygon_zone1)
    selected_zone1 = set(row[0] for row in arcpy.da.SearchCursor('tempLayer1', "ObjectID"))

    # 选择与polygon_zone2相交的点
    arcpy.MakeFeatureLayer_management(point_layer, 'tempLayer2')
    arcpy.SelectLayerByLocation_management('tempLayer2', 'WITHIN', polygon_zone2)
    selected_zone2 = set(row[0] for row in arcpy.da.SearchCursor('tempLayer2', "ObjectID"))

    # 找到同时在两个区域中的点的ObjectID
    common_vehicles = selected_zone1.intersection(selected_zone2)

    # 从整个地图的属性表中提取相关数据
    df_all = pd.DataFrame([row for row in arcpy.da.SearchCursor(point_layer, ["ObjectID", "Timestamp", "Class"])],
                           columns=["ObjectID", "Timestamp", "Class"])

    # 根据common_vehicles筛选数据
    df_common = df_all[df_all['ObjectID'].isin(common_vehicles)]

    # 获取每个ObjectID的最小Timestamp
    df_result = df_common.groupby('ObjectID').agg({
        'Timestamp': 'min',
        'Class': 'first'
    }).reset_index()

    # 添加其他列
    direction = Direction_Judgement_U_Turn(polygon_zone1)
    df_result['Direction'] = direction
    df_result['Site'] = site_name
    df_result['Date'] = pd.to_datetime(df_result['Timestamp']).dt.date
    df_result['Rounded_Time'] = pd.to_datetime(df_result['Timestamp']).dt.round('15min').dt.time

    return df_result


wildcard ='map2023*'
site_list = ['Mcleod']

for site_name in tqdm(site_list):
    base_path = r'Z:\Flamingo_SMP\{}'.format(site_name)

    # 检查并创建Analysis文件夹
    analysis_path = os.path.join(base_path, 'Analysis')
    if not os.path.exists(analysis_path):
        os.mkdir(analysis_path)

    # 检查并创建U_Turn文件夹
    u_turn_path = os.path.join(analysis_path, 'U_Turn')
    if not os.path.exists(u_turn_path):
        os.mkdir(u_turn_path)
    shpFile_folder = r'C:\Users\Fei\Documents\ArcGIS\Projects\QC\{}'.format(site_name)
    path_folder =  u_turn_path
    path_gdb = r'Z:\Flamingo_SMP\{}\{}.gdb'.format(site_name, site_name)

    vehLane_EB = r'{}\U_Turn\VehZone_EB.shp'.format(shpFile_folder)
    vehLane_NB = r'{}\U_Turn\VehZone_NB.shp'.format(shpFile_folder)
    vehLane_WB = r'{}\U_Turn\VehZone_WB.shp'.format(shpFile_folder)
    vehLane_SB = r'{}\U_Turn\VehZone_SB.shp'.format(shpFile_folder)
    vehLane_EB_leave = r'{}\U_Turn\VehZone_EB_leave.shp'.format(shpFile_folder)
    vehLane_NB_leave = r'{}\U_Turn\VehZone_NB_leave.shp'.format(shpFile_folder)
    vehLane_WB_leave = r'{}\U_Turn\VehZone_WB_leave.shp'.format(shpFile_folder)
    vehLane_SB_leave = r'{}\U_Turn\VehZone_SB_leave.shp'.format(shpFile_folder)
    list_vehZone = [vehLane_EB,vehLane_WB,vehLane_NB,vehLane_SB]
    with arcpy.EnvManager(scratchWorkspace=path_gdb, workspace=path_gdb):
        # 使用通配符列出所有以 "map" 开头的图层
        dataset = arcpy.ListFeatureClasses(feature_type='Point', wild_card=wildcard)
        # 使用列表推导式进一步过滤这些图层
        df_result = pd.DataFrame()
        # dataset_projected = Project_Map(dataset)
        for map_30min in tqdm(dataset):
            for each_direction in list_vehZone:
                df_to_append = extract_vehicle_data(map_30min, each_direction ,
                                                    U_Turn_Lane_To_Judgement(each_direction,vehLane_NB_leave,
                                                                             vehLane_SB_leave,vehLane_WB_leave,
                                                                             vehLane_EB_leave), site_name)
                df_result = pd.concat([df_result, df_to_append], ignore_index=True)
        df_result.to_csv('{}\\Results_YieldAnalysis'.format(path_folder) + '{}.csv'.format(site_name))