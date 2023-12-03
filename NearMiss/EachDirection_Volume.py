import arcpy
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict
import os
from tqdm import tqdm
import re

def Direction_Judgement(from_zone, to_zone):
    # 提取方向信息
    from_direction = from_zone.split('_')[-2]
    to_direction = to_zone.split('_')[-2]

    # 定义方向转换字典
    direction_mappings = {
        ('EB', 'EB'): 'EB_Through',
        ('EB', 'NB'): 'EB_LeftTurn',
        ('EB', 'SB'): 'EB_RightTurn',
        ('EB', 'WB'): 'EB_UTurn',
        ('SB', 'SB'): 'SB_Through',
        ('SB', 'EB'): 'SB_LeftTurn',
        ('SB', 'WB'): 'SB_RightTurn',
        ('SB', 'NB'): 'SB_UTurn',
        ('NB', 'NB'): 'NB_Through',
        ('NB', 'WB'): 'NB_LeftTurn',
        ('NB', 'EB'): 'NB_RightTurn',
        ('NB', 'SB'): 'NB_UTurn',
        ('WB', 'WB'): 'WB_Through',
        ('WB', 'SB'): 'WB_LeftTurn',
        ('WB', 'NB'): 'WB_RightTurn',
        ('WB', 'EB'): 'WB_UTurn'
    }

    # 根据方向转换字典得出结果
    return direction_mappings.get((from_direction, to_direction), "Unknown Direction")


def extract_vehicle_volume(point_layer, polygon_zone_from, polygon_zone_to, site_name):
    # 允许覆盖输出
    arcpy.env.overwriteOutput = True

    # 选择与polygon_zone1相交的点
    arcpy.MakeFeatureLayer_management(point_layer, 'tempLayer1')
    arcpy.SelectLayerByLocation_management('tempLayer1', 'WITHIN', polygon_zone_from)
    selected_zone1 = set(row[0] for row in arcpy.da.SearchCursor('tempLayer1', "ObjectID"))

    # 选择与polygon_zone2相交的点
    arcpy.MakeFeatureLayer_management(point_layer, 'tempLayer2')
    arcpy.SelectLayerByLocation_management('tempLayer2', 'WITHIN', polygon_zone_to)
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
    direction = Direction_Judgement(polygon_zone_from, polygon_zone_to)
    df_result['Direction'] = direction
    df_result['Site'] = site_name
    df_result['Date'] = pd.to_datetime(df_result['Timestamp']).dt.date
    df_result['Rounded_15min'] = pd.to_datetime(df_result['Timestamp']).apply(simple_rounddown_time_str)

    return df_result
def round_to_nearest_30min(dt):
    # round down to 30 min
    if dt.minute < 30:
        rounded_minute = 0
    else:
        rounded_minute = 30

    return dt.replace(minute=rounded_minute, second=0, microsecond=0)
def simple_rounddown_time_str(dt):
    # Extract minutes from the datetime object
    minutes = dt.minute

    # Determine the rounded down minute value
    if minutes < 15:
        rounded_minutes = "00"
    elif minutes < 30:
        rounded_minutes = "15"
    elif minutes < 45:
        rounded_minutes = "30"
    else:
        rounded_minutes = "45"

    # Format the datetime as a string with only hours and the rounded down minutes
    return dt.strftime(f"%H:{rounded_minutes}")

def Direction_Judgement_PedZone(from_zone, to_zone):
    # 移除扩展名并提取方向信息
    from_direction = from_zone.split('_')[-1].split('.')[0]
    to_direction = to_zone.split('_')[-1].split('.')[0]

    # 组合方向信息
    return f'{from_direction} to {to_direction}'
def extract_Ped_Volume(point_layer, ped_zone_from, ped_zone_to, site_name):
    # 允许覆盖输出
    arcpy.env.overwriteOutput = True

    # 选择与 ped_zone_from 相交的点
    arcpy.MakeFeatureLayer_management(point_layer, 'tempLayer1')
    arcpy.SelectLayerByLocation_management('tempLayer1', 'WITHIN', ped_zone_from)
    selected_zone1 = set(row[0] for row in arcpy.da.SearchCursor('tempLayer1', "ObjectID"))

    # 选择与 ped_zone_to 相交的点
    arcpy.MakeFeatureLayer_management(point_layer, 'tempLayer2')
    arcpy.SelectLayerByLocation_management('tempLayer2', 'WITHIN', ped_zone_to)
    selected_zone2 = set(row[0] for row in arcpy.da.SearchCursor('tempLayer2', "ObjectID"))

    # 找到同时在两个区域中的点的ObjectID
    common_pedestrians = selected_zone1.intersection(selected_zone2)

    # 从整个地图的属性表中提取相关数据
    df_all = pd.DataFrame([row for row in arcpy.da.SearchCursor(point_layer, ["ObjectID", "Timestamp", "Class"])],
                           columns=["ObjectID", "Timestamp", "Class"])

    # 根据common_pedestrians筛选数据
    df_common = df_all[df_all['ObjectID'].isin(common_pedestrians)]

    # 获取每个ObjectID的最小Timestamp
    df_result = df_common.groupby('ObjectID').agg({
        'Timestamp': 'min',
        'Class': 'first'
    }).reset_index()

    # 获取方向
    direction = Direction_Judgement_PedZone(ped_zone_from, ped_zone_to)
    df_result['Direction'] = direction
    df_result['Site'] = site_name
    df_result['Date'] = pd.to_datetime(df_result['Timestamp']).dt.date
    df_result['Rounded_Time'] = pd.to_datetime(df_result['Timestamp']).dt.round('15min').dt.time
    return df_result
def remove_diagonal_ped_zone_to(zone_from, zones_list):
    # 定义对角线关系
    diagonals = {
        'SW': 'NE',
        'NE': 'SW',
        'SE': 'NW',
        'NW': 'SE'
    }

    # 使用正则表达式提取角落缩写
    match = re.search(r'_(SW|NE|SE|NW)\b', zone_from)
    if not match:
        raise ValueError("Cannot find corner abbreviation in zone_from string")

    corner_abbreviation = match.group(1)

    # 获取 zone_from 的对角线角落
    diagonal_corner = diagonals[corner_abbreviation]

    # 移除对角线角落
    remaining_zones = [zone for zone in zones_list if diagonal_corner not in zone]

    return remaining_zones

def get_summary_table(data, pivot_excel_path):
    # Step 1: Determine all unique 'DateTime' values
    unique_datetimes = data['DateTime'].drop_duplicates().sort_values()

    # Step 2: Determine all unique 'Direction' and 'Direction_from' values
    unique_directions = data['Direction'].drop_duplicates()
    unique_directions_from = data['Direction_from'].drop_duplicates()

    # Step 3: Create a pivot table for 'Direction'
    pivot_direction = data.pivot_table(index='DateTime',
                                       columns='Direction',
                                       aggfunc='size',
                                       fill_value=0)

    # Step 4: Create a pivot table for 'Direction_from'
    pivot_direction_from = data.pivot_table(index='DateTime',
                                            columns='Direction_from',
                                            aggfunc='size',
                                            fill_value=0)

    # Since we want both Direction and Direction_from in the same table, we can join these pivot tables
    # It's important to ensure there are no overlapping column names, hence we will prefix the column names
    pivot_direction.columns = [str(col) + '_Direction' for col in pivot_direction.columns]
    pivot_direction_from.columns = [str(col) + '_Direction_from' for col in pivot_direction_from.columns]

    # Step 5: Join the two pivot tables
    pivot_combined = pivot_direction.join(pivot_direction_from)

    # Step 6: Save the combined pivot table to a new Excel file
    pivot_combined.to_excel(pivot_excel_path)
    return pivot_combined



wildCard = "map2023*"

# ##########################################################################
# below: Keystone_7th
path_output_volume_Veh = r'Z:\NSFSCCTrajectories\KeystoneAnd7thST\Analysis\Volume\Volume_Veh.csv'
path_output_volume_MM = r'Z:\NSFSCCTrajectories\KeystoneAnd7thST\Analysis\Volume\Volume_MM.csv'
path_summary_Veh = r'Z:\NSFSCCTrajectories\KeystoneAnd7thST\Analysis\Volume\Summary_Veh.xlsx'
path_shpFile = r'Z:\NSFSCCTrajectories\KeystoneAnd7thST\Analysis\shpFile_Volume'
geodatabase_path = r'Z:\NSFSCCTrajectories\KeystoneAnd7thST\KeystoneAnd7thST.gdb'
siteName = 'Keystone_7th'
# ##########################################################################

# ##########################################################################
# below: Keystone_McCarren
# path_output_volume_Veh = r'Z:\NSFSCCTrajectories\KeystoneAndMcCarran\New folder\Volume_Veh.csv'
# path_output_volume_MM = r'Z:\NSFSCCTrajectories\KeystoneAndMcCarran\New folder\Volume_MM.csv'
# path_shpFile = r'Z:\NSFSCCTrajectories\KeystoneAndMcCarran\New folder\ShpFiles'
# geodatabase_path = r'Z:\NSFSCCTrajectories\KeystoneAndMcCarran\KeystoneAndMcCarran.gdb'
# siteName = 'Keystone_McCarren'
# ##########################################################################

from_zones_veh = []
to_zones_veh = []
zones_ped = []
for file in os.listdir(path_shpFile):
    if file.endswith('.shp') and 'PedZone' in file:
        zones_ped.append(os.path.join(path_shpFile, file))
for file in os.listdir(path_shpFile):
    if file.endswith('.shp') and 'VehZone' in file:
        if 'from' in file:
            from_zones_veh.append(os.path.join(path_shpFile, file))
        elif 'to' in file:
            to_zones_veh.append(os.path.join(path_shpFile, file))

all_data_veh = []
all_data_ped = []

# 设置工作空间
arcpy.env.workspace = geodatabase_path
arcpy.env.overwriteOutput = True
feature_classes = arcpy.ListFeatureClasses(wild_card=wildCard)

for trajectory_file in tqdm(feature_classes):
    for from_zone in from_zones_veh:
        for to_zone in to_zones_veh:
            site_name = siteName
            result_veh = extract_vehicle_volume(trajectory_file, from_zone, to_zone, site_name)
            all_data_veh.append(result_veh)
    for ped_zone_from in zones_ped:
        for ped_zone_to in remove_diagonal_ped_zone_to(ped_zone_from,zones_ped):
            result_ped = extract_Ped_Volume(trajectory_file, ped_zone_from, ped_zone_to, siteName)
            all_data_ped.append(result_ped)


# 合并所有的 DataFrame
if all_data_veh:
    final_result_veh = pd.concat(all_data_veh, ignore_index=True)
    final_result_veh['DateTime'] = pd.to_datetime(final_result_veh['Date'].astype(str) + ' ' +
                                                  final_result_veh['Rounded_15min'].astype(str))
    # Creating a new 'Direction_from' column by splitting the 'Direction' column and taking the part before '_'
    final_result_veh['Direction_from'] = final_result_veh['Direction'].\
        apply(lambda x: x.split('_')[0] if '_' in x else x)
    final_result_veh['map'] = final_result_veh['DateTime'].apply(round_to_nearest_30min).dt.strftime('map%Y%m%d%H%M%S')
    final_result_veh.to_csv(path_output_volume_Veh)
else:
    print("No vehicle data to write.")

if all_data_ped:
    final_result_ped = pd.concat(all_data_ped, ignore_index=True)
    final_result_ped.to_csv(path_output_volume_MM)
else:
    print("No pedestrian data to write.")
get_summary_table(final_result_veh,path_summary_Veh)