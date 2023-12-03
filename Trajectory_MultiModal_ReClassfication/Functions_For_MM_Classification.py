import pandas as pd
import numpy as np
def process_traffic_data(file_path):
    # Load the Excel file
    data = pd.read_excel(file_path)

    # Replace common placeholders for missing values with NaN
    data.replace([None, "<Null>", ""], pd.NA, inplace=True)

    # Drop duplicates
    data = data.drop_duplicates()
    if 'Dis' in data.columns:
        if 'Height' in data.columns:
            data['Height/Dis'] = data['Height'] / (data['Dis'] + 1e-9)
        if 'Width' in data.columns:
            data['Width/Dis'] = data['Width'] / (data['Dis'] + 1e-9)
        if 'Length' in data.columns:
            data['Length/Dis'] = data['Length'] / (data['Dis'] + 1e-9)
    # Dropping irrelevant columns if they exist
    irrelevant_columns = ['Dir_X_Bbox', 'Dir_Y_Bbox', 'MoveAngle', 'Coord_Y', 'Coord_X', 'Speed_X', 'Speed_Y', 'Speed']
    for col in irrelevant_columns:
        if col in data.columns:
            data = data.drop(columns=col)


    # Filling missing values in 'Acceleration' with 0 and dropping rows where 'AdjSpeed' or 'MedianSpeed' is missing
    if 'Acceleration' in data.columns:
        data['Acceleration'] = data['Acceleration'].fillna(0)
    if 'AdjSpeed' in data.columns and 'MedianSpeed' in data.columns:
        data = data.dropna(subset=['AdjSpeed', 'MedianSpeed'])
    # Adding new columns for max and min values per ObjectID
    if 'ObjectID' in data.columns:
        group = data.groupby('ObjectID')
        data = data.join(group['AdjSpeed'].max().rename('Max_AdjSpeed'), on='ObjectID')
        data = data.join(group['MedianSpeed'].max().rename('Max_MedianSpeed'), on='ObjectID')
        data = data.join(group['Acceleration'].max().rename('Max_Acceleration'), on='ObjectID')

        min_abs_acceleration_per_object = data.groupby('ObjectID')['Acceleration'].apply(lambda x: abs(x.min())).rename(
            'Max_Deceleration')
        data = data.join(min_abs_acceleration_per_object, on='ObjectID')

        data = data.join(group['Height'].max().rename('Max_Height'), on='ObjectID')
        data = data.join(group['Width'].max().rename('Max_Width'), on='ObjectID')
        data = data.join(group['Area'].max().rename('Max_Area'), on='ObjectID')
        data = data.join(group['Height'].apply(lambda x: np.percentile(x, 90)).rename('90thP_Height'), on='ObjectID')
        data = data.join(group['Width'].apply(lambda x: np.percentile(x, 90)).rename('90thP_Width'), on='ObjectID')
        data = data.join(group['Area'].apply(lambda x: np.percentile(x, 90)).rename('90thP_Area'), on='ObjectID')
        data = data.join(group['Length'].apply(lambda x: np.percentile(x, 90)).rename('90thP_Length'), on='ObjectID')
    # Saving the cleaned and extended data to a new Excel file
    output_file_path = file_path.replace('.xlsx', '_processed.xlsx')
    data.to_excel(output_file_path, index=False)

    return output_file_path

# Example usage:
# file_path = r"C:\Users\Fei\Desktop\ForQC\Pecos\Pecos_All_labeled.xlsx"
# processed_file_path = process_traffic_data(file_path)
# print(f"Processed file saved at: {processed_file_path}")

file_path = r"D:\PythonFiles\Trajectory_ReClassfication\Test_LabeledTraj_McCarran_Keystone.xlsx"
processed_file_path = process_traffic_data(file_path)
print(f"Processed file saved at: {processed_file_path}")