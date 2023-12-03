import pandas as pd
import os
import glob



def process_excel_v4(input_path, output_path):
    # Read the input Excel file
    df = pd.read_excel(input_path)
    # Convert 'DateTime' column to datetime format
    df['DateTime'] = pd.to_datetime(df['DateTime'])
    # Replace '<Null>' values in 'MedianSpeed' with NaN and convert the column to float
    df['MedianSpeed'] = df['MedianSpeed'].replace('<Null>', float('nan')).astype(float)
    # Extract the numeric portion before 'sub' in NewObjectID
    df['NumericID'] = df['NewObjectID'].str.split('sub').str[0]
    df['Max_Length'] = df['Max_Length'].replace('<Null>', float('nan')).astype(float)
    # Construct the new ID
    df['New_ID'] = df['NumericID'] + '-' + df['DateTime'].dt.strftime('%Y-%m-%d-%H%M')
    # Group by 'NewObjectID' and compute the required values
    grouped = df.groupby('NewObjectID').agg(
        Global_ID=('New_ID', 'first'),
        Local_ID=('NumericID', 'first'),
        DateTime_min=('DateTime', 'min'),
        Class=('Class', 'first'),
        MedianSpeed_mean=('MedianSpeed', 'mean'),
        MedianSpeed_max=('MedianSpeed', 'max'),
        NoOfPoints=('NewObjectID', 'size'),
        MaxLength_min = ('Max_Length', 'min'),
        MaxLength_max = ('Max_Length', 'max')
    ).reset_index(drop=True)
    # Extract the site name from the file name
    site_name = os.path.splitext(os.path.basename(input_path))[0]
    grouped['Site'] = site_name
    # Rename columns as per requirement
    grouped.columns = ['New_ID', 'NumericID', 'DateTime', 'Class', 'Avg_MedianSpeed', 'Max_MedianSpeed',
                       'No_Points', 'Max_Length_Min','Max_Length_Max ', 'Site']
    # Write to the output Excel file
    grouped.to_excel(output_path, index=False)
    # Usage example:
    # output_path_v4 = "path_to_save_processed_file.xlsx"
    # process_excel_v4("path_to_input_file.xlsx", output_path_v4)
    return grouped


def process_excel_v5(input_path, reference_path, output_path):
    # Read the input Excel file
    df = pd.read_excel(input_path)
    reference_df = pd.read_excel(reference_path)  # Read the reference Excel file for NoOfRows

    # Convert 'DateTime' column to datetime format
    df['DateTime'] = pd.to_datetime(df['DateTime'])

    # Replace '<Null>' values in 'MedianSpeed' and 'Max_Length' with NaN and convert the columns to float
    df['MedianSpeed'] = df['MedianSpeed'].replace('<Null>', float('nan')).astype(float)
    df['Max_Length'] = df['Max_Length'].replace('<Null>', float('nan')).astype(float)

    # Extract the numeric portion before 'sub' in NewObjectID
    df['NumericID'] = df['NewObjectID'].str.split('sub').str[0]

    # Construct the new ID
    df['New_ID'] = df['NumericID'] + '-' + df['DateTime'].dt.strftime('%Y-%m-%d-%H%M')

    # Compute the NoOfRows from the reference dataframe
    no_of_rows = reference_df.groupby('NewObjectID').size()

    # Group by 'NewObjectID' and compute the required values
    grouped = df.groupby('NewObjectID').agg(
        Global_ID=('New_ID', 'first'),
        Local_ID=('NumericID', 'first'),
        DateTime_min=('DateTime', 'min'),
        Class=('Class', 'first'),
        MedianSpeed_mean=('MedianSpeed', 'mean'),
        MedianSpeed_max=('MedianSpeed', 'max'),
        MaxLength_min=('Max_Length', 'min'),
        MaxLength_max=('Max_Length', 'max')
    ).reset_index()

    # Merge with the no_of_rows Series to get the NoOfRows for each NewObjectID
    grouped = grouped.merge(no_of_rows.rename('NoOfRows'), left_on='NewObjectID', right_index=True)

    # Extract the site name from the file name
    site_name = os.path.splitext(os.path.basename(input_path))[0]
    grouped['Site'] = site_name

    # Rename columns as per requirement
    grouped.columns = ['NewObjectID', 'New_ID', 'NumericID', 'DateTime', 'Class', 'Avg_MedianSpeed', 'Max_MedianSpeed',
                       'Max_Length_Min', 'Max_Length_Max', 'NoOfRows', 'Site']

    # Write to the output Excel file
    grouped.to_excel(output_path, index=False)

    return grouped


def process_excel_v6(input_path, reference_files, output_path):
    # Read the input Excel file
    df = pd.read_excel(input_path)

    # Convert 'DateTime' column to datetime format
    df['DateTime'] = pd.to_datetime(df['DateTime'])

    # Replace '<Null>' values in 'MedianSpeed' and 'Max_Length' with NaN and convert the columns to float
    df['MedianSpeed'] = df['MedianSpeed'].replace('<Null>', float('nan')).astype(float)
    df['Max_Length'] = df['Max_Length'].replace('<Null>', float('nan')).astype(float)

    # Extract the numeric portion before 'sub' in NewObjectID
    df['NumericID'] = df['NewObjectID'].str.split('sub').str[0]

    # Construct the new ID
    df['New_ID'] = df['NumericID'] + '-' + df['DateTime'].dt.strftime('%Y-%m-%d-%H%M')

    # Combine all reference files into a single dataframe
    all_ref_dfs = [pd.read_excel(ref_file) for ref_file in reference_files]
    combined_ref_df = pd.concat(all_ref_dfs, ignore_index=True)

    # Compute the NoOfRows from the combined reference dataframe
    no_of_rows = combined_ref_df.groupby('NewObjectID').size()

    # Group by 'NewObjectID' and compute the required values
    grouped = df.groupby('NewObjectID').agg(
        Global_ID=('New_ID', 'first'),
        Local_ID=('NumericID', 'first'),
        DateTime_min=('DateTime', 'min'),
        Class=('Class', 'first'),
        MedianSpeed_mean=('MedianSpeed', 'mean'),
        MedianSpeed_max=('MedianSpeed', 'max'),
        MaxLength_min=('Max_Length', 'min'),
        MaxLength_max=('Max_Length', 'max')
    ).reset_index()

    # Merge with the no_of_rows Series to get the NoOfRows for each NewObjectID
    grouped = grouped.merge(no_of_rows.rename('NoOfRows'), left_on='NewObjectID', right_index=True)

    # Extract the site name from the file name
    site_name = os.path.splitext(os.path.basename(input_path))[0]
    grouped['Site'] = site_name

    # Rename columns as per requirement
    grouped.columns = ['NewObjectID', 'New_ID', 'NumericID', 'DateTime', 'Class', 'Avg_MedianSpeed', 'Max_MedianSpeed',
                       'Max_Length_Min', 'Max_Length_Max', 'NoOfRows', 'Site']

    # Write to the output Excel file
    grouped.to_excel(output_path, index=False)

    return grouped


# Example usage:
# reference_files_list = ["path_to_reference_file1.xlsx", "path_to_reference_file2.xlsx", ...]
# output_path_v6 = "path_to_save_processed_file.xlsx"
# process_excel_v6("path_to_input_file.xlsx", reference_files_list, output_path_v6)


select_path = r"C:\Users\Fei\Desktop\ForQC\Maryland\Maryland_MM_selected.xlsx"
ref_path = glob.glob(r'C:\Users\Fei\Desktop\ForQC\Maryland\Maryland_MM_ALL*.xlsx')
output_path = r"C:\Users\Fei\Desktop\ForQC\Maryland\Maryland_MM.xlsx"
process_excel_v6(select_path, ref_path, output_path)



