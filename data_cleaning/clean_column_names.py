import pandas as pd
import re
#Function to standardise column names
def clean_column_names(df):
    cols_rename_dict = {}
    for cols in df.columns:
        #Trim and lower case
        clean_col_name = str(cols).strip().lower()
        #Remove non alphanumeric characters
        clean_col_name = re.sub(r'[^A-Za-z0-9_ ]+', '', clean_col_name)
        #Replace spaces with '_'
        clean_col_name = re.sub(' ', '_', clean_col_name)
        #Add clean column name to dict
        cols_rename_dict[cols] = clean_col_name
        #Replace column names
        df = df.rename(columns=cols_rename_dict)
        
    return df