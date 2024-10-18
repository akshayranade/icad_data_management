"""
This function performs Initial preparation tasks including:
1. Drop row level duplicates
2. Treat missing values
3. Change Data types to appropriate ones

The function is generic and will work with any type of input data frame having specific column structure
"""

#Import libraries
import pandas as pd
import csv
import string
import os
from pandas.api.types import is_numeric_dtype
import re
import numpy as np


def data_processing(df):
    #remove row level duplicates 
    df = df.drop_duplicates()

    #Change data types for activity date and zip codes
    df = df.dropna()
    df['activity_date'] = pd.to_datetime(df['activity_date'])
    df['zip_code'] = df['zip_code'].astype('Int64')
    df['facility_state'] = df['facility_state'].str.upper()
    
    df['location_clean'] = df['location']\
    .apply(lambda x: str(x).replace('POINT', '').replace('(', '').replace(')', '').split())

    df['location_long'] = df['location_clean']\
    .apply(lambda x: x[0]).astype(np.float64)

    df['location_lat'] = df['location_clean']\
    .apply(lambda x: x[1]).astype(np.float64)

    #Create a column 'activity year'  from 'activity date'
    df['activity_yr'] = pd.DatetimeIndex(df['activity_date']).year

    #Remove information from 'PE DESCRIPTION’ column 
    char1 = '('
    char2 = ')'
    df['seating_capacity'] = df['pe_desc'].apply(lambda x: x[x.find(char1): x.find(char2)+1].strip())
    df['pe_desc'] = df.apply(lambda x: x['pe_desc'].replace(x['seating_capacity'], ''), axis=1)

    #Remove the records where program status = 'INACTIVE'
    df = df[df['program_status'] != 'INACTIVE']

    #Drop rows with missing values
    #df = df.dropna(subset=['seating capacity', 'Zip Codes'])
    df = df[df['seating_capacity']!= '']
    
    return df