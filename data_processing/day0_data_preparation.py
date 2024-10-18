"""
This function cleans and processes the raw inspections data asper the client brief to - 
1. Drop row level duplicates
2. Treat missing values
3. Change Data types to appropriate ones
4. Data Selection as per brief
5. Data manipulation as briefed by client

This function also evaluates central tendencies for the inspection score per year:
a. For each type of vendor’s seating tyoe
b. For each ‘zip code’
c. seating capacity

All the statictics are loaded in database so as to refer back to.

"""
#Import libraries
import toml
import os
import pandas as pd
import numpy as np

#import generic functions
from central_tendancies_function import central_tendancies
from data_processing_function import data_processing


#Function to create the pandas df from postgres table  
def prepare_data_day0():
    #get current working directory
    curr_dir = os.getcwd()
    
    #Change directory to parent
    os.chdir('..')
    wdir = os.getcwd()
    
    #Load configfile
    config = toml.load(wdir + "\config.toml")
    
    #Change directory to parent
    os.chdir(wdir + "\database_operations")
    wdir = os.getcwd()

    #import generic functions for database operations
    from create_database_function import create_postgres_db
    from create_table_function import create_postgres_table
    from create_table_from_pandas_df_function import create_table_from_df
    from create_df_from_sql_function import create_df_from_sql
    
    #Change back to current working directory from where the function is called
    os.chdir(curr_dir)
    
    #Load inspections enriched data to clean & process
    #Using create_df_from_sql function create the pandas dataframe for inspections table.
    #Pass connection config, dbname and table name to the function
    print("Loading inspections enriched data from database.")
    
    try:
        inspections = create_df_from_sql(config['postgres_credentials'],\
                                         config['db_details']['dbname'],\
                                         config['db_details']['inspections_enriched_table'])
        print("Inspections data load completed.")
    except:
        print("Error: Couldn't create data frame from table. Please check the table or load data again.")
        return False
    
    # ***
    # Call function data_processing to clean the data, including:
    # 1. Drop row level duplicates
    # 2. Treat missing values
    # 3. Change Data types to appropriate ones
    # ***
    print("Treating inspections data to clean and process as per client's brief.")
    inspections_treated = data_processing(inspections)
    print('Data cleaning and processing completed.')
    
    #Using create_table_from_df function load the cleaned data into database
    print('Creating a table in Postgres DB and loading inspections cleaned data.')
    create_table_from_df(config['postgres_credentials'], config['db_details']['dbname'],\
                         config['db_details']['inspections_cleaned_table'], inspections_treated)    
    print('Data load for inspections cleaned data is completed.')
    
    #Using create_df_from_sql function create the pandas dataframe for inspections cleaned table to .
    print("Loading cleaned inspections data from database.")
    inspections_clean_data = create_df_from_sql(config['postgres_credentials'],\
                                     config['db_details']['dbname'],\
                                     config['db_details']['inspections_cleaned_table'])
    print("Inspections cleaned data load completed.")
    
    
    # ***
    # Call function central_tendancies to get the mean, median, mode of score for the inspection score per year:
    # 1. For each ‘zip code’
    # ***
    inputAtt = ['zip_code', 'activity_yr']
    targetAtt='score'
    df = inspections_clean_data
    
    print('Calculating central tendencies for the inspection score per year for zip code')
    score_by_zip = central_tendancies(df, inputAtt, targetAtt)
    
    #Load the central tendencies by zip code in database
    print('Loading in database')
    create_table_from_df(config['postgres_credentials'], config['db_details']['dbname'],\
                         config['db_details']['zip_statistics_table'], score_by_zip)    
    print('Data load for central tendencies data by zip is completed.')
    
    # ***
    # Call function central_tendancies to get the mean, median, mode of score for the inspection score per year:
    # 2. For each type of vendor’s seating
    # ***
    inputAtt = ['pe_desc', 'activity_yr']
    targetAtt='score'
    df = inspections_clean_data

    score_by_seating_type = central_tendancies(df, inputAtt, targetAtt)
    
    #Load the central tendencies by zip code in database
    print('Loading in database')
    create_table_from_df(config['postgres_credentials'], config['db_details']['dbname'],\
                         config['db_details']['seating_type_statistics_table'], score_by_seating_type)    
    print('Data load for central tendencies data by type of vendor seating is completed.')
    
    
    # ***
    # Call function central_tendancies to get the mean, median, mode of score for the inspection score per year:
    # 2. For each type of vendor’s seating
    # ***
    inputAtt = ['seating_capacity', 'activity_yr']
    targetAtt='score'
    df = inspections_clean_data

    score_by_seating_capacity = central_tendancies(df, inputAtt, targetAtt)
    
    #Load the central tendencies by zip code in database
    print('Loading in database')
    create_table_from_df(config['postgres_credentials'], config['db_details']['dbname'],\
                         config['db_details']['seating_capacity_statistics_table'], score_by_seating_capacity)    
    print('Data load for central tendencies data by seating capacity is completed.')
    
    ###Aggregations for visualisations
    #Number of facility against number of violations
    viz_num_violations = inspections_clean_data\
    .groupby(['activity_yr', 'num_violations'])['facility_id']\
    .nunique()\
    .reset_index()\
    .rename(columns={'facility_id':'num_facility'})
    
    #Write into postgres db
    print('Loading in database - visualisation aggregation by number of violations')
    create_table_from_df(config['postgres_credentials'], config['db_details']['dbname'],\
                         config['db_details']['viz_num_violations'], viz_num_violations)    
    print('Data load for visualisation aggregation by number of violations is completed.')
    
    
    # Number of facilities against type of violations
    type_of_vio_yr = inspections_clean_data[['activity_yr', 'facility_id', 'violation_type']]
    #Create a list of violations per facility
    type_of_vio_yr['violation_tp_list'] = type_of_vio_yr['violation_type'].apply(lambda x: x.replace('{', '').replace('}','').split(','))
    type_of_vio_yr = type_of_vio_yr[['activity_yr', 'facility_id', 'violation_tp_list']]

    #Stack the dataframe to get row for each element in violatios list
    type_of_vio_stacked = type_of_vio_yr.set_index(['activity_yr', 'facility_id']).violation_tp_list\
    .apply(pd.Series).stack().reset_index()\
    .drop('level_2', axis=1)
    #Set column names
    type_of_vio_stacked.columns = ['activity_yr', 'facility_id', 'violation_type']

    #Aggregate to get number of facilities per violation type
    fac_per_vio_tp = type_of_vio_stacked.groupby(['activity_yr', 'violation_type'])['facility_id'].nunique().reset_index()\
    .rename(columns={'facility_id':'number_of_facilities'})
    
    #Write into postgres db
    print('Loading in database - visualisation aggregation by type of violations')
    create_table_from_df(config['postgres_credentials'], config['db_details']['dbname'],\
                         config['db_details']['viz_violation_type'], fac_per_vio_tp)    
    print('Data load for visualisation aggregation by type of violations is completed.')
    
    #Geographical visualisation
    geo = inspections_clean_data.groupby(['activity_yr', 'location_lat', 'location_long'])['num_violations']\
    .agg({'mean', 'count'})\
    .reset_index()\
    .rename(columns={'mean':'avg_violation_per_facility', 'count':'num_facilities'})
    
    #Write into postgres db
    print('Loading in database - visualisation aggregation for geography')
    create_table_from_df(config['postgres_credentials'], config['db_details']['dbname'],\
                         config['db_details']['viz_geography'], geo)    
    print('Data load for visualisation aggregation for geography is completed.')
    
    
    #Visualisation by Zip Code
    zip_code = inspections_clean_data.groupby(['activity_yr', 'zip_code'])['num_violations']\
    .agg({'mean', 'count'})\
    .reset_index()\
    .rename(columns={'mean':'avg_violation_per_facility', 'count':'num_facilities'})
    
    #Write into postgres db
    print('Loading in database - visualisation aggregation by zip code')
    create_table_from_df(config['postgres_credentials'], config['db_details']['dbname'],\
                         config['db_details']['viz_zip_code'], zip_code)    
    print('Data load for visualisation aggregation by zip code is completed.')
    
    
    print('Process for data cleaning and preparation is completed.')
    
    return True