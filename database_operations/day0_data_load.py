"""
This function creates a database in PostgresDB and loads the data from CSV files for - 
i. Inspections table
ii. violations table

The function expects that the csv files are stored in /Data location under parent directory
The csv files are expected to have consistent headers.

The function further processes the data to -
1. create the aggregate from violations table
2. Join the datasets to get the number of violations for every facility
"""
#Import libraries
import toml
import os
import pandas as pd

#import generic functions for database operations
from create_database_function import create_postgres_db
from create_table_function import create_postgres_table
from create_table_from_pandas_df_function import create_table_from_df
from create_df_from_sql_function import create_df_from_sql

#Function to create the pandas df from postgres table  
def load_data_day0():
    
    #get current working directory
    curr_dir = os.getcwd()
    print('Current Directory is: '+ curr_dir)
    
    #Change directory to parent
    os.chdir('..')
    wdir = os.getcwd()
    print('Current Directory is: '+ wdir)
    
    #Load configfile
    config = toml.load(wdir + "\config.toml")
    #print (config)
    print('Config file loaded successfully.')
    
    os.chdir(curr_dir)
    
    ## CREATE INSPECTIONS DATABASE IN POSTGRES IF NOT ALREADY EXISTING ##
    print('Creating database in Postgres.')
    #Call create database function to create a new database in postgres called 'inspection_db'
    create_postgres_db(config['postgres_credentials'], config['db_details']['dbname'])  
    
    
    ## LOAD INSPECTIONS DATA ##
    #Load data in Pandas dataframe so as to get the datatypes standardised
    print('Reading inspections data from raw csv file.')
    
    #Try reding data i a try.. except block to catch any issues with data file
    try:
        inspections = pd.read_csv(wdir + '\Data\Inspections.csv')

        #Rename dataframe columns in order to be standardised for Postgres schema
        inspections_col_rename = {
            'ACTIVITY DATE':'activity_date', 
            'OWNER ID':'owner_id', 
            'OWNER NAME':'owner_name', 
            'FACILITY ID':'facility_id',
            'FACILITY NAME':'facility_name', 
            'RECORD ID':'record_id', 
            'PROGRAM NAME':'program_name', 
            'PROGRAM STATUS':'program_status',
            'PROGRAM ELEMENT (PE)':'program_element_pe', 
            'PE DESCRIPTION':'pe_desc', 
            'FACILITY ADDRESS':'facility_address',
            'FACILITY CITY':'facility_city', 
            'FACILITY STATE':'facility_state', 
            'FACILITY ZIP':'facility_zip', 
            'SERVICE CODE':'service_code',
            'SERVICE DESCRIPTION':'service_desc', 
            'SCORE':'score', 
            'GRADE':'grade', 
            'SERIAL NUMBER':'serial_number', 
            'EMPLOYEE ID':'employee_id',
            'Location':'location', 
            '2011 Supervisorial District Boundaries (Official)':'supervisorial_district_boundaries',
            'Census Tracts 2010':'census_tracts_2010', 
            'Board Approved Statistical Areas':'board_approved_statistical_areas', 
            'Zip Codes':'zip_code'
            }
        print('Renaming columns of inspections data to standard nomenclature for Postgres.')
        inspections = inspections.rename(columns=inspections_col_rename)

        #Create a table for inspections using a generic function
        print('Creating a table in Postgres DB and loading data for inspections.')
        create_table_from_df(config['postgres_credentials'], config['db_details']['dbname'],\
                             config['db_details']['inspections_table'], inspections)    
        print('Data load for inspections data is completed.')
    
    except FileNotFoundError:
        print("Error: file not found.")
        return False
        
    except:
        print("Error: Please check your data file if it is in proper format and follows structure as expected.")
        return False
    
    ## LOAD VIOLATIONS DATA ##
    #Load data in Pandas dataframe so as to get the datatypes standardised
    print('Reading violations data from raw csv file.')
    
    #Try reding data i a try.. except block to catch any issues with data file
    try:
        violations = pd.read_csv(wdir + "\Data" + "\\" + "violations.csv")
        
        #Rename dataframe columns in order to be standardised for Postgres schema
        violations_col_rename = {
            'SERIAL NUMBER':'serial_number', 
            'VIOLATION  STATUS':'violation_status', 
            'VIOLATION CODE':'violation_code',
            'VIOLATION DESCRIPTION':'violation_desc', 
            'POINTS':'points'
            }

        print('Renaming columns of violations data to standard nomenclature for Postgres.')
        violations = violations.rename(columns=violations_col_rename)

        #Create a table for inspections using a generic function
        print('Creating a table in Postgres DB and loading data for violations.')
        create_table_from_df(config['postgres_credentials'], config['db_details']['dbname'],\
                             config['db_details']['violations_table'], violations)
        print('Data load for violations data is completed.')
    
    except FileNotFoundError:
        print("Error: file not found.")
        return False
        
    except:
        print("Error: Please check your data file if it is in proper format and follows structure as expected.")
        return False
        
    
    ## AGGREGATE VIOLATIONS DATA TO GET NUMBER OF VIOLATIONS PER SERIAL NUMBER ##
    #Using create_df_from_sql function create the pandas dataframe for violations table.
    #Pass connection config, dbname and table name to the function
    print('Reading violations data from Postgres DB.')
    violations_data = create_df_from_sql(config['postgres_credentials'],\
                                         config['db_details']['dbname'], config['db_details']['violations_table'])
    
    #Get the type of violation for every record (index value between # & .)
    char1 = '#'
    char2 = '.'
    violations_data['violation_type'] = violations_data['violation_desc'].apply(lambda x: x[x.find(char1)+1: x.find(char2)].strip())
    
    #Aggregate data to get counts and list of all violations for given serial number
    print('Aggregating violations data to get number of violations.')
    violations_aggregated = violations_data\
    .groupby('serial_number')\
    .agg({'violation_desc':'count', 'violation_type':lambda x: list(x)})\
    .reset_index()\
    .rename(columns={'violation_desc':'num_violations'})
    
    #Write back the violations aggregated data to postgres
    print('Creating a table in Postgres DB and loading data for violations aggregated.')
    create_table_from_df(config['postgres_credentials'], config['db_details']['dbname'],\
                         config['db_details']['violations_agg_table'],violations_aggregated)
    print('Data load for violations aggregated data is completed.')
    
    ##Join Inspections with violations aggregated to enrich the inspections data
    
    #Using create_df_from_sql function create the pandas dataframe for inspections table.
    #Pass connection config, dbname and table name to the function
    print('Reading inspections data from Postgres DB.')
    inspections_data = create_df_from_sql(config['postgres_credentials'],\
                                          config['db_details']['dbname'], config['db_details']['inspections_table'])
    
    #Using create_df_from_sql function create the pandas dataframe for violations aggregated table.
    #Pass connection config, dbname and table name to the function
    print('Reading violations aggregated data from Postgres DB.')
    violations_agg_data = create_df_from_sql(config['postgres_credentials'],\
                                             config['db_details']['dbname'], config['db_details']['violations_agg_table'])
    
    #Join 2 datasets to enrich inspections data with number of violations
    print('Joining inspections data and violations aggregated data to get number of violations per facility.')
    inspections_enriched = pd\
    .merge(inspections_data, violations_agg_data, on='serial_number', how='inner')
    
    #Write inspections enriched data to postgres table
    print('Creating a table in Postgres DB and loading data for inspections enriched.')
    create_table_from_df(config['postgres_credentials'], config['db_details']['dbname'],\
                         config['db_details']['inspections_enriched_table'], inspections_enriched)
    print('Data load for inspections enriched data is completed.')
    
    print('Data Load process from raw CSVs into PostgresDB completed.')
    
    return True