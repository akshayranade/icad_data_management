"""
This function plots avg violations per facility by zip code for a given year. 
And returns top 30 zip code with highest number of violations per vendor 

User provides year value as an input.

The functions reads already processed (aggregated data).
"""

#import libraries
import toml
import os
import pandas as pd
from matplotlib import *
import matplotlib.pyplot as plt

def plot_by_zip_code(yr):
    #get current working directory
    curr_dir = os.getcwd()
    print("current directory is :" + curr_dir) 
    
    #Change directory to parent
    os.chdir('..')
    wdir = os.getcwd()
    
    #Load configfile
    config = toml.load(wdir + "\config.toml")
    
    #Change directory to parent
    os.chdir(wdir + "\database_operations")
    wdir = os.getcwd()

    #import generic functions for database operations
    from create_df_from_sql_function import create_df_from_sql
    
    #Using create_df_from_sql function create the pandas dataframe for aggregated data.
    print("Loading cleaned aggregated data for violations by zip code from database.")
    try:
        violation_zip = create_df_from_sql(config['postgres_credentials'],\
                                           config['db_details']['dbname'],\
                                           config['db_details']['viz_zip_code'])
        
    except:
        print("Data not prosent. Please check and prepare data.")
        return None
    
    #On successful data load into dataframe
    print("Aggregated data by for violations by zip code load completed.")
    
    #Select df for the year selected by user
    try:
        sub_df = violation_zip[violation_zip['activity_yr'] == yr][['zip_code', 'avg_violation_per_facility', 'num_facilities']]\
        .sort_values('avg_violation_per_facility', ascending=False)\
        .head(30)
        
        fig, ax = plt.subplots(1, 1, figsize=(8,4))
        sub_df[['zip_code', 'avg_violation_per_facility']].plot(x='zip_code', kind='bar', ax=ax)
        plt.xlabel('Zip code')
        plt.ylabel('Avg Violations per facility')
        ax.set_title('Violations by Zip code: year {}'.format(yr))
        fig.tight_layout()

        return fig    
    
    except:
        print("Selected year value doesn't exist in data. Please chose different value.")
        return None

     
