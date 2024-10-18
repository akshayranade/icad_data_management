"""
This function plots number of violations by number of facilities for a given year.

User provides year value as an input.

The functions reads already processed (aggregated data).
"""

#import libraries
import toml
import os
import pandas as pd
from matplotlib import *
import matplotlib.pyplot as plt

def plot_by_num_of_violations(yr):
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
    print("Loading cleaned aggregated data by number of violations from database.")
    try:
        violation_num = create_df_from_sql(config['postgres_credentials'],\
                                           config['db_details']['dbname'],\
                                           config['db_details']['viz_num_violations'])
        
    except:
        print("Data not prosent. Please check and prepare data.")
        return None
    
    #On successful data load into dataframe
    print("Aggregated data by number of violations load completed.")
    
    #Select df for the year selected by user
    try:
        sub_df = violation_num[violation_num['activity_yr'] == yr][['num_violations', 'num_facility']]\
        .sort_values('num_facility', ascending=False)\
        .head(30)
        
        fig, ax = plt.subplots(1, 1, figsize=(8,4))
        sub_df[['num_violations', 'num_facility']].plot(x='num_violations', kind='bar', ax=ax)
        plt.xlabel('Number of Violations')
        plt.ylabel('Number of Facilities')
        ax.set_title('By number of violations: year {}'.format(yr))
        fig.tight_layout()

        return fig    
    
    except:
        print("Selected year value doesn't exist in data. Please chose different value.")
        return None
        
     