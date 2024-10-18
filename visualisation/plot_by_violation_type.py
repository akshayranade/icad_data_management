"""
This function plots number of facilities per type of violation for a given year.

User provides year value as an input.

The functions reads already processed (aggregated data).
"""

#import libraries
import toml
import os
import pandas as pd
from matplotlib import *
import matplotlib.pyplot as plt

def plot_by_violation_type(yr):
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
    print("Loading cleaned aggregated data by violation type from database.")
    try:
        violation_type = create_df_from_sql(config['postgres_credentials'],\
                                            config['db_details']['dbname'],\
                                            config['db_details']['viz_violation_type'])
        
    except:
        print("Data not prosent. Please check and prepare data.")
        return None
    
    #On successful data load into dataframe
    print("Aggregated data by violation type load completed.")
    
    #Select df for the year selected by user
    try:
        sub_df = violation_type[violation_type['activity_yr'] == yr][['violation_type', 'number_of_facilities']]\
        .sort_values('number_of_facilities', ascending=False)\
        .head(30)

        
        #Plot using matlab bar plot and return fig
        fig, ax = plt.subplots(1, 1, figsize=(8,4))
        sub_df[['violation_type', 'number_of_facilities']].plot(x='violation_type', kind='bar', ax=ax)
        plt.xlabel('Violation Type')
        plt.ylabel('Number of Facilities')
        ax.set_title('By type of violations: year {}'.format(yr))
        fig.tight_layout()

        return fig    
    
    except:
        print("Selected year value doesn't exist in data. Please chose different value.")
        return None

     
