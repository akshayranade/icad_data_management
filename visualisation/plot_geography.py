"""
This function plots geography using the latitude longitude data.
It shows the spread of facilities on Los Angeles map. 
And colour codes it based on the Avg number of violations by on that post code.

User provides year value as an input.

The functions reads already processed (aggregated data).
"""

#import libraries
import toml
import os
import pandas as pd
from matplotlib import *
import matplotlib.image as mpimg
import matplotlib.pyplot as plt

def plot_geography(yr):
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
    print("Loading aggregated data for lat/long from database.")
    try:
        geo = create_df_from_sql(config['postgres_credentials'],\
                                 config['db_details']['dbname'],\
                                 config['db_details']['viz_geography'])
        
    except:
        print("Data not prosent. Please check and prepare data.")
        return None
    
    #On successful data load into dataframe
    print("Aggregated data for lat/long load completed.")
    
    #Select df for the year selected by user
    try:
        sub_df = geo[geo['activity_yr'] == yr]

        #Change directory to parent
        os.chdir('..' + "\\" + "visualisation")

        fig, ax = plt.subplots(1, 1, figsize=(8,4))
        california_img=mpimg.imread('losangeles.gif')
        sub_df.plot(kind="scatter", x="location_long", y="location_lat", figsize=(10,6),
                    s=sub_df['num_facilities'], label="Violation/facility",
                    c="avg_violation_per_facility", cmap=plt.get_cmap("OrRd"),
                    colorbar=True, alpha=0.4, ax =ax
                   )
        plt.imshow(california_img, extent=[-119, -117.6, 33.5, 34.8], alpha=0.5)
        plt.ylabel("Latitude", fontsize=14)
        plt.xlabel("Longitude", fontsize=14)


        plt.legend(fontsize=8)
        fig.tight_layout()

        os.chdir(curr_dir)

        return fig
    
    except:
        print("Selected year value doesn't exist in data. Please chose different value.")
        os.chdir(curr_dir)
        return None

     
