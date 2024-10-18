"""
This function creates a Pandas Dataframe from a postgres table.
The function excepts inputs- 
1. a dictionary of configs to connect to postgres 
2. name of database
3. table name to be read
"""
#Import libraries
import pandas as pd
from sqlalchemy import create_engine

#Function to create the pandas df from postgres table  
def create_df_from_sql(connection_config, dbname, tbname):
    
    try:
        #Define a database connection url
        db_connection_url = "postgresql://{}:{}@{}:{}/{}".format(
            connection_config['username'],
            connection_config['password'],
            connection_config['host'],
            connection_config['port'],
            dbname
        )
    
        #Create engine for pandas to connect to the 
        engine = create_engine(db_connection_url)
        
        #create a df from given table.
        df = pd.read_sql_table(tbname, engine)
        
        #Return the df
        return df


    except(Exception, Error) as error:
        print("Error :", error)
