"""
This function creates table in postgres in a given a database from a pandas dataframe.
The function excepts inputs- 
1. a dictionary of configs to connect to postgres 
2. name of database
3. table name to be created
4. df
"""
#Import libraries
import psycopg2
from psycopg2 import sql, Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine

#Function to create the postgres table from pandas df
def create_table_from_df(connection_config, dbname, tbname, df):
    
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
        
        #create a table from the input df.
        #If exists, replace it.
        #chunksize set random to 1000
        df.to_sql(tbname, engine, if_exists='replace', index=False, chunksize=1000)
        print('Table successfully created: ' + tbname)


    except(Exception, Error) as error:
        print("Error :", error)
