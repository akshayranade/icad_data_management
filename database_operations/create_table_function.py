"""
This function creates table in a database in postgres.
The function excepts inputs- 
1. a dictionary of configs to connect to postgres 
2. name of database
3. table name to be created
4. schema of the table to be created
"""
#Import libraries
import psycopg2
from psycopg2 import sql, Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

#Function to create the postgres table
def create_postgres_table(connection_config, dbname, tbname, schema):
    #Try block to connect to the postgres server and if successful connection, create database
    try:
        # Connect to an existing postgres database
        connection = psycopg2.connect(user=connection_config['username'],
                                      password=connection_config['password'],
                                      host=connection_config['host'],
                                      port=connection_config['port'],
                                      dbname=dbname
                                     )
    
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT) 

        # Create a cursor to perform database operations
        cur = connection.cursor()
        
        #build a create table query
        create_query = 'create table '+ tbname + ' (' + schema + ')'
        
        # Use the psycopg2.sql to delete database first if exists
        # Try creating a table. If already exists, catch an exception 
        try:
            cur.execute(create_query)
            print('Created a new table: ' + tbname)
        
        #If the table already exists, delete it first and then create a new one.
        except (Exception, Error) as error:
            cur.execute(sql.SQL("drop table if exists {};").format(sql.Identifier(tbname)))
            cur.execute(create_query)            
            print('Existing table deleted and a new one is created: ' + tbname)
    
    #Catch exception for connection issues
    except (Exception, Error) as error:
        print("Error :", error)

    #Close the connection before exiting
    finally:
        if (connection):
            cur.close()
            connection.close()
            print("PostgreSQL connection is closed")
