"""
This function creates database in postgres.
The function excepts inputs- 
1. a dictionary of configs to connect to postgres 
2. name of database to be created
"""
#Import libraries
import psycopg2
from psycopg2 import sql, Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

#Function to create the postgres database
def create_postgres_db(connection_config, dbname):
    #Try block to connect to the postgres server and if successful connection, create database
    try:
        # Connect to an existing postgres database
        connection = psycopg2.connect(user=connection_config['username'],
                                      password=connection_config['password'],
                                      host=connection_config['host'],
                                      port=connection_config['port'])
    
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT) 

        # Create a cursor to perform database operations
        cur = connection.cursor()
            
        # Use the psycopg2.sql to delete database first if exists
        # Try creating a database. If already exists, catch an exception 
        try:
            cur.execute(sql.SQL("create database {};").format(sql.Identifier(dbname)))
            print('New database created: ' + dbname)
        
        #If the database already exists, delete it first and then create a new one.
        except (Exception, Error) as error:
            #cur.execute(sql.SQL("drop database {};").format(sql.Identifier(dbname)))
            #cur.execute(sql.SQL("create database {};").format(sql.Identifier(dbname)))
            #print('Existing database deleted and a new one is created:  ' + dbname)
            print('Database already exists:  ' + dbname)
    
    #Catch exception for connection issues
    except (Exception, Error) as error:
        print("Error: ", error)

    #Close the connection before exiting
    finally:
        if (connection):
            cur.close()
            connection.close()
            print("PostgreSQL connection is closed")