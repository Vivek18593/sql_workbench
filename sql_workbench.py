import mysql.connector
from mysql.connector import Error
import pandas as pd

# Establishing Server Connection
def create_server_connection(hostname, username, password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host = hostname,
            user = username,
            passwd = password
        )
        print('MySQL Database Server Connection Successful')
    except Error as err:
        print(f"Error: {err}")
    return connection

# Creating Database
def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database Created Successfully")
    except Error as err:
        print(f"Error: {err}")

# Connect To Database
def create_db_connection(hostname, username, password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host = hostname,
            user = username,
            passwd = password,
            database = db_name
        )
        print("MySQL Database Connection Successful")
    except Error as err:
        print(f"Error: {err}")
    return connection

# Execute SQL Queries
def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print('Query Executed')
    except Error as err:
        print(f"Error: {err}")

# Read Data
def read_query(connection, query, table_name):
    global column_names
    cursor = connection.cursor()
    table_data = None
    try:
        cursor.execute(f'SHOW COLUMNS FROM {table_name}')
        table_columns = cursor.fetchall()
        columns = []
        for res in table_columns:
            table_columns = list(res)
            columns.append(table_columns)        
        column_names = []
        for n in range(len(columns)):
            column_names.append(columns[n][0])

        cursor.execute(query)
        table_data = cursor.fetchall()
        return table_data
    except Error as err:
        print(f'Error: {err}')

# Display Data (Creating Dataframe Using Pandas)
def display_data(result):
    from_db = []
    for res in result:
        result = list(res)
        from_db.append(result)
    df = pd.DataFrame(from_db, columns = column_names)  
    print(df)

#---------------------------------------------------------------------------------------------------------------------------#
#---------------------------------------------------------------------------------------------------------------------------#

# EXECUTE
status = True
while status:

    print('\n--- Establishing Server Connection ---')
    connect_server = True
    while connect_server:
        hname = input('Enter Hostname: ')
        uname = input('Enter Username: ')
        pword = input('Enter Password: ')
        try:
            server_connection = create_server_connection(hname, uname, pword)
            if server_connection != None:
                connect_server = False
        except Exception as ex:
            print(ex)

    print('\n--- Create Database ---')
    create_db = True
    while create_db:
        check = input('Do you want to create a new database? (Y or N): ').lower()
        try:
            if check == 'y':
                create_database_query = input('Query to create database: ')
                create_database(server_connection, create_database_query)
                create_db = False
            elif check == 'n':
                create_db = False
        except:
            pass

    print('\n--- Connect To Database ---')
    connect_db = True
    while connect_db:
        dbname = input('Enter DB Name: ')
        try:
            if dbname == '' or dbname == ' ':
                print('DB Name Cannot Be Empty/Null..!!')
                connect_db = True
            else:
                db_connection = create_db_connection(hname, uname, pword, dbname)
                if db_connection != None:
                    connect_db = False
        except Exception as ex:
            print(ex)

    print('\n--- Execute SQL Queries ---')
    exe_query = True
    while exe_query:
        query = input("Enter Query or Type 'Quit': ").lower()      
        query_list = ['crea','inse','dele','trun','drop','upda']
        if query[0:4] in query_list:
            execute_query(db_connection, query)
            exe_query = True
        elif query == 'quit':
            exe_query = False
        else:
            table_name = input('Enter Table Name: ').lower()
            try:
                result = read_query(db_connection, query, table_name)
                display_data(result)
                exe_query = True
            except Exception as ex:
                print(ex)

    status = False
    print('\nExited Server Connection.!')    
        
    
