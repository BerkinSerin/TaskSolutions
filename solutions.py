#!/usr/bin/env python
# coding: utf-8


# pip install -r requirements.txt

# import libraries
from lxml import etree
import sqlite3
import pandas as pd
import psycopg2
import sqlalchemy
import sys
import re


def postgresql_conversion(conn_string, db_file, port):
    """
    migrate sqlite db to postgres by converting tables
    into CSVs. If table exists, don't create again.
    :params:
    conn_string: connection string for postgresql connection,
    db_file: sqlite db file to be migrated
    port: port value for conn_string (default 5433)
    :returns: user, password, host, db_name
    """
    # "postgresql://postgres:postgres@localhost:5433/postgres"
    user, password, host = re.match(
        'postgresql://(.*?):(.*?)@(.*?)', conn_string).groups()
    # database name is automatically parsed as the sqlite db name e.g. transactions
    db_name = db_file.split('.')[0]
    # convert all tables to CSV
    db = sqlite3.connect(db_file)  # transactions.db
    cursor = db.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    for table_name in tables:
        table_name = table_name[0]
        if 'sqlite' not in table_name:
            table = pd.read_sql_query("SELECT * from %s" % table_name, db)
            table.to_csv(table_name + '.csv', index=False)
    cursor.close()
    db.close()
    # establishing the connection INITIAL CONNECTION IS MADE TO THE POSTGRES DATABASE!
    conn = psycopg2.connect(
        database='postgres', user=user, password=password, host=host, port=port
    )
    conn.autocommit = True

    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()

    # Preparing query to create a database
    cursor.execute(
        f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{db_name}'")
    exists = cursor.fetchone()
    if not exists:  # If table does not already exist in the db
        sql = f'''CREATE DATABASE {db_name};'''
        # Creating a database
        cursor.execute(sql)
        print(f"Database {db_name} created successfully........")
        # Closing the connection
        conn.close()
        engine = sqlalchemy.create_engine(
            conn_string+':'+port+'/'+db_name, echo=False)
        df = pd.read_csv("Devices.csv")
        df.to_sql("Devices", con=engine, index=False)
        df2 = pd.read_csv("Transactions.csv")
        df2.to_sql("Transactions", con=engine, index=False,
                   dtype={'datetime': sqlalchemy.DateTime})
        engine.dispose()
    return user, password, host, db_name


def create_connection(sql_type, db_file, conn_string=''):

    if sql_type == 'sqlite':

        """ create a database connection to the SQLite database or PostgreSQL
            specified by the db_file
        :params: 
        db_file: database file
        sql_type: postgresql or sqlite
        conn_string: connection string for postgresql connection
        :return: cursorObj, conn
        """
        conn = None
        try:
            conn = sqlite3.connect(db_file)
        except Error as e:
            print(e)
        cursorObj = conn.cursor()
        return cursorObj, conn
    elif sql_type == 'postgresql':
        user, password, host, db_name = postgresql_conversion(
            conn_string, db_file, port='5433')
        # establishing the connection
        conn = psycopg2.connect(
            # to easily write sql queries
            database=db_name, user=user, password=password, host=host, port='5433', options="-c search_path=public"
        )
        # Creating a cursor object using the cursor() method
        cursorObj = conn.cursor()
        return cursorObj, conn


# 3189 Total unique visitors in the database.


# TASK1 Write a Python script to find out which visitor created the most revenue.
def task1(cursorObj, sql_type):
    """
    Solution for task1
    :params:
    cursorObj: Cursor object for connection,
    sql_type: postgresql or sqlite
    """
    most_revenue_query = 'SELECT visitor_id, SUM(revenue) total_revenue FROM Transactions GROUP BY visitor_id ORDER BY 2 DESC LIMIT 1'
    if sql_type == 'sqlite':
        cursorObj.execute(most_revenue_query)
    else:
        most_revenue_query = most_revenue_query.replace(
            'FROM Transactions', 'FROM "Transactions"')
        cursorObj.execute(most_revenue_query)
    result = cursorObj.fetchall()
    print(
        f'Visitor id with the most revenue is: {result[0][0]} and their total revenue is: {result[0][1]}\n')


# TASK2 Write a Python script to find out on which day most revenue for users who ordered via a mobile phone was created.
def task2(cursorObj, sql_type):
    """
    Solution for task2
    :params:
    cursorObj: Cursor object for connection,
    sql_type: postgresql or sqlite
    """
    most_revenue_day_query = "SELECT strftime('%Y-%m-%d', t.datetime), sum(t.revenue) total_revenue FROM Transactions t JOIN Devices d ON t.device_type = d.id WHERE d.device_name = 'Mobile Phone' GROUP BY strftime('%Y-%m-%d', `datetime`) ORDER BY total_revenue DESC LIMIT 1"
    if sql_type == 'sqlite':
        cursorObj.execute(most_revenue_day_query)
    else:
        most_revenue_day_query = most_revenue_day_query.replace(
            'FROM Transactions', 'FROM "Transactions"')
        most_revenue_day_query = most_revenue_day_query.replace(
            'JOIN Devices', 'JOIN "Devices"')
        most_revenue_day_query = most_revenue_day_query.replace(
            "strftime('%Y-%m-%d', t.datetime)", "TO_CHAR(t.datetime, 'YYYY-MM-DD')")
        most_revenue_day_query = most_revenue_day_query.replace(
            "strftime('%Y-%m-%d', `datetime`)", "TO_CHAR(datetime, 'YYYY-MM-DD')")
        cursorObj.execute(most_revenue_day_query)
    result = cursorObj.fetchall()
    print(
        f'Day with the most revenue is: {result[0][0]} and the total revenue is: {result[0][1]}\n')


# TASK3 Write a Python script that combines the contents of Devices and Transactions and store it as a single flat file including the column names.
def task3(cursorObj, sql_type, con):
    """
    Solution for task3
    :params:
    cursorObj: Cursor object for connection,
    sql_type: postgresql or sqlite
    con: connection object
    """
    task3_query = 'SELECT t.*, d.device_name device_name FROM Transactions t JOIN Devices d on t.device_type = d.id'
    if sql_type == 'postgresql':
        task3_query = task3_query.replace(
            'FROM Transactions', 'FROM "Transactions"')
        task3_query = task3_query.replace('JOIN Devices', 'JOIN "Devices"')
    df = pd.DataFrame(pd.read_sql_query(task3_query, con))
    df.to_csv('task3_psql.csv', index=False)
    print('task3.csv is created.\n')


# TASK4 Update the data stored in the database to have the created revenue in EUR.
# xml elementtree, bs4, minidom
def task4(cursorObj, sql_type, con, xml_file, namespace):
    """
    Solution for task4
    :params:
    cursorObj: Cursor object for connection,
    sql_type: postgresql or sqlite
    con: connection object
    xml_file: xml file to be parsed
    namespace: namespace of the elements
    """
    # Passing the path of the
    # xml document to enable the
    # parsing process
    tree = etree.parse(xml_file)  # eurofxref-hist-90d.xml

    # dict to hold date-currency rate pairs
    dates_cur_dict = {}
    # Get the root element
    root = tree.getroot()

    # '{http://www.ecb.int/vocabulary/2002-08-01/eurofxref}'
    namespace = namespace

    # find all elements with the name Cube
    children = tree.findall(f'//{namespace}Cube')

    # loop over all of them, separate Cube elements based on attribs
    for elem in children:
        if 'time' in elem.attrib:
            time = elem.get('time')
            dates_cur_dict[time] = ''
        elif elem.attrib.get('currency') == 'USD':
            dates_cur_dict[time] = float(elem.attrib.get('rate'))

    c = con.cursor()
    # print the state before update
    print('Query Results Before EUR Conversion:')
    c.execute('SELECT id ,datetime, revenue FROM "Transactions" LIMIT 5')
    print(c.fetchall())
    for k, v in dates_cur_dict.items():
        date = k
        cur = v
        if sql_type == 'sqlite':
            update_query = "UPDATE Transactions SET revenue = revenue * ? WHERE strftime('%Y-%m-%d', datetime) = ?"
            c.execute(update_query, (cur, date))
            con.commit()
        else:
            update_query = '''UPDATE "Transactions" SET revenue = revenue * %s WHERE %s = TO_CHAR(datetime, 'YYYY-MM-DD')'''
            c.execute(update_query, (cur, date))
            con.commit()

    # print the state after update
    print('\nQuery Results After EUR Conversion:')
    c.execute('SELECT id, datetime, revenue FROM "Transactions" LIMIT 5')
    print(c.fetchall())
    con.close()


def selection(args):
    """
    Auxilary function to serve as a menu from console
    :params:
    args: arguments received from console
    """
    selection = ''
    sql_type = args[1]
    if sql_type == 'sqlite':
        db_file = args[2]
        selection = args[3]
        cursorObj, con = create_connection(sql_type, db_file)
    elif sql_type == 'postgresql':
        conn_string = args[2]
        db_file = args[3]
        selection = args[4]
        cursorObj, con = create_connection(sql_type, db_file, conn_string)
    else:
        print('There is something wrong with the sql type selection.')
    if selection == 'task1':
        task1(cursorObj, sql_type)
    elif selection == 'task2':
        task2(cursorObj, sql_type)
    elif selection == 'task3':
        task3(cursorObj, sql_type, con)
    elif selection == 'task4':
        task4(cursorObj, sql_type, con, xml_file='eurofxref-hist-90d.xml',
              namespace='{http://www.ecb.int/vocabulary/2002-08-01/eurofxref}')
    elif selection == 'all':
        task1(cursorObj, sql_type)
        task2(cursorObj, sql_type)
        task3(cursorObj, sql_type, con)
        task4(cursorObj, sql_type, con, xml_file='eurofxref-hist-90d.xml',
              namespace='{http://www.ecb.int/vocabulary/2002-08-01/eurofxref}')
    else:
        print('Select a task between 1-4 or all')


args = sys.argv  # first arg is script name
# usage: python3 tasks.py postgresql postgresql://postgres:postgres@localhost transactions.db task1(or all)
# usage: python3 tasks.py sqlite transactions.db task1(or all)
selection(args)
