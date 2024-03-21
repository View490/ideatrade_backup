import pandas as pd
import mysql.connector
from datetime import datetime
from tqdm import tqdm
import csv

def _conn():
    conn = mysql.connector.connect(
        host = 'ideatrade.serveftp.net',
        user = 'Sura@view',
        password = 'ZP6HpF-T04CGw_t(',
        database = 'db_ideatrade',
        port = 51410
    )
    return conn

def sql_pulling_star(table_name, cursor, print_value=False):
    select_query = 'SELECT * FROM `{}`'.format(table_name)
    cursor.execute(select_query)
    rows = cursor.fetchall()
    print('rows len (datetime): ', len(rows))
    if print_value:
        for row in rows:
            print('row: ', row)
    return cursor, rows

def sql_pulling_head(table_name, cursor, print_value=False):
    select_query = 'SELECT * FROM `{}` LIMIT 1'.format(table_name)
    cursor.execute(select_query)
    columns = cursor.description
    column_names = [column[0] for column in columns]
    if print_value:
        print("Column Names:", column_names)
    return cursor, column_names

def sql_pulling_datetime(table_name, cursor, print_value=False):
    select_query = 'SELECT datetime FROM `{}`'.format(table_name)
    cursor.execute(select_query)
    rows = cursor.fetchall()
    datetime_list = [row[0] for row in rows]
    if print_value:
        print("datetime:")
        for i in range(len(datetime_list)):
            print('\t', datetime_list[i])
    return cursor, datetime_list

def sql_head_star_to_csv(table_name, cursor, review=True, print_value=False):
    cursor, column_names = sql_pulling_head(table_name, cursor, print_value)
    cursor.fetchall()
    cursor, rows = sql_pulling_star(table_name, cursor, print_value)
    
    if review:
        # Print head and first few rows with column names
        print("Column Names:", column_names)
        print("Data Preview:")
        for row in rows[:5]:  # Print first few rows
            print(row)
    else:
        # Writing to CSV
        with open('./output.csv', 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(column_names)
            csv_writer.writerows(rows)
    
    print("CSV file created successfully.")

if __name__ == '__main__':
    conn = _conn()
    cursor = conn.cursor()
    print('>>> cursor connected.')
    
    # Define the table name with spaces
    table_name = "Program_Trading_by_Securities"
    cursor, heads = sql_pulling_head(table_name, cursor, print_value=True)
    cursor.fetchall()
    cursor, col_name = sql_pulling_datetime(table_name, cursor, print_value=True)
    cursor, rows = sql_pulling_star(table_name, cursor, print_value=False)
    sql_head_star_to_csv(table_name, cursor, review=True)
    conn.close()
    
    print(type(rows), '-- len(datetime): ', len(rows), ', n_symbols: ', len(rows[0]))