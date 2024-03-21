import pandas as pd
import mysql.connector
from datetime import datetime
from tqdm import tqdm

def _conn():
    conn = mysql.connector.connect(
        host = 'ideatrade.serveftp.net',
        user = 'Sura@view',
        password = 'ZP6HpF-T04CGw_t(',
        database = 'db_ideatrade',
        port = 51410
    )
    return conn

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

def sql_delete_datetime(table_name, cursor, datetime_sel, conn=[], print_value=False, commit=False, ):
    select_query = "DELETE FROM {} WHERE datetime = '{}'".format(table_name, datetime_sel)
    # select_query = 'SELECT datetime FROM `{}` LIMIT 1'.format(table_name)
    
    print(select_query)
    
    if print_value:
        cursor, heads = sql_pulling_datetime(table_name, cursor)
        datetime_5 = heads[:5]
        print('>>> [BEFORE] last 5 datetime: {}'.format(datetime_5))
    
    print('>>> execute DELETE datetime={}'.format(datetime_sel))
    cursor.execute(select_query)
    if commit:
        conn.commit()
        print(f'>>> [{table_name} was deleted datetime=`{datetime_sel}`]')
        
    if print_value:
        cursor, heads = sql_pulling_datetime(table_name, cursor)
        datetime_5 = heads[:5]
        print('>>> [AFTER] last 5 datetime: {}'.format(datetime_5))
    
    
    
    return cursor

if __name__ == '__main__':
    conn = _conn()
    cursor = conn.cursor()
    print('>>> cursor connected.')
    # Define the table name with spaces
    table_name = "Program_Trading_by_Securities"
    
    datetime_sel  = '2024-03-19'
    cursor = sql_delete_datetime(table_name, cursor, datetime_sel, commit=True, conn=conn, print_value=True)
    conn.close()