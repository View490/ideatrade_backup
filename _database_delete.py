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

def sql_pulling_star_len(table_name, cursor, print_value=False):
    select_query = 'SELECT * FROM `{}`'.format(table_name)
    cursor.execute(select_query)
    rows = cursor.fetchall()
    print('rows: ', rows)
    # Print the rows
    if print_value:
        print('rows len: ', len(rows))
    return cursor

def sql_delete_star(table_name, cursor, commit=False):
    print('>>> before delete.')
    sql_pulling_star_len(table_name, cursor, print_value=True)
    select_query = 'DELETE FROM {}'.format(table_name)
    cursor.execute(select_query)
    print('>>> after delete.')
    sql_pulling_star_len(table_name, cursor, print_value=True)
    if commit:
        conn.commit()
        print(f'>>> [{table_name} was deleted all sample.]')
    return cursor

if __name__ == '__main__':
    print('running hello.')
    conn = _conn()
    cursor = conn.cursor()
    print('>>> cursor connected.')
    
    # Define the table name with spaces [commit or not ???]
    table_name = "Program_Trading_by_Securities"
    cursor = sql_delete_star(table_name, cursor, commit=True)
    conn.close()