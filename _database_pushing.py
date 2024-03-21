import pandas as pd
import mysql.connector
from datetime import datetime
from _database_pulling import sql_pulling_head


def _get_csv_from_path(path='./DataMining/rpa/df_securities.csv'):
    df = pd.read_csv(path)
    def convert_to_mysql_datetime(datetime_str):
        try:
            dt = datetime.strptime(datetime_str, "%A, %B %d, %Y")
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None  # Return None if datetime string cannot be parsed
    df['datetime'] = df['datetime'].apply(convert_to_mysql_datetime)
    return df, path

def _conn():
    conn = mysql.connector.connect(
        host = 'ideatrade.serveftp.net',
        user = 'Sura@view',
        password = 'ZP6HpF-T04CGw_t(',
        database = 'db_ideatrade',
        port = 51410
    )
    return conn

def _sql_create_table(table_name, col_def):
    create_table_query = "CREATE TABLE IF NOT EXISTS {} ({})".format(table_name, ',\n    '.join(col_def))
    # Execute the SQL query
    cursor.execute(create_table_query)
    return cursor

def _def_col_float_datetime(df):
    col_def = [
        "`{}` FLOAT".format(column_name.replace(' ', '_'))
        if column_name != 'datetime' else "`{}` DATETIME".format(column_name)
        for column_name in df.columns
    ]
    return col_def

if __name__ == '__main__':
    df, path = _get_csv_from_path()
    print('>>> get df, path.')
    
    conn = _conn()
    cursor = conn.cursor()
    print('>>> cursor connected.')
    
    col_def = _def_col_float_datetime(df)
    print('>>> col_def')
    
    table_name = "Program_Trading_by_Securities"
    cursor, ex_col_def = sql_pulling_head(table_name, cursor, print_value=False)
    cursor.fetchall()
    ###
    # Extract column names from col_def
    new_col_names = [col.split()[0][1:-1] for col in col_def]
    # Identify columns not specified yet
    not_specified_columns = [col_name for col_name in new_col_names if col_name not in ex_col_def]
    print('[BEFORE] not_specified columns: ',not_specified_columns)
    
    # Add new columns to the existing table
    for column in not_specified_columns:
        data_type = [col.split()[1] for col in col_def if col.split()[0][1:-1] == column][0]
        add_column_query = f"ALTER TABLE {table_name} ADD {column} {data_type};"
        print(f"Added column {column} with data type {data_type} to the table.")
        cursor.execute(add_column_query)
        conn.commit()
    
    cursor, ex_col_def = sql_pulling_head(table_name, cursor, print_value=False)
    cursor.fetchall()
    not_specified_columns = [col_name for col_name in new_col_names if col_name not in ex_col_def]
    print('[AFTER] not_specified columns: ',not_specified_columns)
    if not_specified_columns==[]:
        print('>>> all columns are updated into {} table'.format(table_name))
    else:
        print('>>> not all columns are updated into {} table.'.format(table_name))
    ###
    
    cursor = _sql_create_table(table_name, col_def)
    print('>>> table created || table exsited.')

    # Iterate through DataFrame rows and insert data into MySQL table
    for _, row in df.iterrows():
        insert_query = "INSERT INTO {} ({}) VALUES ({})" \
            .format(\
                table_name, \
                ','.join(['`' + col.replace(' ', '_') + '`' for col in df.columns]), \
                ','.join(['%s']*len(df.columns))
                )
        cursor.execute(insert_query, tuple(row))
    # Commit changes
    conn.commit()
    print(f'>>> [{table_name}] INSERT by [{path}]')
    print('>>> data commited.')
    
    # Close cursor and connection
    conn.close()