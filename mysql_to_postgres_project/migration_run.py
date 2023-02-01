import psycopg2 as pg
from sshtunnel import SSHTunnelForwarder
import pandas as pd
import mysql.connector
from mysql.connector import Error
from sqlalchemy import create_engine
import logging
import time
import warnings


def my_sql_con():
    try:
        conn = mysql.connector.connect(host='',
                                database='',
                                user='',
                                password='')
        if conn.is_connected():
            logging.info("You're connected to MySQL")
            return conn
    except Error as e:
        logging.info(f"Print your error msg {e}")


def my_sql_con_ssh():
    try:
        logging.info('Connecting to the PostgreSQL Database...')
        ssh_tunnel = SSHTunnelForwarder(
                (''),
                ssh_username="localadmin",
                ssh_password= '',
                remote_bind_address=('', 3306)
            )
        ssh_tunnel.start()  
        engine = create_engine('mysql+pymysql://{user}:{password}@{host}:{port}/{db}'.format(
                 host='127.0.0.1',
                 port=ssh_tunnel.local_bind_port,
                 user='',
                 password='',
                 db=''
                ))
        return engine
    except Error as e:
        logging.info(f"Connection Has Failed... {e}")


def pq_sql_con():
    try:
        logging.info('Connecting to the PostgreSQL Database...')
        ssh_tunnel = SSHTunnelForwarder(
                (''),
                ssh_username="",
                ssh_password= '',
                remote_bind_address=('', 5000)
            )
        ssh_tunnel.start()  
        engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{db}'.format(
                 host='127.0.0.1',
                 port=ssh_tunnel.local_bind_port,
                 user='',
                 password='',
                 db=''
                ))
        return engine
    except Error as e:
        logging.info(f"Connection Has Failed... {e}")


def check_pg_count(table_name: str) -> int:
    truncate_sql = f"select count(*) from {table_name}"
    result_dataFrame = pd.read_sql(truncate_sql, pq_con)
    return result_dataFrame.to_dict()['count'][0]


def check_mysql_count(table_name: str) -> int:
    truncate_sql = f"select count(*) as count from {table_name}"
    result_dataFrame = pd.read_sql(truncate_sql, mysql_con)
    return result_dataFrame.to_dict()['count'][0]


mysql_con = my_sql_con_ssh()
pq_con = pq_sql_con()


pk_tables_query = """
SELECT "insurance_types" AS REFERENCED_TABLE_NAME
UNION
SELECT REFERENCED_TABLE_NAME
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE REFERENCED_TABLE_SCHEMA IS NOT NULL;"""
pk_tbl_dataframe = pd.read_sql(pk_tables_query, mysql_con)

all_tables_query = "SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA = 'laradb' order by TABLE_NAME;"
all_tbl_dataframe = pd.read_sql(all_tables_query, mysql_con)

skip_tables = ["personal_access_tokens", "error_logs"]


for item in pk_tbl_dataframe.to_dict()["REFERENCED_TABLE_NAME"].values():
    try:
        if not item.startswith("telescope") and check_pg_count(item) == 0 and item not in skip_tables:
            print(f"Starting That table is {item}")
            for chunk_dataframe in pd.read_sql(f"SELECT * FROM {item}", mysql_con, chunksize=50000):
                chunk_dataframe.to_sql(con=pq_con, name=item, schema="public", if_exists='append',index=False, method="multi")
                print(f"That table is uploaded {item} tables count={len(chunk_dataframe.index)}")
        else:
            print(f"That table is not empty skipted {item}")
    except Error as e:
        print(f"Loading Has Failed on table{item}... {e}")


print(f"Starting OTHER_TABLE_NAME")
for item in all_tbl_dataframe.to_dict()["TABLE_NAME"].values():
    try:
        if not item.startswith("telescope") and check_pg_count(item) == 0 and item not in skip_tables:
            print(f"That table started {item}")
            for chunk_dataframe in pd.read_sql(f"SELECT * FROM {item}", mysql_con, chunksize=10000):
                chunk_dataframe.to_sql(con=pq_con, name=item, schema="public", if_exists='append',index=False, method="multi")
                print(f"That table is uploaded {item} tables count={len(chunk_dataframe.index)}")
        else:
            print(f"That table is not empty skipted {item}")
    except Error as e:
        print(f"Loading Has Failed on table{item}... {e}")
print("Migration Done")


print("Starting DQ Test")
for item in all_tbl_dataframe.to_dict()["TABLE_NAME"].values():
    if check_pg_count(item) == check_mysql_count(item):
        print(f"Table looks good {item}")
    else:
        print(f"Table not matched {item}")
print("DQ End")
        
