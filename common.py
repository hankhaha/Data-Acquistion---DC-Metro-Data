import textwrap
import pandas
import psycopg2 as psql
from airflow.operators.postgres_operator import PostgresOperator, PostgresHook
from airflow.hooks.base_hook import BaseHook

headers = {'api_key': 'a2d3eedb7b804592b09ed95ac11ffc9f'}

sql_hook = PostgresHook(postgres_conn_id='warehouse')
warehouse = PostgresHook(postgres_conn_id='warehouse').get_conn()

def return_data(response, key):
    '''
    convert JSON format to pandas data frame
    '''
    return pandas.io.json.json_normalize(response.json()[key])

# insert records into warehouse table
def write_api_table(data, schema, table):
    '''
    write a temporary table to the database,using the temporary table to execute
    SQL statements (@ cursor.execute(sql)), and then dropping the temporary table.
    '''
    sql_hook = PostgresHook(postgres_conn_id='warehouse')
    warehouse = PostgresHook(postgres_conn_id='warehouse').get_conn()
    cursor = warehouse.cursor()
    cursor.execute('''DROP TABLE IF EXISTS {0}.{1}_api;'''.format(schema, table))
    warehouse.commit()
    data.to_sql(
    schema = schema,
    name = table+'_api',
    index = False,
    con = sql_hook.get_sqlalchemy_engine())
    warehouse.close()

def execute_sql_statement(sql):
    '''
    execute the sql queries to insert new records, update existing information,
    and mark expired records
    '''
    warehouse = PostgresHook(postgres_conn_id='warehouse').get_conn()
    cursor = warehouse.cursor()
    cursor.execute(sql)
    warehouse.commit()
    warehouse.close()

def drop_api_table(schema,table):
    '''
    drop the api table that contains the updated wmata information
    '''
    warehouse = PostgresHook(postgres_conn_id='warehouse').get_conn()
    cursor = warehouse.cursor()
    cursor.execute('''DROP TABLE IF EXISTS {0}.{1}_api;'''.format(schema, table))
    warehouse.commit()
    warehouse.close()
