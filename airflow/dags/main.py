from datetime import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable, Connection

from modules.get_api import GetApi
from modules.connector import Connector
from modules.transform import transform

def get_api_data(**kwargs):
    scraper = GetApi(Variable.get('http://103.150.197.96:5005/api/v1/rekapitulasi_v2/jabar/harian?level=kab'))
    data = scraper.get_api_data()
    print(data.info())

    get_conn = Connection.get_connection_from_secrets('Mysql')
    connector = Connector()
    engine_sql = connector.connect_mysql(
        host = get_conn.host,
        user = get_conn.login,
        password = get_conn.password,
        db = get_conn.schema,
        port = get_conn.port
    )

    try:
        p = "DROP table IF EXISTS covid_data"
        engine_sql.execute(p)
    except Exception as e:
        logging.error(e)

    data.to_sql(con = engine_sql, name = 'covid_data', index = False)
    logging.info('DATA INSERTED SUCCESSFULLY TO MYSQL')   

def generate_dim(**kwargs):
    get_conn_mysql = Connection.get_connection_from_secrets("Mysql")
    get_conn_postgres = Connection.get_connection_from_secrets("Postgres")    
    connector = Connector()
    engine_sql = connector.connect_mysql(
        host = get_conn_mysql.host,
        user = get_conn_mysql.login,
        password = get_conn_mysql.password,
        db = get_conn_mysql.schema,
        port = get_conn_mysql.port
    )

    engine_postgres = connector.connect_postgres(
        host = get_conn_postgres.host,
        user = get_conn_postgres.login,
        password = get_conn_postgres.password,
        db = get_conn_postgres.schema,
        port = get_conn_postgres.port
    )

    transformer = transform(engine_sql, engine_postgres)
    transformer.create_dim_case()
    transformer.create_dim_district()
    transformer.create_dim_province()

def insert_province_daily(**kwargs):
    get_conn_mysql = Connection.get_connection_from_secrets("Mysql")
    get_conn_postgres = Connection.get_connection_from_secrets("Postgres")    
    connector = Connector()
    engine_sql = connector.connect_mysql(
        host = get_conn_mysql.host,
        user = get_conn_mysql.login,
        password = get_conn_mysql.password,
        db = get_conn_mysql.schema,
        port = get_conn_mysql.port
    )

    engine_postgres = connector.connect_postgres(
        host = get_conn_postgres.host,
        user = get_conn_postgres.login,
        password = get_conn_postgres.password,
        db = get_conn_postgres.schema,
        port = get_conn_postgres.port
    )
  
    transformer = transform(engine_sql, engine_postgres)
    transformer.create_province_daily()

def insert_district_daily(**kwargs):
    get_conn_mysql = Connection.get_connection_from_secrets("Mysql")
    get_conn_postgres = Connection.get_connection_from_secrets("Postgres")    
    connector = Connector()
    engine_sql = connector.connect_mysql(
        host = get_conn_mysql.host,
        user = get_conn_mysql.login,
        password = get_conn_mysql.password,
        db = get_conn_mysql.schema,
        port = get_conn_mysql.port
    )

    engine_postgres = connector.connect_postgres(
        host = get_conn_postgres.host,
        user = get_conn_postgres.login,
        password = get_conn_postgres.password,
        db = get_conn_postgres.schema,
        port = get_conn_postgres.port
    )
 
    transformer = transform(engine_sql, engine_postgres)
    transformer.create_district_daily()

with DAG(
    dag_id = 'dag_fp_erik',
    start_date = datetime(2023, 7, 20),
    schedule_interval = '0 0 * * * ',
    catchup = False
) as dag:
    
    op_get_data_from_api = PythonOperator(
        task_id = 'get_data_from_api',
        python_callable = get_api_data
    )

    op_generate_dim = PythonOperator(
        task_id = 'generate_dim',
        python_callable = generate_dim
    )

    op_insert_province_daily = PythonOperator(
        task_id = 'insert_province_daily',
        python_callable = insert_province_daily
    )

    op_insert_district_daily = PythonOperator(
        task_id = 'insert_district_daily',
        python_callable = insert_district_daily
    )

op_get_data_from_api >> op_generate_dim
op_generate_dim >> op_insert_province_daily
op_generate_dim >> op_insert_district_daily
    

