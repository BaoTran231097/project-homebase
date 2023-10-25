import json, time, datetime, logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow import DAG
import pandas as pd


default_database ="postgres_connect"

# function get data from database postgresql
def func_data_in_db():
    # the sql
    SQL = """
            select *
            from public.wine_data;
    """
    # connect
    db = PostgresHook()
    df = db.get_pandas_df(SQL)
    return json.dumps(df.to_dict(orient='records'))

# function transfer data
def func_transfer_data(data):
    data=pd.DataFrame(json.loads(data))
    # convert data['classes'] (1 -> A, 2->B, 3->C)
    data['classes'] = [ 'A' if i==1 else ('B' if i==2 else 'C') for i in data['classes']]
    # convert data['quality'] (True -> 1, False->2)
    data['quality'] = (data['quality'])*1
    return json.dumps(data.to_dict(orient='records'))

# function return log
def func_logging(mess: str):
    return mess

# function insert into data to database clickhouse
def func_insert_data_clickhouse(data):
    import requests
    import json
    from io import BytesIO

    # Define the ClickHouse server URL and the table name
    clickhouse_url = 'http://172.17.0.1:8123'
    table_name = 'clickhousedb.wine_data'

    # Set the full URL for the HTTP insert endpoint
    insert_url = f'{clickhouse_url}/default/?user=clickhouse&password=clickhouse&query=INSERT INTO {table_name} FORMAT CSV settings format_csv_allow_single_quotes=0'

    try:
        data=json.loads(data)
        df = pd.DataFrame(data)
        df = df[['classes', 'alcohol', 'malic_acid', 'ash', 'alcalinity_of_ash',
       'magnesium', 'total_phenols', 'flavanoids', 'nonflavanoid_phenols',
       'proanthocyanins', 'color_intensity', 'hue', 'proline','quality', 'process_dt']]
        file = df.to_csv(sep=',',index=False, header=False, line_terminator='\n',encoding='utf-8').encode('utf-8')
        # Send an HTTP POST request to insert the JSON data
    
        print(df)
        print(file)
        response = requests.post(insert_url, data=BytesIO(file))

        # Check the response status
        if response.status_code == 200:
            print("Data inserted successfully.")
        else:
            print(f"Data insertion failed with status code: {response.txt}")
    except Exception as e:
        raise(f"Error: {str(e)}")

    return 'Success'

default_args = {
    "owner": "airflow",
    'email': ['trangiabao231097@gmail.com'],
	'email_on_failure': True,
	'email_on_retry': True,
}

with DAG(
    dag_id="Pipeline_Data_In_Homebase",
    default_args=default_args,
    start_date=days_ago(2),
    schedule_interval='@hourly',
    tags=['HOMEBASE'],
) as dag:

    result_data_in_db = PythonOperator(task_id = "func_data_in_db", 
                        python_callable = func_data_in_db)
    
    result_transfer_data = PythonOperator(task_id = "func_transfer_data", 
        python_callable = func_transfer_data, 
        op_kwargs={'data': result_data_in_db.output} 
    )


    result_insert_data_clickhouse = PythonOperator(task_id = "func_insert_data_clickhouse", 
        python_callable = func_insert_data_clickhouse, 
        op_kwargs={'data': result_transfer_data.output} 
    )

    result_logging_success = PythonOperator(task_id = "insert_data_clickhouse_success", 
        python_callable = func_logging, 
        op_kwargs={'mess': "INSERT DATA SUCCESS"},
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )
    
    result_logging_fail = PythonOperator(task_id = "insert_data_clickhouse_fail", 
        python_callable = func_logging, 
        op_kwargs={'mess': "INSERT DATA FAIL"} ,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    result_data_in_db >> result_transfer_data >> result_insert_data_clickhouse 
    result_insert_data_clickhouse >> [result_logging_success, result_logging_fail]