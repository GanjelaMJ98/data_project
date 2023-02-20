from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def get_my_library():
    import smartc_lib
    print(f'My lib with wersion {smartc_lib}')


def get_pandas():
    import pandas
    print(f'Pandas with wersion {pandas.__version__}')



with DAG(
    dag_id='test_libs_dag',
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example','test'],
) as dag:

    get_my_library = PythonOperator(
        task_id = 'get_my_library',
        python_callable=get_my_library
    )

    get_pandas = PythonOperator(
        task_id = 'get_pandas',
        python_callable=get_pandas
    )

    get_my_library >>  get_pandas
