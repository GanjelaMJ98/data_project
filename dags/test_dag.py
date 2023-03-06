from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator


def get_my_library():
    from smartc_lib.meetings.preprocessor import meetings_preprocessor
    print(f'My lib function {meetings_preprocessor}')


def get_pandas():
    import pandas
    print(f'Pandas with wersion {pandas.__version__}')


def get_openpyxl():
    import openpyxl
    print(f'Pandas with wersion {openpyxl.__version__}')


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

    get_openpyxl = PythonOperator(
        task_id = 'get_openpyxl',
        python_callable=get_openpyxl
    )

    get_my_library >> get_pandas >> get_openpyxl
