from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from smartc_lib.meetings.preprocessor import meetings_preprocessor

from datetime import datetime
import pandas as pd


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 2, 20),
    'retries': 1,
    'excel_file_path': '/opt/airflow/data/testcase.xlsx',
    'output_file_path': '/opt/airflow/data/output.xlsx',
}


def read_meetings(excel_file_path, output_file_path):
    meetings = pd.read_excel(excel_file_path, sheet_name='meetings')
    addressbook = pd.read_excel(excel_file_path, sheet_name='addressbook')
    df = meetings_preprocessor(meetings, addressbook)
    if df.empty:
        raise ValueError('DataFrame is empty')
    print(df.head(1))
    df.to_excel(output_file_path, index=False)


def print_sum_duration(output_file_path):
    df = pd.read_excel(output_file_path)
    total_duration = df['duration'].sum()
    print(f'Total sum: {total_duration}')


with DAG('csv_sum_dag_', default_args=default_args, schedule_interval=None) as dag:

    task1 = PythonOperator(
        task_id='add_column',
        python_callable=read_meetings,
        op_kwargs={
            'excel_file_path': default_args['excel_file_path'],
            'output_file_path': default_args['output_file_path']
        },
        on_failure_callback=lambda context: dag.get_task('print_sum_duration').set_upstream(None)
    )

    task2 = PythonOperator(
        task_id='print_sum_duration',
        python_callable=print_sum_duration,
        op_kwargs={'output_file_path': default_args['output_file_path']}
    )

    task1 >> task2