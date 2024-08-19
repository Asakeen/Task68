from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


def generate_pascals_triangle(n):
    triangle = [[1]]
    for i in range(1, n):
        row = [1]
        for j in range(1, i):
            row.append(triangle[i-1][j-1] + triangle[i-1][j])
        row.append(1)
        triangle.append(row)
    return triangle


def print_pascals_triangle():
    triangle = generate_pascals_triangle(10)
    for row in triangle:
        print(row)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 18),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'pascal',
    default_args=default_args,
    description='A simple DAG to print Pascal\'s Triangle',
    schedule_interval=timedelta(days=1),
)

run_this = PythonOperator(
    task_id='print_pascals_triangle',
    python_callable=print_pascals_triangle,
    dag=dag,
)
