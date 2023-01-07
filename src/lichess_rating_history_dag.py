import airflow
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta
from airflow.utils.dates import days_ago

import lichess_rating_history

#docker cp {local_file} {container_id}:/{target_location}
#docker cp lichess_rating_history_dag.py :/opt/airflow/dags/lichess_rating_history_dag.py
#docker cp lichess_rating_history.py :/opt/airflow/dags/lichess_rating_history.py

args = {
            'owner': 'phil',    
            #'start_date': airflow.utils.dates.days_ago(2),
            # 'end_date': datetime(),
            # 'depends_on_past': False,
            'email': ['mcdanipc@gmail.com'],
            #'email_on_failure': False,
            #'email_on_retry': False,
            # If a task fails, retry it once after waiting
            # at least 5 minutes
            #'retries': 1,
            'retry_delay': timedelta(minutes=5),
        }

dag = DAG(
	dag_id = "simple_python_dag",
	default_args=args,
	# schedule_interval='0 0 * * *',
	schedule_interval='@once',	
	dagrun_timeout=timedelta(minutes=60),
	description='use case of python operator in airflow',
	start_date=datetime(year=2022, month=2, day=1)
    )

t1 = PythonOperator(
    task_id='task_number_1',
    python_callable= lichess_rating_history.write_lichess_data_to_csv,
    dag=dag,
)
t1