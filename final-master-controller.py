import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.utils.dates import days_ago
from datetime import timedelta
from pendulum import duration

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0
}

dag = DAG(
    dag_id='final-master-controller',
    default_args=default_args,
    description=' final project master dag',
    schedule_interval=None,
    max_active_runs=1,
    concurrency=1,
    catchup=False,
    dagrun_timeout=None,
)

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)
wait_after_ai = TimeDeltaSensor(task_id="wait_after_model", delta=duration(seconds=10), dag=dag)
wait_after_controller = TimeDeltaSensor(task_id="wait_after_key", delta=duration(seconds=10), dag=dag)

# llm controller
ai_controller = TriggerDagRunOperator(
    task_id="ai_controller",
    trigger_dag_id="ai-controller",  
    dag=dag)
    
# eop controller
ease_of_prep_controller = TriggerDagRunOperator(
    task_id="ease_of_prep_controller",
    trigger_dag_id="ease-of-prep-controller",  
    dag=dag)
    
# comments controller
comments_controller = TriggerDagRunOperator(
    task_id="comments_controller",
    trigger_dag_id="comments-controller",  
    dag=dag)


start >> ai_controller >> wait_after_ai >> ease_of_prep_controller >> wait_after_controller >>  comments_controller >> end