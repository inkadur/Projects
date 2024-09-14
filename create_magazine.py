"""A dag that ingests all the raw data into BQ."""
import airflow
from airflow import models
from google.cloud import bigquery
from airflow.utils.trigger_rule import TriggerRule
from airflow.decorators import task
import json, datetime

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0
}

def serialize_datetime(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")


@task(trigger_rule="all_done")
def _create_meal_service(**kwargs):
    
    project_id = kwargs["dag_run"].conf["project_id"]
    region = kwargs["dag_run"].conf["region"]
    stg_dataset_name = kwargs["dag_run"].conf["stg_dataset_name"]

# Snack_Service
@task(trigger_rule="all_done")
def _create_magazine_stg(**kwargs):
    
    project_id = kwargs["dag_run"].conf["project_id"]
    region = kwargs["dag_run"].conf["region"]
    stg_dataset_name = kwargs["dag_run"].conf["stg_dataset_name"]
    stg_table_name = "Snack_Service" # uppercase the name because it's a final table

    
    target_table_id = f"{project_id}.{stg_dataset_name}.{stg_table_name}"

    client = bigquery.Client()

    table_name = 'Magazines'

    schema = [
    bigquery.SchemaField("magazine_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("magazine_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("website", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("pub_frequency_weeks", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("publishing_company", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("subscription_price", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("data_source", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("load_time", "TIMESTAMP", mode="REQUIRED", default_value_expression="CURRENT_TIMESTAMP"),
    ]

    table_ref = client.dataset("magazine_recipes_stg_af").table(table_name)
    table = bigquery.Table(table_ref, schema=schema)

    client.create_table(table)

    rows_to_insert = []
    table_ref = client.dataset("magazine_recipes_stg_af").table('Magazines')

    for i in range(1, 21):
        row = {"magazine_id": i, "magazine_name": None, "website": None, "pub_frequency_weeks": None, "publishing_company" : None, 'subscription_price' : None, 'data_source': None, "load_time": None}
        rows_to_insert.append(row)

    errors = client.insert_rows(table_ref, rows_to_insert, schema)

    if errors == []:
        print("Rows inserted successfully.")
    else:
        print("Encountered errors while inserting rows:", errors)


    
with models.DAG(
    "create_magazine",
    schedule_interval=None,
    default_args=default_args,
    render_template_as_native_obj=True,
):
    create_magazine_stg = _create_magazine_stg()

    # run both tasks in parallel
    create_magazine_stg


