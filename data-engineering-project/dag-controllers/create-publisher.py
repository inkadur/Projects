"""A dag that ingests all the raw data into BQ."""
import airflow
from airflow import models
from google.cloud import bigquery
from airflow.utils.trigger_rule import TriggerRule
from airflow.decorators import task
import random
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
def _create_publisher_stg(**kwargs):    
    project_id = 'shidcs329e'
    region = 'us-central1'
    raw_dataset_name = 'magazine_recipes_raw_af'
    stg_dataset_name = 'magazine_recipes_stg_af'


    client = bigquery.Client()
    sql_query = """
        SELECT recipe_id
        FROM magazine_recipes_stg_af.Recipes"""
    query_job = client.query(sql_query)

    journalist_magazine_map = {}
    for i in range(1, 91):
        if i <= 20:
            journalist_magazine_map[i] = i
        else:
            journalist_magazine_map[i] = random.randint(1, 20)
    recipe_ids = [row.recipe_id for row in query_job]

    table_name = 'Publications'

    schema = [
    bigquery.SchemaField("publication_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("recipe_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("magazine_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("journalist_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("date", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("volume", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("issue", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("publication_type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("data_source", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("load_time", "TIMESTAMP", mode="NULLABLE", default_value_expression="CURRENT_TIMESTAMP"),
    ]

    table_ref = client.dataset("magazine_recipes_stg_af").table(table_name)
    table = bigquery.Table(table_ref, schema=schema)

    client.create_table(table)

    rows_to_insert = []

    for index, recipe_id in enumerate(recipe_ids):
        journalist_id = random.randint(1, 90)
        magazine_id = journalist_magazine_map[journalist_id]
        row = {"publication_id": index, "recipe_id": recipe_id, "magazine_id": magazine_id, "journalist_id": journalist_id, "date" : None, 'volume' : None, 'issue': None, 'publication_type': None, 'data_source': None, "load_time": None}
        rows_to_insert.append(row)

    errors = client.insert_rows(table_ref, rows_to_insert, schema)

    if errors == []:
        print("Rows inserted successfully.")
    else:
        print("Encountered errors while inserting rows:", errors)


with models.DAG(
    "create-publisher",
    schedule_interval=None,
    default_args=default_args,
    render_template_as_native_obj=True,
):
    create_publisher_stg = _create_publisher_stg()

    # task dependencies
    create_publisher_stg
