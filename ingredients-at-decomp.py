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

# removes the entries from the dictionary whose values are None
# this filter is needed for loading JSON into BQ
def remove_none_values(record):
    filtered_record = {}
    for field in record.keys():
        if record[field] != None:
            filtered_record[field] = record[field]
    return filtered_record


@task(trigger_rule="all_done")
def _create_recipe_ingredient_at(**kwargs):    
    project_id = 'shidcs329e'
    region = 'us-central1'
    raw_dataset_name = 'magazine_recipes_raw_af'
    stg_dataset_name = 'magazine_recipes_stg_af'
    raw_table_name = "recipe_at"
    stg_table_name = "recipe_ingredient_at"

    recipe_at = []
    target_table_id = "{}.{}.{}".format(project_id, stg_dataset_name, stg_table_name)


    schema = [
      bigquery.SchemaField("recipe_id", "INTEGER", mode = "REQUIRED"),
      bigquery.SchemaField("name", "STRING", mode = "NULLABLE"),
      bigquery.SchemaField("rating", "INTEGER", mode = "NULLABLE"),
      bigquery.SchemaField("ease_of_prep", "STRING", mode = "NULLABLE"),
      bigquery.SchemaField("note", "STRING", mode = "NULLABLE"),
      bigquery.SchemaField("type","STRING", mode = "NULLABLE"),
      bigquery.SchemaField("prep_time", "INTEGER", mode = "NULLABLE"),
      bigquery.SchemaField("cookbook", "STRING", mode = "NULLABLE"),
      bigquery.SchemaField("page", "INTEGER", mode = "NULLABLE"),
      bigquery.SchemaField("ingredient_1","STRING", mode = "NULLABLE"),
      bigquery.SchemaField("ingredient_2","STRING", mode = "NULLABLE"),
      bigquery.SchemaField("ingredient_3","STRING", mode = "NULLABLE"),
      bigquery.SchemaField("ingredient_4","STRING", mode = "NULLABLE"),
      bigquery.SchemaField("ingredient_5","STRING", mode = "NULLABLE"),
      bigquery.SchemaField("ingredient_6","STRING", mode = "NULLABLE"),
      bigquery.SchemaField("ingredient_7","STRING", mode = "NULLABLE"),
      bigquery.SchemaField("slowcooker","STRING", mode = "NULLABLE"),
      bigquery.SchemaField("link","STRING", mode = "NULLABLE"),
      bigquery.SchemaField("last_made","STRING", mode = "NULLABLE"),
      bigquery.SchemaField("load_time", "TIMESTAMP", mode="NULLABLE", default_value_expression="CURRENT_TIMESTAMP")
    ]


    bq_client = bigquery.Client()
    sql = "select * from {}.{}".format(raw_dataset_name, raw_table_name)
    query_job = bq_client.query(sql)

    for index, row in enumerate(query_job):
        name = row["name"]
        rating = row["rating"]
        ease_of_prep = row["ease_of_prep"]
        note = row["note"]
        type = row["type"]
        prep_time = row["prep_time"]
        cookbook = row["cookbook"]
        page = row["page"]
        slowcooker = row["slowcooker"]
        link = row["link"]
        last_made = row["last_made"]
        load_time = json.dumps(row["load_time"], default=serialize_datetime).replace('"', '')
        # define ingredients individually
        ingredients = row["ingredients"]

        ingredients = row["ingredients"]
        if ingredients:
            ingredients_list = ingredients.split(",")
            for i in range(7):
                if i < len(ingredients_list):
                    record[f'ingredient_{i+1}'] = ingredients_list[i].strip()
                else:
                    record[f'ingredient_{i+1}'] = None
        else:
          ingredient_1 = None
          ingredient_2 = None
          ingredient_3 = None
          ingredient_4 = None
          ingredient_5 = None
          ingredient_6 = None
          ingredient_7 = None
        # else:
        #   ingredient_num = len(ingredients.split(","))
        #   for i in range(7):
        #     if i < ingredient_num:
        #       variable_value = ingredients.split(",")[i].strip()
        #       exec(f"ingredient_{i+1} = '{variable_value}'")
        #     else:
        #       exec(f"ingredient_{i+1} = None")

        record = {}
        record['recipe_id'] = index+1

        if name != None:
          record["name"] = name
        if rating != None:
          record["rating"] = rating
        if ease_of_prep != None:
          record['ease_of_prep'] = ease_of_prep
        if note != None:
          record['note'] = note
        if type != None:
          record['type'] = type
        if prep_time != None:
          record['prep_time'] = prep_time
        if cookbook != None:
          record['cookbook'] = cookbook
        if page != None:
          record['page'] = page
        if slowcooker != None:
          record['slowcooker'] = slowcooker
        if link != None:
          record['link'] = link
        if last_made != None:
          record['last_made'] = last_made
        if load_time != None:
          record['load_time'] = load_time
        if ingredient_1 != None:
          record['ingredient_1'] = ingredient_1
        if ingredient_2 != None:
          record['ingredient_2'] = ingredient_2
        if ingredient_3 != None:
          record['ingredient_3'] = ingredient_3
        if ingredient_4 != None:
          record['ingredient_4'] = ingredient_4
        if ingredient_5 != None:
          record['ingredient_5'] = ingredient_5
        if ingredient_6 != None:
          record['ingredient_6'] = ingredient_6
        if ingredient_7 != None:
          record['ingredient_7'] = ingredient_7

        recipe_at.append(record)

    # load records into staging table

    job_config = bigquery.LoadJobConfig(schema=schema, source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, write_disposition='WRITE_TRUNCATE')
    table_ref = bigquery.table.TableReference.from_string(target_table_id)

    try:
        job = bq_client.load_table_from_json(recipe_at, table_ref, job_config=job_config)
        #print(job.error_result, job.errors)
        print('Inserted into', stg_table_name, ':', (len(recipe_at)), 'records')

        if job.errors:
          print('job errors:', job.errors)

    except Exception as e:
        print("Error inserting into BQ: {}".format(e))


with models.DAG(
    "ingredients-at-decomp",
    schedule_interval=None,
    default_args=default_args,
    render_template_as_native_obj=True,
):
    create_recipe_ingredient_at = _create_recipe_ingredient_at()

    # task dependencies
    create_recipe_ingredient_at


    



