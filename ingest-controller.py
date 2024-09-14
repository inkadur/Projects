import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

project_id = 'shidcs329e'
dataset_name = 'magazine_recipes_raw_af'
bucket_name = 'cookbook_data113'
region = 'us-central1'

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0
}

dag = DAG(
    dag_id='ingest-controller',
    default_args=default_args,
    description='controller dag',
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
)

start = DummyOperator(task_id="start", dag=dag)

# create dataset
create_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='create_dataset_if_needed',
    project_id=project_id,
    dataset_id=dataset_name,
    location=region,
    if_exists='ignore',
    dag=dag)

# load bird_recipes
bird_recipes_table_name = 'bird_recipes'
bird_recipes_file_name = 'raw/Recipes.csv'
bird_recipes_delimiter = ','
bird_recipes_skip_leading_rows = 1

bird_recipes_schema_full = [
  {"name": "recipe_id", "type": "INTEGER", "mode": "REQUIRED"},
  {"name": "title", "type": "STRING", "mode": "NULLABLE"},
  {"name": "subtitle", "type": "STRING", "mode": "NULLABLE"},
  {"name": "servings", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "yield_unit", "type": "STRING", "mode": "NULLABLE"},
  {"name": "prep_min", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "cook_min", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "stnd_min", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "source", "type": "STRING", "mode": "NULLABLE"},
  {"name": "intro", "type": "STRING", "mode": "NULLABLE"},
  {"name": "directions", "type": "STRING", "mode": "NULLABLE"},
  {"name": "load_time", "type": "TIMESTAMP", "mode": "REQUIRED","default_value_expression": "CURRENT_TIMESTAMP"},
]


bird_recipes_schema = [
  {"name": "recipe_id", "type": "INTEGER", "mode": "REQUIRED"},
  {"name": "title", "type": "STRING", "mode": "NULLABLE"},
  {"name": "subtitle", "type": "STRING", "mode": "NULLABLE"},
  {"name": "servings", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "yield_unit", "type": "STRING", "mode": "NULLABLE"},
  {"name": "prep_min", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "cook_min", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "stnd_min", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "source", "type": "STRING", "mode": "NULLABLE"},
  {"name": "intro", "type": "STRING", "mode": "NULLABLE"},
  {"name": "directions", "type": "STRING", "mode": "NULLABLE"},
]

bird_recipes = TriggerDagRunOperator(
    task_id="bird_recipes",
    trigger_dag_id="p5-ingest-table",  
    conf={"project_id": project_id, "dataset_name": dataset_name, "bucket_name": bucket_name, \
           "table_name": bird_recipes_table_name, "file_name": bird_recipes_file_name, \
           "delimiter": bird_recipes_delimiter, "skip_leading_rows": bird_recipes_skip_leading_rows, \
           "schema_full": bird_recipes_schema_full, "schema": bird_recipes_schema},
    dag=dag)


# load ingredients
bird_ingredients_table_name = 'bird_ingredients'
bird_ingredients_file_name = 'raw/Ingredients.csv'
bird_ingredients_delimiter = ','
bird_ingredients_skip_leading_rows = 1

bird_ingredients_schema_full = [
  {"name": "ingredient_id", "type": "INTEGER", "mode": "REQUIRED"},
  {"name": "category", "type": "STRING", "mode": "NULLABLE"},
  {"name": "name", "type": "STRING", "mode": "REQUIRED"},
  {"name": "plural", "type": "STRING", "mode": "NULLABLE"},
  {"name": "load_time", "type": "TIMESTAMP", "mode": "REQUIRED", "default_value_expression": "CURRENT_TIMESTAMP"},
]


bird_ingredients_schema = [
  {"name": "ingredient_id", "type": "INTEGER", "mode": "REQUIRED"},
  {"name": "category", "type": "STRING", "mode": "NULLABLE"},
  {"name": "name", "type": "STRING", "mode": "REQUIRED"},
  {"name": "plural", "type": "STRING", "mode": "NULLABLE"},
]


bird_ingredients = TriggerDagRunOperator(
    task_id="bird_ingredients",
    trigger_dag_id="p5-ingest-table",  
    conf={"project_id": project_id, "dataset_name": dataset_name, "bucket_name": bucket_name, \
           "table_name": bird_ingredients_table_name, "file_name": bird_ingredients_file_name, \
           "delimiter": bird_ingredients_delimiter, "skip_leading_rows": bird_ingredients_skip_leading_rows, \
           "schema_full": bird_ingredients_schema_full, "schema": bird_ingredients_schema},
    dag=dag)
    

# load Quantity
bird_quantity_table_name = 'bird_quantity'
bird_quantity_file_name = 'raw/Quantity.csv'
bird_quantity_delimiter = ","
bird_quantity_skip_leading_rows = 1

bird_quantity_schema_full = [
  {"name": "quantity_id", "type": "INTEGER", "mode": "REQUIRED"},
  {"name": "recipe_id", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "ingredient_id", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "max_qty", "type": "FLOAT", "mode": "NULLABLE"},
  {"name": "min_qty", "type": "FLOAT", "mode": "NULLABLE"},
  {"name": "unit", "type": "STRING", "mode": "NULLABLE"},
  {"name": "preparation", "type": "STRING", "mode": "NULLABLE"},
  {"name": "optional", "type": "BOOLEAN", "mode": "NULLABLE"},
  {"name": "load_time", "type": "TIMESTAMP", "mode": "REQUIRED", "default_value_expression": "CURRENT_TIMESTAMP"},
]

bird_quantity_schema = [
  {"name": "quantity_id", "type": "INTEGER", "mode": "REQUIRED"},
  {"name": "recipe_id", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "ingredient_id", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "max_qty", "type": "FLOAT", "mode": "NULLABLE"},
  {"name": "min_qty", "type": "FLOAT", "mode": "NULLABLE"},
  {"name": "unit", "type": "STRING", "mode": "NULLABLE"},
  {"name": "preparation", "type": "STRING", "mode": "NULLABLE"},
  {"name": "optional", "type": "BOOLEAN", "mode": "NULLABLE"},
]

bird_quantity = TriggerDagRunOperator(
    task_id="bird_quantity",
    trigger_dag_id="p5-ingest-table",  
    conf={"project_id": project_id, "dataset_name": dataset_name, "bucket_name": bucket_name, \
           "table_name": bird_quantity_table_name, "file_name": bird_quantity_file_name, \
           "delimiter": bird_quantity_delimiter, "skip_leading_rows": bird_quantity_skip_leading_rows, \
           "schema_full": bird_quantity_schema_full, "schema": bird_quantity_schema},
    dag=dag)


# load Nutrition
bird_nutrition_table_name = 'bird_nutrition'
bird_nutrition_file_name = 'raw/Nutrition.csv'
bird_nutrition_delimiter = ","
bird_nutrition_skip_leading_rows = 1

bird_nutrition_schema_full = [
  {"name": "recipe_id", "type": "INTEGER", "mode": "REQUIRED"},
  {"name": "protien", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "carbo", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "alcohol", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "total_fat", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "sat_fat", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "cholestrl", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "sodium", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "iron", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "vitamin_c", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "vitamin_a", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "fiber", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "pcnt_cal_carb", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "pcnt_cal_fat", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "pcnt_cal_prot", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "calories", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "load_time", "type": "TIMESTAMP", "mode": "REQUIRED", "default_value_expression": "CURRENT_TIMESTAMP"}
]


bird_nutrition_schema = [
  {"name": "recipe_id", "type": "INTEGER", "mode": "REQUIRED"},
  {"name": "protien", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "carbo", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "alcohol", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "total_fat", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "sat_fat", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "cholestrl", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "sodium", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "iron", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "vitamin_c", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "vitamin_a", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "fiber", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "pcnt_cal_carb", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "pcnt_cal_fat", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "pcnt_cal_prot", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "calories", "type": "FLOAT64", "mode": "NULLABLE"},
]


bird_nutrition = TriggerDagRunOperator(
    task_id="bird_nutrition",
    trigger_dag_id="p5-ingest-table",  
    conf={"project_id": project_id, "dataset_name": dataset_name, "bucket_name": bucket_name, \
           "table_name": bird_nutrition_table_name, "file_name": bird_nutrition_file_name, \
           "delimiter": bird_nutrition_delimiter, "skip_leading_rows": bird_nutrition_skip_leading_rows, \
           "schema_full": bird_nutrition_schema_full, "schema": bird_nutrition_schema},
    dag=dag)


# load meals
faker_journalists_table_name = 'faker_journalists'
faker_journalists_file_name = 'raw/faker_recipe_journalists.csv'
faker_journalists_delimiter = ","
faker_journalists_skip_leading_rows = 1

faker_journalists_schema_full = [
  {"name": "author_id", "type": "INTEGER", "mode": "REQUIRED"},
  {"name": "name", "type": "STRING", "mode": "NULLABLE"},
  {"name": "age", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "phone_number", "type": "STRING", "mode": "NULLABLE"},
  {"name": "state", "type": "STRING", "mode": "NULLABLE"},
  {"name": "load_time", "type": "TIMESTAMP", "mode": "REQUIRED", "default_value_expression": "CURRENT_TIMESTAMP"},
]


faker_journalists_schema = [
  {"name": "author_id", "type": "INTEGER", "mode": "REQUIRED"},
  {"name": "name", "type": "STRING", "mode": "NULLABLE"},
  {"name": "age", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "phone_number", "type": "STRING", "mode": "NULLABLE"},
  {"name": "state", "type": "STRING", "mode": "NULLABLE"},
]


faker_journalists = TriggerDagRunOperator(
    task_id="faker_journalists",
    trigger_dag_id="p5-ingest-table",  
    conf={"project_id": project_id, "dataset_name": dataset_name, "bucket_name": bucket_name, \
           "table_name": faker_journalists_table_name, "file_name": faker_journalists_file_name, \
           "delimiter": faker_journalists_delimiter, "skip_leading_rows": faker_journalists_skip_leading_rows, \
           "schema_full": faker_journalists_schema_full, "schema": faker_journalists_schema},
    dag=dag)


# load snacks
recipe_at_table_name = 'recipe_at'
recipe_at_file_name = 'raw/recipe_at.csv'
recipe_at_delimiter = ","
recipe_at_skip_leading_rows = 1

recipe_at_schema_full = [
  {"name": "name", "type": "STRING", "mode": "NULLABLE"},
  {"name": "rating", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "ease_of_prep", "type": "STRING", "mode": "NULLABLE"},
  {"name": "note", "type": "STRING", "mode": "NULLABLE"},
  {"name": "type", "type": "STRING", "mode": "NULLABLE"},
  {"name": "prep_time", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "cookbook", "type": "STRING", "mode": "NULLABLE"},
  {"name": "page", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "ingredients", "type": "STRING", "mode": "NULLABLE"},
  {"name": "slowcooker", "type": "STRING", "mode": "NULLABLE"},
  {"name": "link", "type": "STRING", "mode": "NULLABLE"},
  {"name": "last_made", "type": "STRING", "mode": "NULLABLE"},
  {"name": "load_time", "type": "TIMESTAMP", "mode": "NULLABLE", "default_value_expression": "CURRENT_TIMESTAMP"},
]


recipe_at_schema = [
  {"name": "name", "type": "STRING", "mode": "NULLABLE"},
  {"name": "rating", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "ease_of_prep", "type": "STRING", "mode": "NULLABLE"},
  {"name": "note", "type": "STRING", "mode": "NULLABLE"},
  {"name": "type", "type": "STRING", "mode": "NULLABLE"},
  {"name": "prep_time", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "cookbook", "type": "STRING", "mode": "NULLABLE"},
  {"name": "page", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "ingredients", "type": "STRING", "mode": "NULLABLE"},
  {"name": "slowcooker", "type": "STRING", "mode": "NULLABLE"},
  {"name": "link", "type": "STRING", "mode": "NULLABLE"},
  {"name": "last_made", "type": "STRING", "mode": "NULLABLE"},
]


recipe_at = TriggerDagRunOperator(
    task_id="recipe_at",
    trigger_dag_id="p5-ingest-table",  
    conf={"project_id": project_id, "dataset_name": dataset_name, "bucket_name": bucket_name, \
           "table_name": recipe_at_table_name, "file_name": recipe_at_file_name, \
           "delimiter": recipe_at_delimiter, "skip_leading_rows": recipe_at_skip_leading_rows, \
           "schema_full": recipe_at_schema_full, "schema": recipe_at_schema},
    dag=dag)


end = DummyOperator(task_id="end", dag=dag)

start >> create_dataset >> bird_recipes >> end
start >> create_dataset >> bird_ingredients >> end
start >> create_dataset >> bird_quantity >> end
start >> create_dataset >> bird_nutrition >> end
start >> create_dataset >> faker_journalists >> end
start >> create_dataset >> recipe_at >> end


