import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteTableOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

project_id = 'shidcs329e'
stg_dataset_name = 'magazine_recipes_stg_af'
csp_dataset_name = 'magazine_recipes_csp_af'
region = 'us-central1'

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0
}

dag = DAG(
    dag_id='target-controller',
    default_args=default_args,
    description='target table controller dag',
    schedule_interval=None,
    max_active_runs=1,
    concurrency=3,
    catchup=False,
    dagrun_timeout=None,
)

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)


# create csp dataset
create_dataset_csp = BigQueryCreateEmptyDatasetOperator(
    task_id='create_dataset_csp',
    project_id=project_id,
    dataset_id=csp_dataset_name,
    location=region,
    if_exists='ignore',
    dag=dag)

# create Recipes
recipes_sql = (
f"""
create or replace table {csp_dataset_name}.Recipes(
  recipe_id INT64 not null,
  title STRING,
  subtitle STRING,
  servings INT64,
  yield_unit STRING,
  prep_min INT64,
  cook_min INT64,
  stnd_min INT64,
  source STRING,
  intro STRING,
  directions STRING,
  rating INT64,
  ease_of_prep STRING,
  note STRING,
  type STRING,
  page INT64,
  slowcooker STRING,
  link STRING,
  last_made DATE,
  data_source STRING not null,
  load_time	TIMESTAMP not null,
  effective_time TIMESTAMP default current_timestamp() not null,
  discontinue_time TIMESTAMP,
  status_flag BOOL not null,
  primary key (recipe_id, effective_time) not enforced)
  as select *, current_timestamp(), null, true
  from {stg_dataset_name}.Recipes
""")

create_recipes_csp = BigQueryInsertJobOperator(
    task_id="create_recipes_csp",
    configuration={
        "query": {
            "query": recipes_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# create Quantity
quantity_sql = (
f"""
create or replace table {csp_dataset_name}.Quantity(
  quantity_id	INT64 not null,
  recipe_id INT64 not null,
  ingredient_id INT64 not null,
  max_qty FLOAT64,
  min_qty FLOAT64,
  unit STRING,
  preparation STRING,
  optional BOOLEAN,
  data_source STRING not null,
  load_time	TIMESTAMP not null,
  effective_time TIMESTAMP default current_timestamp() not null,
  discontinue_time TIMESTAMP,
  status_flag BOOL not null,
  primary key (quantity_id, effective_time) not enforced)
  as select *, current_timestamp(), null, true
  from {stg_dataset_name}.Quantity
"""
)

create_quantity_csp = BigQueryInsertJobOperator(
    task_id="create_quantity_csp",
    configuration={
        "query": {
            "query": quantity_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# create Publications
publications_sql = (
f"""
create or replace table {csp_dataset_name}.Publications(
  publication_id INT64 not null,
  recipe_id INT64 not null,
  magazine_id	INT64 not null,
  journalist_id INT64 not null,
  date DATE,
  volume INT64,
  issue INT64,
  publication_type STRING,
  data_source STRING,
  load_time	TIMESTAMP not null,
  effective_time TIMESTAMP default current_timestamp() not null,
  discontinue_time TIMESTAMP,
  status_flag BOOL not null,
  primary key (publication_id, effective_time) not enforced)
  as select *, current_timestamp(), null, true
  from {stg_dataset_name}.Publications
"""
)

create_publications_csp = BigQueryInsertJobOperator(
    task_id="create_publications_csp",
    configuration={
        "query": {
            "query": publications_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# create Nutrition
nutrition_sql = (
f"""
create or replace table {csp_dataset_name}.Nutrition(
  recipe_id	INT64 not null,
  protien FLOAT64 not null,
  carbo FLOAT64 not null,
  alcohol FLOAT64 not null,
  total_fat FLOAT64 not null,
  sat_fat FLOAT64 not null,
  cholestrl FLOAT64 not null,
  sodium FLOAT64 not null,
  iron FLOAT64 not null,
  vitamin_c FLOAT64 not null,
  vitamin_a FLOAT64 not null,
  fiber FLOAT64 not null,
  pcnt_cal_carb FLOAT64 not null,
  pcnt_cal_fat FLOAT64 not null,
  pcnt_cal_prot FLOAT64 not null,
  calories FLOAT64 not null,
  data_source STRING not null,
  load_time	TIMESTAMP not null,
  effective_time TIMESTAMP default current_timestamp() not null,
  discontinue_time TIMESTAMP,
  status_flag BOOL not null,
  primary key (recipe_id, effective_time) not enforced)
  as select *, current_timestamp(), null, true
  from {stg_dataset_name}.Nutrition
"""
)

create_nutrition_csp = BigQueryInsertJobOperator(
    task_id="create_nutrition_csp",
    configuration={
        "query": {
            "query": nutrition_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

    
# create Magazines
magazines_sql = (
f"""
create or replace table {csp_dataset_name}.Magazines(
  magazine_id	INT64 not null,
  magazine_name STRING,
  website STRING,
  pub_frequency_weeks INT64,
  publishing_company STRING,
  subscription_price INT64,
  data_source STRING,
  load_time	TIMESTAMP not null,
  effective_time TIMESTAMP default current_timestamp() not null,
  discontinue_time TIMESTAMP,
  status_flag BOOL not null,
  primary key (magazine_id, effective_time) not enforced)
  as select *, current_timestamp(), null, true
  from {stg_dataset_name}.Magazines
"""
)

create_magazines_csp = BigQueryInsertJobOperator(
    task_id="create_magazines_csp",
    configuration={
        "query": {
            "query": magazines_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)
    

# create Journalists
journalists_sql = (
f"""
create or replace table {csp_dataset_name}.Journalists(
  journalist_id	INT64 not null,
  f_name STRING not null,
  l_name STRING not null,
  age INT64 not null,
  phone STRING not null,
  state STRING not null,
  data_source STRING not null,
  load_time	TIMESTAMP not null,
  effective_time TIMESTAMP default current_timestamp() not null,
  discontinue_time TIMESTAMP,
  status_flag BOOL not null,
  primary key (journalist_id, effective_time) not enforced)
  as select *, current_timestamp(), null, true
  from {stg_dataset_name}.Journalists
"""
)

create_journalists_csp = BigQueryInsertJobOperator(
    task_id="create_journalists_csp",
    configuration={
        "query": {
            "query": journalists_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# create Ingredients
ingredients_sql = (
f"""
create or replace table {csp_dataset_name}.Ingredients(
  ingredient_id	INT64 not null,
  category STRING,
  name STRING,
  plural STRING,
  data_source STRING not null,
  load_time	TIMESTAMP not null,
  effective_time TIMESTAMP default current_timestamp() not null,
  discontinue_time TIMESTAMP,
  status_flag BOOL not null,
  primary key (ingredient_id, effective_time) not enforced)
  as select *, current_timestamp(), null, true
  from {stg_dataset_name}.Ingredients
"""
)

create_ingredients_csp = BigQueryInsertJobOperator(
    task_id="create_ingredients_csp",
    configuration={
        "query": {
            "query": ingredients_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)
    

start >> create_dataset_csp >> create_recipes_csp >> end
start >> create_dataset_csp >> create_quantity_csp >> end
start >> create_dataset_csp >> create_publications_csp >> end
start >> create_dataset_csp >> create_nutrition_csp >> end
start >> create_dataset_csp >> create_magazines_csp >> end
start >> create_dataset_csp >> create_journalists_csp >> end
start >> create_dataset_csp >> create_ingredients_csp >> end

