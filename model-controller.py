import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteTableOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.utils.dates import days_ago
from datetime import timedelta
from pendulum import duration

project_id = 'shidcs329e'
raw_dataset_name = 'magazine_recipes_raw_af'
stg_dataset_name = 'magazine_recipes_stg_af'
region = 'us-central1'

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0
}

dag = DAG(
    dag_id='model-controller',
    default_args=default_args,
    description='controller dag',
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
)

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)
wait = TimeDeltaSensor(task_id="wait", delta=duration(seconds=5), dag=dag)
before_deletes = DummyOperator(task_id = "before_deletes", dag = dag)


# create stg dataset
create_dataset_stg = BigQueryCreateEmptyDatasetOperator(
    task_id='create_dataset_stg',
    project_id=project_id,
    dataset_id=stg_dataset_name,
    location=region,
    if_exists='ignore',
    dag=dag)

nutrition_sql = (
f''' CREATE OR REPLACE TABLE {stg_dataset_name}.Nutrition AS
select * except(load_time), 'bird' as data_source, load_time from {raw_dataset_name}.bird_nutrition''')


create_nutrition_stg = BigQueryInsertJobOperator(
    task_id="create_nutrition_stg",
    configuration={
        "query": {
            "query": nutrition_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

create_ingredient_decomposition_stg = TriggerDagRunOperator(
    task_id = 'create_ingredient_decomposition_stg',
    trigger_dag_id = 'ingredients-at-decomp',
    conf = {'project_id':project_id, 'region': region, 'stg_dataset_name' : stg_dataset_name}, dag = dag)


ingredients_sql = (
    """
        CREATE OR REPLACE TABLE shidcs329e.magazine_recipes_stg_af.ingredients AS
        SELECT
          recipe_id, ingredient_1 AS ingredient_name, load_time
        FROM
          shidcs329e.magazine_recipes_stg_af.recipe_ingredient_at
        WHERE
          ingredient_1 IS NOT NULL

        UNION ALL

        SELECT
          recipe_id, ingredient_2 AS ingredient_name, load_time
        FROM
          shidcs329e.magazine_recipes_stg_af.recipe_ingredient_at
        WHERE
          ingredient_2 IS NOT NULL

        UNION ALL

        SELECT
          recipe_id, ingredient_3 AS ingredient_name, load_time
        FROM
          shidcs329e.magazine_recipes_stg_af.recipe_ingredient_at
        WHERE
          ingredient_3 IS NOT NULL

        UNION ALL

        SELECT
          recipe_id, ingredient_4 AS ingredient_name, load_time
        FROM
          shidcs329e.magazine_recipes_stg_af.recipe_ingredient_at
        WHERE
          ingredient_4 IS NOT NULL

        UNION ALL

        SELECT
          recipe_id, ingredient_5 AS ingredient_name, load_time
        FROM
          shidcs329e.magazine_recipes_stg_af.recipe_ingredient_at
        WHERE
          ingredient_5 IS NOT NULL

        UNION ALL

        SELECT
          recipe_id, ingredient_6 AS ingredient_name, load_time
        FROM
          shidcs329e.magazine_recipes_stg_af.recipe_ingredient_at
        WHERE
          ingredient_6 IS NOT NULL

        UNION ALL

        SELECT
          recipe_id, ingredient_7 AS ingredient_name, load_time
        FROM
          shidcs329e.magazine_recipes_stg_af.recipe_ingredient_at
        WHERE
          ingredient_7 IS NOT NULL;""")

create_ingredients_stg = BigQueryInsertJobOperator(
task_id="create_ingredients_stg",
configuration={
    "query": {
        "query": ingredients_sql,
        "useLegacySql": False,
    }
},
location=region,
dag=dag)


unique_ingredient_with_id_sql = (
    '''CREATE OR REPLACE TABLE shidcs329e.magazine_recipes_stg_af.unique_ingredient_with_id AS
        select distinct LOWER(si.ingredient_name) as ingredient_name, ri.ingredient_id, si.load_time
        from magazine_recipes_stg_af.ingredients as si
        left join magazine_recipes_raw_af.bird_ingredients as ri
        on LOWER(si.ingredient_name) = ri.name;''')

create_unique_ingredient_with_id_stg = BigQueryInsertJobOperator(
task_id="create_unique_ingredient_with_id_stg",
configuration={
    "query": {
        "query": unique_ingredient_with_id_sql,
        "useLegacySql": False,
    }
},
location=region,
dag=dag)


ingredient_id_no_null_sql = (
    '''CREATE OR REPLACE TABLE shidcs329e.magazine_recipes_stg_af.ingredient_id_no_nulls AS
        SELECT ingredient_name, COALESCE(ingredient_id, 4645 + ROW_NUMBER() OVER()) as ingredient_id, load_time FROM magazine_recipes_stg_af.unique_ingredient_with_id;''')

create_ingredient_id_no_null_stg = BigQueryInsertJobOperator(
task_id="create_ingredient_id_no_null_stg",
configuration={
    "query": {
        "query": ingredient_id_no_null_sql,
        "useLegacySql": False,
    }
},
location=region,
dag=dag)


ingredients_at_sql = (
    '''CREATE OR REPLACE TABLE magazine_recipes_stg_af.ingredients_at AS
    select ingredient_id, ingredient_name, "airtable" as data_source, load_time FROM magazine_recipes_stg_af.ingredient_id_no_nulls;''')


create_ingredients_at_stg = BigQueryInsertJobOperator(
task_id="create_ingredients_at_stg",
configuration={
    "query": {
        "query": ingredients_at_sql,
        "useLegacySql": False,
    }
},
location=region,
dag=dag)


quantity_at_sql = (
    '''CREATE OR REPLACE TABLE shidcs329e.magazine_recipes_stg_af.quantity_at AS
        select (6400 + ROW_NUMBER() OVER()) as quantity_id, si.recipe_id, iinn.ingredient_id, 'airtable' as data_source, iinn.load_time
        from magazine_recipes_stg_af.ingredient_id_no_nulls as iinn
        RIGHT JOIN magazine_recipes_stg_af.ingredients as si
        ON LOWER(iinn.ingredient_name) = LOWER(si.ingredient_name);''')

create_quantity_at_stg = BigQueryInsertJobOperator(
task_id="create_quantity_at_stg",
configuration={
    "query": {
        "query": quantity_at_sql,
        "useLegacySql": False,
    }
},
location=region,
dag=dag)



recipes_at_sql = (
    '''CREATE OR REPLACE TABLE magazine_recipes_stg_af.recipes_at AS
        SELECT recipe_id, name, rating, ease_of_prep, note, type, prep_time, cookbook, page, slowcooker,
        link, last_made, "airtable" as data_source, load_time FROM magazine_recipes_stg_af.recipe_ingredient_at''')

create_recipes_at_stg = BigQueryInsertJobOperator(
task_id="create_recipes_at_stg",
configuration={
    "query": {
        "query": recipes_at_sql,
        "useLegacySql": False,
    }
},
location=region,
dag=dag)




recipes_at_union_sql = (
    '''CREATE OR REPLACE TABLE magazine_recipes_stg_af.recipes_at_union AS
    SELECT recipe_id, name as title, CAST(NULL as STRING) as subtitle, CAST(NULL as INTEGER) as servings, CAST(NULL as STRING) yield_unit,
    prep_time as prep_min, CAST(NULL as INTEGER) as cook_min, CAST(NULL as INTEGER) as stnd_min, cookbook as source,
    CAST(NULL as STRING) as intro, CAST(NULL as STRING) as directions, rating, ease_of_prep, note, type, page, slowcooker,
    link, PARSE_DATE('%m/%d/%Y', last_made) as last_made, 'airtable' as data_source, load_time
    FROM magazine_recipes_stg_af.recipes_at;''')

create_recipes_at_union_stg = BigQueryInsertJobOperator(
task_id="create_recipes_at_union_stg",
configuration={
    "query": {
        "query": recipes_at_union_sql,
        "useLegacySql": False,
    }
},
location=region,
dag=dag)


recipes_bird_union_sql = (
    '''CREATE OR REPLACE TABLE magazine_recipes_stg_af.recipes_bird_union AS
        SELECT recipe_id, title, subtitle, servings, yield_unit, prep_min, cook_min, stnd_min, source, intro, directions,
        CAST(NULL as INTEGER) as rating, CAST(NULL as STRING) as ease_of_prep, CAST(NULL as STRING) as note,  CAST(NULL as STRING) as type,
        CAST(NULL as INTEGER) page, CAST(NULL as STRING) slowcooker, CAST(NULL as STRING) link, CAST(NULL as DATE) last_made,
        'bird' as data_source, load_time
        FROM magazine_recipes_raw_af.bird_recipes;''')

create_recipes_bird_union_stg = BigQueryInsertJobOperator(
task_id="create_recipes_bird_union_stg",
configuration={
    "query": {
        "query": recipes_bird_union_sql,
        "useLegacySql": False,
    }
},
location=region,
dag=dag)

merge_recipes_sql = (
    '''CREATE OR REPLACE TABLE magazine_recipes_stg_af.Recipes AS
        SELECT * FROM magazine_recipes_stg_af.recipes_bird_union
        UNION ALL
        SELECT * FROM magazine_recipes_stg_af.recipes_at_union;''')

merge_recipes_stg = BigQueryInsertJobOperator(
task_id="merge_recipes_stg",
configuration={
    "query": {
        "query": merge_recipes_sql,
        "useLegacySql": False,
    }
},
location=region,
dag=dag)

merge_quantity_sql = (
    '''CREATE OR REPLACE TABLE magazine_recipes_stg_af.Quantity as
        SELECT quantity_id, recipe_id, ingredient_id, CAST(NULL as FLOAT64) as max_qty, CAST(NULL as FLOAT64) as min_qty,
        CAST(NULL as STRING) as unit, CAST(NULL as STRING) as preparation, CAST(NULL as BOOLEAN) as optional, data_source, load_time
        FROM magazine_recipes_stg_af.quantity_at
        UNION ALL
        SELECT * except(load_time), 'bird' as data_source, load_time
        FROM magazine_recipes_raw_af.bird_quantity;''')

merge_quantity_stg = BigQueryInsertJobOperator(
task_id="merge_quantity_stg",
configuration={
    "query": {
        "query": merge_quantity_sql,
        "useLegacySql": False,
    }
},
location=region,
dag=dag)


dual_ingredients_sql = (
    '''CREATE OR REPLACE TABLE magazine_recipes_stg_af.dual_ingredients as
    SELECT a.ingredient_id, a.ingredient_name, b.category, b.plural, b.load_time, 'bird-airtable' as data_source,
    FROM magazine_recipes_stg_af.ingredients_at a
    JOIN magazine_recipes_raw_af.bird_ingredients b
    on a.ingredient_id = b.ingredient_id;''')

dual_ingredients_stg = BigQueryInsertJobOperator(
task_id="dual_ingredients_stg",
configuration={
    "query": {
        "query": dual_ingredients_sql,
        "useLegacySql": False,
    }
},
location=region,
dag=dag)

bird_ingredients_sql = (
    '''CREATE OR REPLACE TABLE magazine_recipes_stg_af.bird_ingredients AS
    SELECT * EXCEPT(load_time), 'bird' as data_source, load_time FROM magazine_recipes_raw_af.bird_ingredients;''')

bird_ingredients_stg = BigQueryInsertJobOperator(
task_id="bird_ingredients_stg",
configuration={
    "query": {
        "query": bird_ingredients_sql,
        "useLegacySql": False,
    }
},
location=region,
dag=dag)

bird_ingredient_update_sql = (
    '''UPDATE magazine_recipes_stg_af.bird_ingredients
    SET data_source = 'bird-airtable'
    WHERE ingredient_id in (select ingredient_id from magazine_recipes_stg_af.dual_ingredients);''')

bird_ingredients_update_stg = BigQueryInsertJobOperator(
task_id="bird_ingredients_update_stg",
configuration={
    "query": {
        "query": bird_ingredient_update_sql,
        "useLegacySql": False,
    }
},
location=region,
dag=dag)

ingredients_at_union_sql = (
    '''CREATE TABLE magazine_recipes_stg_af.ingredients_at_union as
        SELECT * FROM magazine_recipes_stg_af.ingredients_at
        WHERE ingredient_id not in (select ingredient_id from magazine_recipes_stg_af.dual_ingredients);''')

ingredients_at_union_stg = BigQueryInsertJobOperator(
task_id="ingredients_at_union_stg",
configuration={
    "query": {
        "query": ingredients_at_union_sql,
        "useLegacySql": False,
    }
},
location=region,
dag=dag)


merge_ingredients_sql = (
    '''CREATE OR REPLACE TABLE magazine_recipes_stg_af.Ingredients as
        SELECT * FROM magazine_recipes_stg_af.bird_ingredients
        UNION ALL
        SELECT ingredient_id, ingredient_name, CAST(NULL AS STRING) AS category,CAST(NULL AS STRING) as plural, data_source, load_time
        FROM magazine_recipes_stg_af.ingredients_at_union;''')


merge_ingredients_stg = BigQueryInsertJobOperator(
task_id="merge_ingredients_stg",
configuration={
    "query": {
        "query": merge_ingredients_sql,
        "useLegacySql": False,
    }
},
location=region,
dag=dag)


# delete snacks (intermediate table)
delete_ingredients_stg = BigQueryDeleteTableOperator(
    task_id="delete_ingredients_stg",
    deletion_dataset_table=f"{project_id}.{stg_dataset_name}.ingredients",
    location=region,
    dag=dag)

delete_unique_ingredient_with_id_stg = BigQueryDeleteTableOperator(
    task_id="delete_unique_ingredient_with_id_stg",
    deletion_dataset_table=f"{project_id}.{stg_dataset_name}.unique_ingredient_with_id",
    location=region,
    dag=dag)

delete_ingredient_id_no_nulls_stg = BigQueryDeleteTableOperator(
    task_id="delete_ingredient_id_no_nulls_stg",
    deletion_dataset_table=f"{project_id}.{stg_dataset_name}.ingredient_id_no_nulls",
    location=region,
    dag=dag)

delete_ingredients_at_stg = BigQueryDeleteTableOperator(
    task_id="delete_ingredients_at_stg",
    deletion_dataset_table=f"{project_id}.{stg_dataset_name}.ingredients_at",
    location=region,
    dag=dag)

delete_quantity_at_stg = BigQueryDeleteTableOperator(
    task_id="delete_quantity_at_stg",
    deletion_dataset_table=f"{project_id}.{stg_dataset_name}.quantity_at",
    location=region,
    dag=dag)

delete_recipes_at_stg = BigQueryDeleteTableOperator(
    task_id="delete_recipes_at_stg",
    deletion_dataset_table=f"{project_id}.{stg_dataset_name}.recipes_at",
    location=region,
    dag=dag)

delete_recipes_at_union_stg = BigQueryDeleteTableOperator(
    task_id="delete_recipes_at_union_stg",
    deletion_dataset_table=f"{project_id}.{stg_dataset_name}.recipes_at_union",
    location=region,
    dag=dag)

delete_recipes_bird_union_stg = BigQueryDeleteTableOperator(
    task_id="delete_recipes_bird_union_stg",
    deletion_dataset_table=f"{project_id}.{stg_dataset_name}.recipes_bird_union",
    location=region,
    dag=dag)

delete_dual_ingredients_stg = BigQueryDeleteTableOperator(
    task_id="delete_dual_ingredients_stg",
    deletion_dataset_table=f"{project_id}.{stg_dataset_name}.dual_ingredients",
    location=region,
    dag=dag)

delete_bird_ingredients_stg = BigQueryDeleteTableOperator(
    task_id="delete_bird_ingredients_stg",
    deletion_dataset_table=f"{project_id}.{stg_dataset_name}.bird_ingredients",
    location=region,
    dag=dag)

delete_ingredients_at_union_stg = BigQueryDeleteTableOperator(
    task_id="delete_ingredients_at_union_stg",
    deletion_dataset_table=f"{project_id}.{stg_dataset_name}.ingredients_at_union",
    location=region,
    dag=dag)

delete_recipe_ingredient_at_stg = BigQueryDeleteTableOperator(
    task_id="delete_recipe_ingredient_at_stg",
    deletion_dataset_table=f"{project_id}.{stg_dataset_name}.recipe_ingredient_at",
    location=region,
    dag=dag)


decomp_journalists_sql = (
    '''create or replace table magazine_recipes_stg_af.Journalists as
        select journalist_id, name_array[0] as f_name, name_array[1] as l_name, age, phone, state, 'faker' as data_source, load_time
        from
        (select author_id as journalist_id, age, phone_number as phone, state, split(name, ' ') as name_array, load_time
        from magazine_recipes_raw_af.faker_journalists);''')


decomp_journalists_stg = BigQueryInsertJobOperator(
task_id="decomp_journalists_stg",
configuration={
    "query": {
        "query": decomp_journalists_sql,
        "useLegacySql": False,
    }
},
location=region,
dag=dag)

create_magazine_stg = TriggerDagRunOperator(
    task_id = 'create_magazine_stg',
    trigger_dag_id = 'create_magazine',
    conf = {'project_id':project_id, 'region': region, 'stg_dataset_name' : stg_dataset_name}, dag = dag)

create_publisher_stg = TriggerDagRunOperator(
    task_id = 'create_publisher_stg',
    trigger_dag_id = 'create-publisher',
    conf = {'project_id':project_id, 'region': region, 'stg_dataset_name' : stg_dataset_name}, dag = dag)




start >> create_dataset_stg >> create_nutrition_stg >> end
start >> create_dataset_stg >> create_ingredient_decomposition_stg  >> wait >> create_ingredients_stg >> create_unique_ingredient_with_id_stg >> create_ingredient_id_no_null_stg >> \
create_ingredients_at_stg >> create_recipes_at_stg >> create_quantity_at_stg >> create_recipes_at_union_stg >> create_recipes_bird_union_stg >> merge_recipes_stg >> merge_quantity_stg >>\
dual_ingredients_stg >> bird_ingredients_stg >> bird_ingredients_update_stg >> ingredients_at_union_stg >> merge_ingredients_stg >> before_deletes >>  end

start >> decomp_journalists_stg >> create_magazine_stg >> wait >> create_publisher_stg >> end  
before_deletes >> delete_ingredients_stg >> end
before_deletes >> delete_unique_ingredient_with_id_stg >> end
before_deletes >> delete_recipes_bird_union_stg >> end
before_deletes >> delete_ingredient_id_no_nulls_stg >> end
before_deletes >> delete_ingredients_at_stg >> end
before_deletes >> delete_quantity_at_stg >> end
before_deletes >> delete_recipes_at_stg >> end
before_deletes >> delete_recipes_at_union_stg >> end
before_deletes >> delete_dual_ingredients_stg >> end
before_deletes >> delete_bird_ingredients_stg >> end
before_deletes >> delete_ingredients_at_union_stg >> end
before_deletes >> delete_recipe_ingredient_at_stg >> end

    

