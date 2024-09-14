import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteTableOperator
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
    dag_id='ai-controller',
    default_args=default_args,
    description='ai controller dag',
    schedule_interval=None,
    max_active_runs=1,
    concurrency=1,
    catchup=False,
    dagrun_timeout=None,
)

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)
wait = TimeDeltaSensor(task_id="wait", delta=duration(seconds=30), dag=dag)

project_id = 'shidcs329e'
ai_stg_dataset_name = 'magazine_recipes_stg_af_ai'
stg_dataset_name = 'magazine_recipes_stg_af'
csp_dataset_name = 'magazine_recipes_csp_af'
region = 'us-central1'

# create magazine_recipes_stg_af_ai

# create stg dataset
create_dataset_stg_af_ai = BigQueryCreateEmptyDatasetOperator(
    task_id='create_dataset_stg_af_ai',
    project_id=project_id,
    dataset_id=ai_stg_dataset_name,
    location=region,
    if_exists='ignore',
    dag=dag)



# create recipes_directions

recipes_directions_sql = (f"""
declare prompt_query STRING default " Create directions based on the ingredients and title of the recipe. All in one line, no numbering of steps. Return output as json with recipe_id, title, ingredients, and directions. Give best attempt at creating directions, no null directions.";
create or replace table {ai_stg_dataset_name}.recipes_directions as
select *
from ML.generate_text(
  model remote_model_central.gemini_pro,
  (
    SELECT concat(prompt_query, to_json_string(json_object("recipe_id", recipe_id, "title", title, "ingredients", all_ingredients))) as prompt
     FROM (
     SELECT
      r.recipe_id,
      r.title,
      STRING_AGG(i.name, ', ') AS all_ingredients
    FROM
      {stg_dataset_name}.Recipes AS r
    LEFT JOIN {stg_dataset_name}.Quantity AS q
    ON q.recipe_id = r.recipe_id
    LEFT JOIN {stg_dataset_name}.Ingredients AS i
    ON i.ingredient_id = q.ingredient_id
    WHERE r.directions IS NULL
    AND r.title NOT LIKE '-%-'
    GROUP BY
      r.recipe_id, r.title)
  ),
  struct(TRUE as flatten_json_output)
)
"""
)

create_recipes_directions_csp = BigQueryInsertJobOperator(
    task_id="create_recipes_directions_csp",
    configuration={
        "query": {
            "query": recipes_directions_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# change json format for recipes_directions
recipes_directions_json_format_sql = (f"""
create or replace table  {ai_stg_dataset_name}.recipes_directions_json_format as
select trim(replace(replace(replace(replace(replace(ml_generate_text_llm_result, '```json', ''), '```', ''), '\\\\n', ''), ']', ''), '[' , '')) as ml_generate_text_llm_result
from {ai_stg_dataset_name}.recipes_directions;
""")

create_recipes_directions_json_format_csp = BigQueryInsertJobOperator(
    task_id="create_recipes_directions_json_format_csp",
    configuration={
        "query": {
            "query": recipes_directions_json_format_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# close the json for recipes_directions_format_sql 
recipes_directions_closed_sql =(""" 
UPDATE  magazine_recipes_stg_af_ai.recipes_directions_json_format 
set ml_generate_text_llm_result = concat(ml_generate_text_llm_result, '"}')
where ml_generate_text_llm_result not like '%"}%';
""")

create_recipes_directions_closed_csp = BigQueryInsertJobOperator(
    task_id="create_recipes_directions_closed_csp",
    configuration={
        "query": {
            "query": recipes_directions_closed_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# create directions 

directions_sql = (f"""
CREATE OR REPLACE TABLE {ai_stg_dataset_name}.directions AS
select json_value(ml_generate_text_llm_result, '$.recipe_id') as recipe_id,
  json_value(ml_generate_text_llm_result, '$.title') as title,
  json_value(ml_generate_text_llm_result, '$.directions') as recipe_directions,
  json_value(ml_generate_text_llm_result, '$.ingredients') as database_ingredients
from {ai_stg_dataset_name}.recipes_directions_json_format
"""
)

create_directions_csp = BigQueryInsertJobOperator(
    task_id="create_directions_csp",
    configuration={
        "query": {
            "query": directions_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# create recipes_directions_for_nulls 

recipes_directions_for_nulls_sql = (f"""
declare prompt_query STRING default " You must write directions for the following recipe WITH LESS THAN 30 WORDS. All in one line, no numbering of steps. Return output with recipe_id, title, and directions as a VALID CLOSED JSON. ";
create or replace table {ai_stg_dataset_name}.recipes_directions_for_nulls as
select *
from ML.generate_text(
  model remote_model_central.gemini_pro,
  (
    SELECT concat(prompt_query, to_json_string(json_object("recipe_id", recipe_id, "title", title))) as prompt
     FROM (
     SELECT
      recipe_id,
      title,
    FROM
    {ai_stg_dataset_name}.directions
    WHERE recipe_directions IS NULL)
  ),
  struct(TRUE as flatten_json_output)
);
"""
)

create_recipes_directions_for_nulls_csp = BigQueryInsertJobOperator(
    task_id="create_recipes_directions_for_nulls_csp",
    configuration={
        "query": {
            "query": recipes_directions_for_nulls_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# change json format for recipes_directions_for_nulls

recipes_directions_for_nulls_json_format_sql = (f"""
create or replace table  {ai_stg_dataset_name}.recipes_directions_for_nulls_json_format as
select trim(replace(replace(replace(replace(replace(ml_generate_text_llm_result, '```json', ''), '```', ''), '\\\\n', ''), ']', ''), '[' , '')) as ml_generate_text_llm_result
from {ai_stg_dataset_name}.recipes_directions_for_nulls
""")

create_recipes_directions_for_nulls_json_format_csp = BigQueryInsertJobOperator(
    task_id="create_recipes_directions_for_nulls_json_format_csp",
    configuration={
        "query": {
            "query": recipes_directions_for_nulls_json_format_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# close the json for recipes_directions_for_nulls_format_sql 
recipes_directions_for_nulls_closed_sql =(""" 
UPDATE  magazine_recipes_stg_af_ai.recipes_directions_for_nulls_json_format 
set ml_generate_text_llm_result = concat(ml_generate_text_llm_result, '"}')
where ml_generate_text_llm_result not like '%"}%'
""")

create_recipes_directions_for_nulls_closed_csp = BigQueryInsertJobOperator(
    task_id="create_recipes_directions_for_nulls_closed_csp",
    configuration={
        "query": {
            "query": recipes_directions_for_nulls_closed_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# create directions_for_nulls

directions_for_nulls_sql = (f"""
CREATE OR REPLACE TABLE {ai_stg_dataset_name}.directions_for_nulls AS
select json_value(ml_generate_text_llm_result, '$.recipe_id') as recipe_id,
  json_value(ml_generate_text_llm_result, '$.title') as title,
  json_value(ml_generate_text_llm_result, '$.directions') as recipe_directions
from {ai_stg_dataset_name}.recipes_directions_for_nulls_json_format
"""
)

create_directions_for_nulls_csp = BigQueryInsertJobOperator(
    task_id="create_directions_for_nulls_csp",
    configuration={
        "query": {
            "query": directions_for_nulls_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# Update directions with 9 nulls 

update_directions_sql = (f"""
UPDATE {ai_stg_dataset_name}.directions d
SET d.recipe_directions = dn.recipe_directions
FROM {ai_stg_dataset_name}.directions_for_nulls AS dn
WHERE d.recipe_id = dn.recipe_id
"""
)

update_directions_csp = BigQueryInsertJobOperator(
    task_id="update_directions_csp",
    configuration={
        "query": {
            "query": update_directions_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# create directions_ingredients 

directions_ingredients_sql = (f"""
declare prompt_query STRING default "Identify all ingredients mentioned in the directions of the recipe. Return output as json with recipe_id, title, and mentioned_ingredients. ";
create or replace table {ai_stg_dataset_name}.direction_ingredients as
select *
from ML.generate_text(
  model remote_model_central.gemini_pro,
  (
    SELECT concat(prompt_query, to_json_string(json_object("recipe_id", recipe_id, "title", title, "recipe_directions", recipe_directions))) as prompt
     FROM {ai_stg_dataset_name}.directions
  ),
  struct(TRUE as flatten_json_output)
);
"""
)

create_directions_ingredients_csp = BigQueryInsertJobOperator(
    task_id="create_directions_ingredients_csp",
    configuration={
        "query": {
            "query": directions_ingredients_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)



# change json format for direction_ingredients
direction_ingredients_json_format_sql = (f"""
create or replace table  {ai_stg_dataset_name}.direction_ingredients_json_format as
select trim(replace(replace(replace(replace(replace(ml_generate_text_llm_result, '```json', ''), '```', ''), '\\\\n', ''), ']', ''), '[' , '')) as ml_generate_text_llm_result
from {ai_stg_dataset_name}.direction_ingredients
""")

create_direction_ingredients_json_format_csp = BigQueryInsertJobOperator(
    task_id="create_direction_ingredients_json_format_csp",
    configuration={
        "query": {
            "query": direction_ingredients_json_format_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# close the json for direction_ingredients_format_sql 
direction_ingredients_closed_sql =(""" 
UPDATE magazine_recipes_stg_af_ai.direction_ingredients_json_format 
set ml_generate_text_llm_result = concat(ml_generate_text_llm_result, '"}')
where ml_generate_text_llm_result not like '%"}%'
""")

create_direction_ingredients_closed_csp = BigQueryInsertJobOperator(
    task_id="create_direction_ingredients_closed_csp",
    configuration={
        "query": {
            "query": direction_ingredients_closed_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)



# create direction_mentioned_ingredients 

direction_mentioned_ingredients_sql = (f"""
CREATE OR REPLACE TABLE {ai_stg_dataset_name}.direction_mentioned_ingredients AS
select json_value(ml_generate_text_llm_result, '$.recipe_id') as recipe_id,
  json_QUERY(ml_generate_text_llm_result, '$.mentioned_ingredients') as mentioned_ingredients,
from {ai_stg_dataset_name}.direction_ingredients_json_format                                      
"""
)

create_direction_mentioned_ingredients_csp = BigQueryInsertJobOperator(
    task_id="create_direction_mentioned_ingredients_csp",
    configuration={
        "query": {
            "query": direction_mentioned_ingredients_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# create compare_ingredients

compare_ingredients_sql = (f"""
create or replace table {ai_stg_dataset_name}.compare_ingredients as
select d.recipe_id, replace(d.database_ingredients, '\\\'', '') as db_ingredients, REPLACE(REPLACE(REPLACE(di.mentioned_ingredients, '"', ''), ']', ''), '[', '') AS ai_ingredients
from {ai_stg_dataset_name}.directions as d
INNER JOIN {ai_stg_dataset_name}.direction_mentioned_ingredients as di
  ON d.recipe_id = di.recipe_id
"""
)

create_compare_ingredients_csp = BigQueryInsertJobOperator(
    task_id="create_compare_ingredients_csp",
    configuration={
        "query": {
            "query": compare_ingredients_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


#

# Create ingredients_difference

ingredients_difference_sql = (f"""
declare prompt_query STRING default " Identify and return a list of missing_ingredients which contains all elements of the ai_ingredients list that are not present in the db_ingredients list. Output a json document with only the following fields: recipe_id, db_ingredients, ai_ingredients and ingredients_difference.";
CREATE OR REPLACE TABLE {ai_stg_dataset_name}.ingredients_difference AS
select *
from ML.generate_text(
  model remote_model_central.gemini_pro,
  (
    select concat(prompt_query, to_json_string(json_object("recipe_id",recipe_id, "db_ingredients", db_ingredients, "ai_ingredients", ai_ingredients ))) as prompt
    from  {ai_stg_dataset_name}.compare_ingredients
  ),
  struct(TRUE as flatten_json_output)
);
""")

create_ingredients_difference_csp = BigQueryInsertJobOperator(
    task_id="create_ingredients_difference_csp",
    configuration={
        "query": {
            "query": ingredients_difference_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)



# change json format for ingredients_difference
ingredients_difference_json_format_sql = (f"""
create or replace table {ai_stg_dataset_name}.ingredients_difference_json_format as
select trim(replace(replace(replace(replace(replace(ml_generate_text_llm_result, '```json', ''), '```', ''), '\\\\n', ''), ']', ''), '[' , '')) as ml_generate_text_llm_result
from {ai_stg_dataset_name}.ingredients_difference
""")

create_ingredients_difference_json_format_csp = BigQueryInsertJobOperator(
    task_id="create_ingredients_difference_json_format_csp",
    configuration={
        "query": {
            "query": ingredients_difference_json_format_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# close the json for ingredients_difference_format_sql 

# ingredients_difference_closed_sql =(""" 
# UPDATE  magazine_recipes_stg_af_ai.ingredients_difference_json_format 
# set ml_generate_text_llm_result = concat(ml_generate_text_llm_result, '"}')
# where ml_generate_text_llm_result not like '%"}%'
# """)

# create_ingredients_difference_closed_csp = BigQueryInsertJobOperator(
#     task_id="create_ingredients_difference_closed_csp",
#     configuration={
#         "query": {
#             "query": ingredients_difference_closed_sql,
#             "useLegacySql": False,
#         }
#     },
#     location=region,
#     dag=dag)


# create missing_ingredients_list 

missing_ingredients_list_sql = (f"""
CREATE OR REPLACE TABLE {ai_stg_dataset_name}.missing_ingredients_list AS                           
SELECT 
    json_value(ml_generate_text_llm_result, '$.recipe_id') AS recipe_id,
    ARRAY(
        SELECT REPLACE(ingredient, '"', '') 
        FROM UNNEST(SPLIT(json_QUERY(ml_generate_text_llm_result, '$.ingredients_difference'), ',')) AS ingredient
    ) AS missing_ingredients
FROM 
    {ai_stg_dataset_name}.ingredients_difference_json_format
"""
)

create_missing_ingredients_list_csp = BigQueryInsertJobOperator(
    task_id="create_missing_ingredients_list_csp",
    configuration={
        "query": {
            "query": missing_ingredients_list_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# create missing_ingredients_unnest 

missing_ingredients_unnest_sql = (f"""
CREATE OR REPLACE TABLE {ai_stg_dataset_name}.missing_ingredients_unnest AS
SELECT
  recipe_id,
  ingredient
FROM
{ai_stg_dataset_name}.missing_ingredients_list
CROSS JOIN
  UNNEST(missing_ingredients) AS ingredient;
"""
)

create_missing_ingredients_unnest_csp = BigQueryInsertJobOperator(
    task_id="create_missing_ingredients_unnest_csp",
    configuration={
        "query": {
            "query": missing_ingredients_unnest_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# missing_ingredients_with_ids

missing_ingredients_with_ids_sql = (f"""
CREATE OR REPLACE TABLE {ai_stg_dataset_name}.missing_ingredients_with_ids AS
SELECT miu.*, i.ingredient_id FROM {ai_stg_dataset_name}.missing_ingredients_unnest AS miu
LEFT JOIN (select ingredient_id, name, CONCAT(name, plural) as pluralname from {stg_dataset_name}.Ingredients) as i
  ON miu.ingredient = i.name
  or miu.ingredient = i.pluralname
WHERE LENGTH(miu.ingredient) > 0
"""
)

create_missing_ingredients_with_ids_csp = BigQueryInsertJobOperator(
    task_id="create_missing_ingredients_with_ids_csp",
    configuration={
        "query": {
            "query": missing_ingredients_with_ids_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# create similiar_ingredients

similiar_ingredients_sql = (f"""
create or replace table {ai_stg_dataset_name}.similar_ingredients as
WITH row_number as (select mi.recipe_id, mi.ingredient, i.ingredient_id, i.name, ROW_NUMBER() OVER (PARTITION BY mi.ingredient ORDER BY i.name ASC) AS rn
from {stg_dataset_name}.Ingredients i
join {ai_stg_dataset_name}.missing_ingredients_with_ids mi
 on i.name like CONCAT('%', mi.ingredient ,'%')
where mi.ingredient_id is null)
SELECT * FROM row_number
WHERE rn = 1
""" 
)

create_similiar_ingredients_csp = BigQueryInsertJobOperator(
    task_id="create_similar_ingredients_csp",
    configuration={
        "query": {
            "query": similiar_ingredients_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# update missing_ingredients_with_ids

update_missing_ingredients_sql = (f"""
update {ai_stg_dataset_name}.missing_ingredients_with_ids miid
set ingredient_id = (select ingredient_id from {ai_stg_dataset_name}.similar_ingredients si where si.recipe_id = miid.recipe_id and si.ingredient = miid.ingredient),
ingredient = (select name from {ai_stg_dataset_name}.similar_ingredients si where si.recipe_id = miid.recipe_id and si.ingredient = miid.ingredient)
where ingredient_id is null and ingredient IN (SELECT ingredient FROM {ai_stg_dataset_name}.similar_ingredients)
"""
)

update_missing_ingredients_csp = BigQueryInsertJobOperator(
    task_id="update_missing_ingredients_csp",
    configuration={
        "query": {
            "query": update_missing_ingredients_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# create new_ingredients_with_category 

new_ingredients_with_category_sql = (f"""
declare prompt_query STRING default " Identify the best category that matches the ingredient from the list. Return recipe_id, ingredient, and category in output as json";
CREATE OR REPLACE TABLE {ai_stg_dataset_name}.new_ingredients_with_category AS
select *
from ML.generate_text(
  model remote_model_central.gemini_pro,
  (
    select concat(prompt_query, to_json_string(json_object("recipe_id",recipe_id, "ingredients", ingredient, "categories", (select string_agg(distinct(category), ', ') from {stg_dataset_name}.Ingredients)))) as prompt
    from {ai_stg_dataset_name}.missing_ingredients_with_ids
    where ingredient_id is null and ingredient NOT IN ('no difference','[]')
  ),
  struct(TRUE as flatten_json_output)
)
"""
)

create_new_ingredients_with_category_csp = BigQueryInsertJobOperator(
    task_id="create_new_ingredients_with_category",
    configuration={
        "query": {
            "query": new_ingredients_with_category_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# change json format for new_ingredients_with_category
# new_ingredients_with_category_json_format_sql = (f"""
# create or replace table  {ai_stg_dataset_name}.new_ingredients_with_category_json_format as
# select trim(replace(replace(replace(replace(replace(ml_generate_text_llm_result, '```json', ''), '```', ''), '\\\\n', ''), ']', ''), '[' , '')) as ml_generate_text_llm_result
# from {ai_stg_dataset_name}.new_ingredients_with_category
# """)

# create_new_ingredients_with_category_json_format_csp = BigQueryInsertJobOperator(
#     task_id="create_new_ingredients_with_category_json_format_csp",
#     configuration={
#         "query": {
#             "query": new_ingredients_with_category_json_format_sql,
#             "useLegacySql": False,
#         }
#     },
#     location=region,
#     dag=dag)




# change json format for new_ingredients_with_category
new_ingredients_with_category_json_format_sql = (f"""
create or replace table  {ai_stg_dataset_name}.new_ingredients_with_category_json_format as
select trim(replace(replace(replace(replace(replace(ml_generate_text_llm_result, '```json', ''), '```', ''), '\\\\n', ''), ']', ''), '[' , '')) as ml_generate_text_llm_result
from {ai_stg_dataset_name}.new_ingredients_with_category
""")

create_new_ingredients_with_category_json_format_csp = BigQueryInsertJobOperator(
    task_id="create_new_ingredients_with_category_json_format_csp",
    configuration={
        "query": {
            "query": new_ingredients_with_category_json_format_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# close the json for new_ingredients_with_category_format_sql 
new_ingredients_with_category_closed_sql =(""" 
UPDATE  magazine_recipes_stg_af_ai.new_ingredients_with_category_json_format 
set ml_generate_text_llm_result = concat(ml_generate_text_llm_result, '"}')
where ml_generate_text_llm_result not like '%"}%'
""")

create_new_ingredients_with_category_closed_csp = BigQueryInsertJobOperator(
    task_id="create_new_ingredients_with_category_closed_csp",
    configuration={
        "query": {
            "query": new_ingredients_with_category_closed_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# create new_ingredients
new_ingredients_sql = (f"""
CREATE OR REPLACE TABLE {ai_stg_dataset_name}.new_ingredients AS
select (select max(ingredient_id) from {stg_dataset_name}.Ingredients) + ROW_NUMBER() OVER() as ingredient_id,
json_value(ml_generate_text_llm_result, '$.ingredients') as ingredient,
  json_value(ml_generate_text_llm_result, '$.categories') as category
from {ai_stg_dataset_name}.new_ingredients_with_category_json_format
"""
)

create_new_ingredients_csp = BigQueryInsertJobOperator(
    task_id="create_new_ingredients_csp",
    configuration={
        "query": {
            "query": new_ingredients_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# insert_ingredients 

insert_into_ingredients_sql = (f"""
INSERT INTO {stg_dataset_name}.Ingredients (ingredient_id, category, name, plural, data_source, load_time)
SELECT
  ingredient_id,
  category,
  ingredient,
  NULL AS plural,
  'ai' AS data_source,
  current_timestamp() AS load_time
FROM {ai_stg_dataset_name}.new_ingredients
WHERE ingredient IS NOT NULL 
"""
)

insert_into_ingredients_csp = BigQueryInsertJobOperator(
    task_id="insert_into_ingredients_csp",
    configuration={
        "query": {
            "query": insert_into_ingredients_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# insert_quantities 

insert_into_quantities_sql = (f"""
INSERT INTO {stg_dataset_name}.Quantity (quantity_id, recipe_id, ingredient_id, max_qty, min_qty, unit, preparation, optional, data_source, load_time)
SELECT
  (SELECT MAX(quantity_id) FROM {stg_dataset_name}.Quantity) + ROW_NUMBER() OVER() as quantity_id,
  cast(recipe_id as int64) as recipe_id,
  ingredient_id,
  NULL AS max_qty,
  NULL AS min_qty,
  cast(NULL as string) AS unit,
  cast(NULL as string) AS preparation,
  cast(NULL as bool) AS optional,
  'ai' AS data_source,
  current_timestamp() AS load_time
FROM {ai_stg_dataset_name}.missing_ingredients_with_ids
WHERE ingredient_id IS NOT NULL
"""
)

insert_into_quantities_csp = BigQueryInsertJobOperator(
    task_id="insert_into_quantities_csp",
    configuration={
        "query": {
            "query": insert_into_quantities_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# update recipes 

update_recipes_sql = (f"""
UPDATE {stg_dataset_name}.Recipes as r
SET directions = (SELECT recipe_directions FROM {ai_stg_dataset_name}.directions d WHERE CAST(d.recipe_id as INT64) = r.recipe_id)
, data_source = CONCAT(data_source,'-ai')
WHERE recipe_id IN (SELECT CAST(recipe_id AS INT64) FROM {ai_stg_dataset_name}.directions) AND directions IS NULL
"""
)

update_recipes_csp = BigQueryInsertJobOperator(
    task_id="update_recipes_csp",
    configuration={
        "query": {
            "query": update_recipes_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# do ease of prep stuff 



start >> create_dataset_stg_af_ai >> wait >> create_recipes_directions_csp >> create_recipes_directions_json_format_csp >> create_recipes_directions_closed_csp >>create_directions_csp >> create_recipes_directions_for_nulls_csp >> create_recipes_directions_for_nulls_json_format_csp >> create_recipes_directions_for_nulls_closed_csp >> create_directions_for_nulls_csp >> update_directions_csp >> create_directions_ingredients_csp >> create_direction_ingredients_json_format_csp >> create_direction_ingredients_closed_csp >> create_direction_mentioned_ingredients_csp >> create_compare_ingredients_csp >> create_ingredients_difference_csp >> create_ingredients_difference_json_format_csp >> create_missing_ingredients_list_csp  >> create_missing_ingredients_unnest_csp >> create_missing_ingredients_with_ids_csp >> create_similiar_ingredients_csp  >> update_missing_ingredients_csp >> create_new_ingredients_with_category_csp >> create_new_ingredients_with_category_json_format_csp >> create_new_ingredients_with_category_closed_csp >> create_new_ingredients_csp >> insert_into_ingredients_csp >> insert_into_quantities_csp >> update_recipes_csp >> end 