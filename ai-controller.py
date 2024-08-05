import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
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
    dag_id='ai-comments-controller',
    default_args=default_args,
    description='controller dag to create ai generated comments',
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
    dataset_id=stg_dataset_name,
    location=region,
    if_exists='ignore',
    dag=dag)



# create recipes_directions 

ai_comments_sql = (f"""
declare prompt_query STRING default "You are a user of a recipe sharing social media, write 3 original, creative, and relevant comments for the recipe in 25 or fewer words (you are a different user for each comment, so exhibit a different level of satisfaction). Return output as a CLOSED, COMPLETE json with only recipe_id, comment1, comment2, comment3";
create or replace table {ai_stg_dataset_name}.ai_comments as
select *
from ML.generate_text(
  model remote_models.gemini_pro,
  (
    select concat(prompt_query, to_json_string(json_object("recipe_id", recipe_id, "title", title, "directions", directions, "note", note, 'ease_of_prep', ease_of_prep))) as prompt
    from {stg_dataset_name}.Recipes as r
    order by r.recipe_id
  ),
  struct(TRUE as flatten_json_output)
);
"""
)

create_ai_comments_csp = BigQueryInsertJobOperator(
    task_id="create_ai_comments_csp",
    configuration={
        "query": {
            "query": ai_comments_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# format comments 

format_comments_sql = (f"""
create or replace table  {ai_stg_dataset_name}.comments_formatted as
  select trim(replace(replace(replace(replace(replace(ml_generate_text_llm_result, '```json', ''), '```', ''), '\n', ''), ']', ''), '[' , '')) as formatted_comment
  from {ai_stg_dataset_name}.ai_comments
"""
)

format_ai_comments_csp = BigQueryInsertJobOperator(
    task_id="format_ai_comments_csp",
    configuration={
        "query": {
            "query": format_comments_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# close json for incomplete outputs

close_comments_sql = (f"""
update  {ai_stg_dataset_name}.comments_formatted 
set formatted_comment = concat(formatted_comment, '"}')
where formatted_comment not like '%"}%
""")

close_comments_csp = BigQueryInsertJobOperator(
    task_id="close_comments_csp",
    configuration={
        "query": {
            "query": close_comments_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# create wide comments table from ai output 

recipe_comments_sql = (f"""
%%bigquery
CREATE OR REPLACE TABLE {ai_stg_dataset_name}.recipe_comments AS
select
cast(json_value(formatted_comment, '$.recipe_id') as int64) as recipe_id,
json_value(formatted_comment, '$.title') as title,
json_value(formatted_comment, '$.comment1') as comment1,
json_value(formatted_comment, '$.comment2') as comment2,
json_value(formatted_comment, '$.comment3') as comment3
from {ai_stg_dataset_name}.comments_formatted 
"""
)

create_recipe_comments_csp = BigQueryInsertJobOperator(
    task_id="create_recipe_comments_csp",
    configuration={
        "query": {
            "query": recipe_comments_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# create long comments table (one row per comment)

individual_comments_sql = (f"""
create or replace table {ai_stg_dataset_name}.individual_comments as 
select recipe_id, comment1 as comment
from {ai_stg_dataset_name}.recipe_comments
union all
select recipe_id, comment2 as comment
from {ai_stg_dataset_name}.recipe_comments
union all
select recipe_id, comment3 as comment
from {ai_stg_dataset_name}.recipe_comments
"""
)

create_individual_comments_csp = BigQueryInsertJobOperator(
    task_id="create_individual_comments_csp",
    configuration={
        "query": {
            "query": individual_comments_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# Create the staging table for comments 

create_comments_sql = (f"""
create or replace table {stg_dataset_name}.Comments as
SELECT ROW_NUMBER() OVER () AS comment_id, 
      p.post_id, 
      cast(FLOOR(RAND() * (select max(user_id) from magazine_recipes_stg.Users) + 1) as int64) AS user_id, 
      c.comment,
      'ai' as data_source, 
      current_timestamp() as load_time
from {ai_stg_dataset_name}.individual_comments c
left join {stg_dataset_name}.Posts p on c.recipe_id = p.recipe_id
where comment is not null""")


create_comments_csp = BigQueryInsertJobOperator(
    task_id="create_comments_csp",
    configuration={
        "query": {
            "query": create_comments_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# add timestamp to comment

add_comment_timestamp_sql = (f"""
update {stg_dataset_name}.Comments c
SET timestamp_commented = TIMESTAMP_ADD(
    (SELECT p.timestamp_posted FROM {stg_dataset_name}.Posts p WHERE p.post_id = c.post_id),
    INTERVAL CAST(FLOOR(RAND() * 6108032) AS INT64) SECOND
)
where 1 = 1;""")

add_comment_timestamp_csp = BigQueryInsertJobOperator(
    task_id="add_comment_timestamp_csp",
    configuration={
        "query": {
            "query": add_timestamp_comments_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# fix dual comments

fix_dual_comments_sql = (f"""
update {stg_dataset_name}.Comments
set user_id =
    CASE 
    WHEN MOD((user_id + length(comment)), (select max(user_id) from {stg_dataset_name}.Users)) != 0 THEN MOD((user_id + length(comment)), (select max(user_id) from magazine_recipes_stg.Users))
    ELSE MOD((user_id + length(comment)), (select max(user_id) from {stg_dataset_name}.Users)) + 1
    END
where post_id in (
select c1.post_id from {stg_dataset_name}.Comments c1
join {stg_dataset_name}.Comments c2 on c1.post_id = c2.post_id and c1.user_id = c2.user_id and c1.comment_id != c2.comment_id)
""")

fix_dual_comments_csp = BigQueryInsertJobOperator(
    task_id="fix_dual_comments_csp",
    configuration={
        "query": {
            "query": fix_dual_comments_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# delete remaining duals

remove_dual_comments_sql = (f"""
delete from {stg_dataset_name}.Comments
where comment_id in (
select least(c.comment_id, c1.comment_id) 
from {stg_dataset_name}.Comments c
join {stg_dataset_name}.Comments c1 on c.user_id = c1.user_id and c.post_id = c1.post_id and c.comment_id != c1.comment_id)
""")

remove_dual_comments_csp = BigQueryInsertJobOperator(
    task_id="remove_dual_comments_csp",
    configuration={
        "query": {
            "query": remove_dual_comments_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# change matching posters and commenters
fix_self_comments_sql = (f"""
update {stg_dataset_name}.Comments
SET user_id = 
    CASE 
        WHEN user_id < (SELECT MAX(user_id) FROM {stg_dataset_name}.Users) THEN user_id + 1
        ELSE user_id - 1
    END
WHERE post_id IN (
    SELECT p.post_id 
    FROM {stg_dataset_name}.Comments c
    JOIN {stg_dataset_name}.Posts p ON c.user_id = p.user_id AND c.post_id = p.post_id
);
""")

fix_self_comments_csp = BigQueryInsertJobOperator(
    task_id="fix_self_comments_csp",
    configuration={
        "query": {
            "query": fix_self_comments_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# add forgein and primary key constraints



start >> create_model_csp >> create_recipes_directions_csp >> create_directions_csp >> create_recipes_directions_for_nulls_csp >> create_directions_for_nulls_csp >> update_directions_csp >> create_directions_ingredients_csp >> create_direction_mentioned_ingredients_csp >> create_compare_ingredients_csp >> create_ingredients_difference_csp >> create_missing_ingredients_list_csp  >> create_missing_ingredients_unnest_csp >> create_missing_ingredients_with_ids_csp >> create_similiar_ingredients_csp  >> update_missing_ingredients_csp >> create_new_ingredients_with_category_csp >> create_new_ingredients_csp >> insert_into_ingredients_csp >> insert_into_quantities_csp >> update_recipes_csp >> end 