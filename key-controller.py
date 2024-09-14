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
region = 'us-central1'

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0
}

dag = DAG(
    dag_id='key-controller',
    default_args=default_args,
    description='key controller dag',
    schedule_interval=None,
    max_active_runs=1,
    concurrency=3,
    catchup=False,
    dagrun_timeout=None,
)

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)
join = DummyOperator(task_id="join", dag=dag)


# Recipes PK
table_name = "Recipes"
pk_columns = ["recipe_id"]

recipes_pk = TriggerDagRunOperator(
    task_id="recipes_pk",
    trigger_dag_id="create-pk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, "table_name": table_name, "pk_columns": pk_columns},
    dag=dag)


# Quantity PK
table_name = "Quantity"
pk_columns = ["quantity_id"]

quantity_pk = TriggerDagRunOperator(
    task_id="quantity_pk",
    trigger_dag_id="create-pk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, "table_name": table_name, "pk_columns": pk_columns},
    dag=dag)


# Publications
table_name = "Publications"
pk_columns = ["publication_id"]

publications_pk = TriggerDagRunOperator(
    task_id="publications_pk",
    trigger_dag_id="create-pk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, "table_name": table_name, "pk_columns": pk_columns},
    dag=dag)
    
    
# Nutrition PK
table_name = "Nutrition"
pk_columns = ["recipe_id"]

nutrition_pk = TriggerDagRunOperator(
    task_id="nutrition_pk",
    trigger_dag_id="create-pk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, "table_name": table_name, "pk_columns": pk_columns},
    dag=dag)
  
    
# Magazines PK
table_name = "Magazines"
pk_columns = ["magazine_id"]

magazines_pk = TriggerDagRunOperator(
    task_id="magazines_pk",
    trigger_dag_id="create-pk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, "table_name": table_name, "pk_columns": pk_columns},
    dag=dag)
    

# Journalists PK
table_name = "Journalists"
pk_columns = ["journalist_id"]

journalists_pk = TriggerDagRunOperator(
    task_id="journalists_pk",
    trigger_dag_id="create-pk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, "table_name": table_name, "pk_columns": pk_columns},
    dag=dag)
    
    
# Ingredients PK
table_name = "Ingredients"
pk_columns = ["ingredient_id"]

ingredients_pk = TriggerDagRunOperator(
    task_id="ingredients_pk",
    trigger_dag_id="create-pk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, "table_name": table_name, "pk_columns": pk_columns},
    dag=dag)
    

# Nutrition FK
child_table_name = "Nutrition"
fk_columns = ["recipe_id"]
parent_table_name = "Recipes"
pk_columns = ["recipe_id"]

nutrition_recipes_fk = TriggerDagRunOperator(
    task_id="nutrition_recipe_fk",
    trigger_dag_id="create-fk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, \
            "child_table_name": child_table_name, "fk_columns": fk_columns, \
            "parent_table_name": parent_table_name, "pk_columns": pk_columns, },
    dag=dag)
  
  
# Publications KF #1
child_table_name = "Publications"
fk_columns = ["recipe_id"]
parent_table_name = "Recipes"
pk_columns = ["recipe_id"]

publications_recipes_fk = TriggerDagRunOperator(
    task_id="publications_recipes_fk",
    trigger_dag_id="create-fk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, \
            "child_table_name": child_table_name, "fk_columns": fk_columns, \
            "parent_table_name": parent_table_name, "pk_columns": pk_columns, },
    dag=dag)
    

# Publications FK #2
child_table_name = "Publications"
fk_columns = ["magaginze_id"]
parent_table_name = "Magazines"
pk_columns = ["magazine_id"]

publications_magazines_fk = TriggerDagRunOperator(
    task_id="publications_magazines_fk",
    trigger_dag_id="create-fk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, \
            "child_table_name": child_table_name, "fk_columns": fk_columns, \
            "parent_table_name": parent_table_name, "pk_columns": pk_columns, },
    dag=dag)
   
    
# Quantity FK #1
child_table_name = "Quantity"
fk_columns = ["recipe_id"]
parent_table_name = "Recipes"
pk_columns = ["recipe_id"]

quantity_recipes_fk = TriggerDagRunOperator(
    task_id="quantity_recipes_fk",
    trigger_dag_id="create-fk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, \
            "child_table_name": child_table_name, "fk_columns": fk_columns, \
            "parent_table_name": parent_table_name, "pk_columns": pk_columns, },
    dag=dag)
   
    
# Quantity FK #2
child_table_name = "Quantity"
fk_columns = ["ingredient_id"]
parent_table_name = "Ingredients"
pk_columns = ["ingredient_id"]

quantity_ingredients_fk = TriggerDagRunOperator(
    task_id="quantity_ingredients_fk",
    trigger_dag_id="create-fk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, \
            "child_table_name": child_table_name, "fk_columns": fk_columns, \
            "parent_table_name": parent_table_name, "pk_columns": pk_columns, },
    dag=dag)
    

    

# Primary Keys
start >> recipes_pk >> join
start >> quantity_pk >> join
start >> publications_pk >> join
start >> nutrition_pk >> join
start >> magazines_pk >> join
start >> journalists_pk >> join
start >> ingredients_pk >> join

# Foreign Keys
join >> nutrition_recipes_fk >> end
join >> publications_recipes_fk >> end
join >> publications_magazines_fk >> end
join >> quantity_recipes_fk >> end
join >> quantity_ingredients_fk >> end
   
