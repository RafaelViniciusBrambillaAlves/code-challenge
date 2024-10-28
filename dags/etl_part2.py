from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models.param import Param
from datetime import datetime
from src.edit_definitions import update_paths_with_date
from src.check_directory import check_date_directory

    
def check_rename_function(params=None, **context):
    dag_run_conf = context.get("dag_run").conf if context.get("dag_run") else {}
    date_param = dag_run_conf.get('date') or params.get('date')

    if date_param == 'default_date':
        date_param = datetime.now().strftime('%Y-%m-%d')

    try:
        valid_date = datetime.strptime(date_param, '%Y-%m-%d').strftime('%Y-%m-%d')
        print(f"Data: {valid_date}")
        
        check_date_directory(valid_date, **context)

        update_paths_with_date('/opt/airflow/meltano-project/files-definitions-part2.json', valid_date)

        return valid_date
    
    except ValueError:
        raise Exception(f"Data inválida: {date_param}. O formato deve ser YYYY-MM-DD.")  

with DAG(
    dag_id='etl_part2',
    start_date=datetime(2023, 9, 23),
    schedule_interval=None, 
    catchup=False,
    params={
        "date": Param("default_date", type="string")
    },
    render_template_as_native_obj=True
) as dag:

    task_edit_definitions = PythonOperator(
        task_id='run_edit_check_definitions',
        python_callable=check_rename_function,
        op_kwargs={'params': dag.params},
        provide_context=True
    )

    task_load_postgres = BashOperator(
        task_id='run_meltano_load_postgres',
        # bash_command='cd /opt/airflow/meltano-project && meltano run load-postgres'
        bash_command='cd /opt/airflow/meltano-project && meltano add loader target-postgres && meltano run load-postgres'
    )

# Task de esperar primeiro
task_edit_definitions >> task_load_postgres
