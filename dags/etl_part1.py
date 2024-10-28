from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from src.organize_order_details import organize_order_details
from src.organize_public_files import organize_public_files
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id='etl_part1',
    start_date=datetime(2023, 9, 23),
    schedule_interval='00 11 * * *',  # Executa diariamente
    catchup=False,
    render_template_as_native_obj=True
) as dag:
    
    task_extract_postgres = BashOperator(
        task_id='run_meltano_extract_postgres',
        bash_command='cd /opt/airflow/meltano-project && meltano run extract-postgres'
    )

    task_extract_csv = BashOperator(
        task_id='run_meltano_extract_csv',
        bash_command='cd /opt/airflow/meltano-project && meltano run extract-csv'
    )

    task_organize_public_files = PythonOperator(
        task_id='organize_public_files',
        python_callable=organize_public_files,
    )

    task_organize_order_details = PythonOperator(
        task_id='organize_order_details',
        python_callable=organize_order_details,
    )

    trigger_etl_part2 = TriggerDagRunOperator(
        task_id='trigger_etl_part2',
        trigger_dag_id='etl_part2',  # Nome da DAG que será disparada
        wait_for_completion=False,  # Se quiser esperar a DAG disparada finalizar, use True
    )

task_extract_csv >> task_organize_order_details
task_extract_postgres >> task_organize_public_files

[task_organize_order_details, task_organize_public_files] >> trigger_etl_part2
