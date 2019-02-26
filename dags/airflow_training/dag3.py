import airflow
from airflow import DAG
from airflow_training.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator

dag = DAG(
    dag_id='real_estate',
    schedule_interval='@daily',
    default_args={
        'owner': 'GoDataDriven',
        'start_date': airflow.utils.dates.days_ago(2)
    }
)

pgsl_to_gcs = PostgresToGoogleCloudStorageOperator(
    task_id='get_data',
    postgres_conn_id='postgres_default',
    sql="select * \
         from public.land_registry_price_paid_uk \
         WHERE transfer_date = {{ds}}",
    bucket='riccardos_bucket',
    filename='house_data_{{ds}}.json',
    dag=dag,
)


