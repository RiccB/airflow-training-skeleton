import airflow
from airflow import DAG
from datetime import datetime
from airflow.operators.slack_operator import SlackAPIPostOperator
from bigquery_get_data import BigQueryGetDataOperator

dag = DAG(
    dag_id='godatafest',
    schedule_interval='@daily',
    default_args={
        'owner': 'GoDataDriven',
        'start_date': airflow.utils.dates.days_ago(2)
    }
)

bq_fetch_data = BigQueryGetDataOperator(
    task_id='bq_fetch_data',
    sql="select committer.name, count(*) as number \
        from [bigquery-public-data.github_repos.commits] \
        where date(committer.date) = '{{ ds }}' \
        group by committer.name \
        order by number desc \
        limit 5",
    dag=dag
)

from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable


def send_to_slack_func(**context):
    lista = context.get('ti').xcom_pull(key='return_value', task_ids='bq_fetch_data')
    lista = ['date: {}'.format(context.get('ds'))] + lista
    operator = SlackAPIPostOperator(
        task_id='slack_api',
        text=str(lista),
        token=Variable.get('token'),
        channel=Variable.get("slack_channel")
    )
    return operator.execute(context=context)

send_to_slack = PythonOperator(
    task_id='send_to_slack',
    python_callable=send_to_slack_func,
    provide_context=True,
    dag=dag,
)

bq_fetch_data >> send_to_slack