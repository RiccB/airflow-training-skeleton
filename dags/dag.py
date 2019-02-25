import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

dag = DAG(
    dag_id="hello_airflow",
    default_args={
        "owner": "godatadriven",
        "start_date": airflow.utils.dates.days_ago(3),
    },
)

print_date = BashOperator(
    task_id="print_exec_date", bash_command="echo {{ execution_date }}", dag=dag
)

# wait_1 = BashOperator(
#     task_id="wait_1",
#     bash_command="sleep 1",
#     dag=dag
# )
#
# wait_5 = BashOperator(
#     task_id="wait_5",
#     bash_command="sleep 5",
#     dag=dag
# )
#
# wait_10 = BashOperator(
#     task_id="wait_10",
#     bash_command="sleep 10",
#     dag=dag
# )

operators = []
for i in [1, 5, 10]:
    operators.append(BashOperator(task_id='wait_{}'.format(i), bash_command='sleep {}'.format(i), dag=dag))

end = DummyOperator(task_id="end", dag=dag)

print_date >> operators >> end


