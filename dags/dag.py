import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

args = {"owner": "godatadriven", "start_date": airflow.utils.dates.days_ago(14)}

dag = DAG(
    dag_id="exercise3",
    default_args=args,
    description="Demo DAG showing BranchPythonOperator.",
    schedule_interval="0 0 * * *",
    start_date=args['start_date']
)


def print_weekday(execution_date, **context):
    print(execution_date.strftime("%a"))


print_weekday = PythonOperator(
    task_id="print_weekday",
    python_callable=print_weekday,
    provide_context=True,
    dag=dag
)

# Definition of who is emailed on which weekday
weekday_person_to_email = {
    0: "Bob",  # Monday
    1: "Joe",  # Tuesday
    2: "Alice",  # Wednesday
    3: "Joe",  # Thursday
    4: "Alice",  # Friday
    5: "Alice",  # Saturday
    6: "Alice",  # Sunday
}


def choice(execution_date, **context):
    day = execution_date.weekday()
    return weekday_person_to_email[day]


branch_task = BranchPythonOperator(
    task_id='branching',
    python_callable=choice,
    provide_context=True,
    dag=dag
)

options = [DummyOperator(task_id=name, dag=dag) for name in list(set(weekday_person_to_email.values()))]
end = DummyOperator(task_id='End', dag=dag, trigger_rule=TriggerRule.ONE_SUCCESS)

print_weekday >> branch_task >> options >> end
