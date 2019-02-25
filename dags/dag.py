import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator

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
    if execution_date.strftime("%a") == 'Mon':
        return 'Bob'
    elif (execution_date.strftime("%a") == 'Tue') | (execution_date.strftime("%a") == 'Thu'):
        return 'Joe'
    else:
        return 'Alice'


branch_task = BranchPythonOperator(
    task_id='branching',
    python_callable=choice,
    provide_context=True,
    dag=dag
)

print_weekday >> branch_task >> [DummyOperator(task_id='name') for name in list(set(weekday_person_to_email.items()))]
