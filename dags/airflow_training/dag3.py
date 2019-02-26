import airflow
from airflow import DAG
from airflow_training.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator
from tempfile import NamedTemporaryFile
from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

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
         WHERE transfer_date = '{{ds}}'",
    bucket='riccardos_bucket',
    filename='house_data/{{ds}}.json',
    dag=dag,
)


class HttpToGcsOperator(BaseOperator):
    """
    Calls an endpoint on an HTTP system to execute an action
    :param http_conn_id: The connection to run the operator against
    :type http_conn_id: string
    :param endpoint: The relative part of the full url. (templated)
    :type endpoint: string
    :param gcs_path: The path of the GCS to store the result
    :type gcs_path: string
    """
    template_fields = ('endpoint', 'gcs_path')
    template_ext = ()
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(self,
                 method,
                 endpoint,
                 gcs_path=None,
                 *args, **kwargs):
        super(HttpToGcsOperator, self).__init__(*args, **kwargs)

        self.endpoint = endpoint
        self.gcs_path = gcs_path
        self.method = method


    def execute(self, context):
        request = HttpHook(self.method)
        response = request.run(self.endpoint)
        self.log.info(str(response))


HttpToGcsOperator(method='GET', endpoint="convert-currency?date={{ds}}&from=GBP&to=EUR", )
