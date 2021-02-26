#
# [Source:] Unchanged copy from [Airflow Sensor to check status of DAG Execution](https://medium.com/@sunilkhaire17/airflow-sensor-to-check-status-of-dag-execution-225342ec2897)
#

from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.models.dagrun import DagRun
from airflow.exceptions import AirflowException
import logging


__author__ = "Sunil Khaire"
__email__ = "sunilkhaire17@gmail.com"

""" 
This sensor is for checking latest status of dag. If its success then only it will return true or mark 
as completed so next task will get execute.

In case dag is failed it will raise an airflow exception as failed.
params:
dag_name: pass the dag_id for your dag
status_to_check: pass 'success' if you want sensor to return true for this.
for e.g. 

dag_status_sensor = DagStatusSensor(dag_name='test_dag',status_to_check='success',task_id='dag_status_sensor',poke_interval=30,timeout=1800,dag=dag)

"""

class DagStatusSensor(BaseSensorOperator):

    @apply_defaults
    def __init__(self,dag_name,status_to_check,*args,**kwargs):
        self.dag_name = dag_name
        self.status_to_check = status_to_check

        super(DagStatusSensor,self).__init__(*args,**kwargs)

    def poke (self,context):

        dag_runs = DagRun.find(dag_id=self.dag_name)

        length = len(dag_runs)

        if dag_runs[length-1].state == self.status_to_check:
            return True
        elif dag_runs[length-1].state == "failed":
            raise AirflowException(f"""Exception occcured and dag with dag_id {self.dag_name} is failed""")
