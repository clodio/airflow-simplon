
# The DAG object; we'll need this to instantiate a DAG
import airflow
import random
import os
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.edgemodifier import Label
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.decorators import task
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule


from datetime import datetime, timedelta
from textwrap import dedent

# 'start_date': (datetime.now() - timedelta(days=1)).replace(hour=2),
default_args = {
  'owner': 'Simplon Team',
  'depends_on_past': False,
  'email': ['claude.seguret@gmail.com'],
  'email_on_failure': True,
  'email_on_retry': False,
  'retries': 0,
  'max_active_runs': 1,
  'retry_delay': timedelta(minutes=15)
}

shared_variable = 'shared_value'

dag_id = 'start'
dag = DAG('test_airflow',
  dag_id,
  default_args=default_args,
  schedule_interval=timedelta(hours=1),
  start_date = datetime(2022, 10, 15, 9, 0, 0)
)

## Ajouter un dummy operator au DAG et le nommer start vérifier dans l'UI airflow que le dummy operator est apparu
start = DummyOperator(task_id='task_start_id',  dag=dag)

## Ajouter une tâche utilisant le bash operator la nommer "list" et lui faire exécuter la commande "ls"
list = BashOperator(task_id='task_list_id',  bash_command='ls -l', dag=dag)

## Utiliser l'opérateur bitshift (>>) pour faire en sorte que la tâche "list" soit exécutée après la tâche "start"
start >> list

## Ajouter une tâche utilisant le python opérateur la nommer "hello" et lui faire écrire un fichier contenant le mot "world" et contenant la date d'execution du workflow.
def hello_function():
    now = datetime.now()
    f = open("/home/simplon/dag_launch_date.txt", "a")
    f.write("The launch date is " + now.strftime("%d/%m/%Y %H:%M:%S") + '\n')
    f.close()

    # used for sensor
    f = open("/home/simplon/sensor.txt", "a")
    f.write("The launch date is " + now.strftime("%d/%m/%Y %H:%M:%S") + '\n')
    f.close()
    return True

hello = PythonOperator(task_id='task_hello_id',  python_callable=hello_function, dag=dag)
list >> hello


## Créer deux tâches utilisant le bash opérator: la tâche nommée "sleep10" devra faire un sleep de 10 secondes la tache nommée "sleep15" devra faire un sleep de 15 secondes.
sleep10 = BashOperator(task_id='task_bash_sleep10_id',  bash_command='sleep 1', dag=dag)# temps reduit car trop long
sleep15 = BashOperator(task_id='task_bash_sleep15_id',  bash_command='sleep 1', dag=dag)# temps reduit car trop long

## faire s'exécuter ces deux tâches en parallèle après la tâche "hello"
hello >> [sleep10, sleep15]

## Créer une tâche nommée "join" qui utilise le dummy operator et qui attend que les tâches sleep10 et sleep15 soient terminées pour s'exécuter.
join = DummyOperator(task_id='task_join_id',  dag=dag, trigger_rule=TriggerRule.ALL_DONE)
join.set_upstream([sleep10, sleep15])

## optionnel

# générer un fichier dont le nom contient la date d'exécution du workflow
def generate_file_with_execution_date_workflow():
    now = datetime.now()
    f = open(now.strftime("%d_%m_%Y__%H_%M_%S") + '_airflow.log', "a")
    f.write("The launch date is " + now.strftime("%d/%m/%Y %H:%M:%S") + '\n')
    f.close()
    return True

generate_file_with_execution_date_workflow = PythonOperator(task_id='task_generate_file_with_execution_date_workflow_id',  python_callable=generate_file_with_execution_date_workflow, dag=dag)
start >> generate_file_with_execution_date_workflow

# rejouer de manière idempotente une exécution passé

# transmettre un identifiant entre les différentes tâches
# fait avec xcom (xcom_push, xcom_pull) ,   et shared_variable
def test_transmission_identifiant(**context):
   context['task_instance'].xcom_push(key='shared_variable', value='shared_value')
   return context['task_instance'].xcom_pull(key='shared_variable')

test_transmission_identifiant_id = PythonOperator(task_id='test_transmission_identifiant_id', provide_context=True, python_callable=test_transmission_identifiant, dag=dag)

list >> test_transmission_identifiant_id

# utiliser un sensor pour détecter l'arrivée d'un fichier
def sensor_action():
  ## désactivé sinon ca bloque du fait de l'install mono-tache d'airflow
  # if os.path.exists("/home/simplon/sensor.txt"):
    ## désactive sinon ca bloque du fait de l'install monotache d'airflow
    # os.remove("/home/simplon/sensor.txt")
  return "file detected and deleted"

sensor_action = PythonOperator(task_id='sensor_action_id',  python_callable=sensor_action, dag=dag)

waiting_for_file = FileSensor(task_id='waiting_for_file_id', poke_interval=1,  filepath="/home/simplon/sensor.txt")
test_transmission_identifiant_id >> waiting_for_file >> sensor_action
  
# créer dynamiquement des tâches dans un DAG à partir d'une list de string python

# créer et utiliser des variables airflow

# créer et utiliser des connections airflow pour se connecter à une base de donnée (postgresql)

# créer son propre opérateur qui execute des requêtes hive

## executer un fichier hive depuis le terminal "hive -f /mon/fichier.hql"
## executer une commande hive depuis le terminal "hive -e 'select * from cities_2022'"


 
