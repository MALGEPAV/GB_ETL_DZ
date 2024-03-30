from datetime import datetime
import requests
import json
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

def get_weather_data(ti = None):
    url = "https://api.openweathermap.org/data/2.5/weather?q=London,uk&APPID=cde25583263075ff71ffa3961bd239d9"
    response = requests.request("GET", url)

    temperature = json.loads(response.text)['main']['temp'] - 273
    location = json.loads(response.text)['name']
    print(f"{location = }, {temperature = }")

    ti.xcom_push(key="temperature", value=temperature)

def warm_or_not(ti = None):
    temperature = ti.xcom_pull(key = "temperature", task_ids = 'get_weather_data')
    if temperature >= 15:
        return 'print_warm'
    return 'print_cold'

    
dz7dag = DAG( 'dz7dag', description= 'DZ7 dag',
schedule_interval= '0 12 * * *' ,
start_date=datetime( 2021,4,1),
catchup= False )

get_weather = PythonOperator(task_id = "get_weather_data",python_callable=get_weather_data,dag = dz7dag)

what_to_print = BranchPythonOperator(task_id="what_to_print",python_callable=warm_or_not, dag = dz7dag)

bash_print_warm = BashOperator(task_id = 'print_warm', bash_command='echo "WARM"', dag = dz7dag)
bash_print_cold = BashOperator(task_id = 'print_cold', bash_command='echo "COLD"', dag = dz7dag)

get_weather >> what_to_print >> [bash_print_warm, bash_print_cold]