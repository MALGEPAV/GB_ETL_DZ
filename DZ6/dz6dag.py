# 1. Создайте новый граф. Добавьте в него BashOperator, 
# который будет генерировать рандомное число и печатать его в
# консоль.

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from random import randint

dz6dag = DAG( 'dz6dag', description= 'DZ6 dag',
schedule_interval= '0 12 * * *' ,
start_date=datetime( 2024,3,26),
catchup= False )

bash_random_number_generator = BashOperator(task_id = 'bash_random',
                                            bash_command='echo $RANDOM',
                                            dag=dz6dag)



# 2. Создайте PythonOperator, который генерирует рандомное число,
# возводит его в квадрат и выводит в консоль исходное число и результат.

def random_square():
    random_number = randint(1,100)
    print(f"Random number: {random_number}, its square: {random_number**2}")


python_random_number_operator = PythonOperator(task_id = 'python_random',
                                python_callable=random_square,
                                dag = dz6dag)



bash_random_number_generator >> python_random_number_operator