# Required dependencies
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
#import datetime
from datetime import datetime
import os, psutil
import pandas as pd

# Importing .csv module to write to file
import csv
from csv import writer

# Setting a global variable to the airflow destination file
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')

# Defining function to load data
def _load_data():
    process = psutil.Process(os.getpid())
    time_start = datetime.now()
    
    data = pd.read_csv(AIRFLOW_HOME + '/DAGs/ecg.csv', header=None)
    data = data.to_json()
    
    end_time = datetime.now() - time_start
    memory_used = process.memory_info().rss / 1000000
    
    return [end_time.total_seconds(), memory_used]


def _send_data_to_file(ti):
    # Pulling data from first task using Xcom
    dt = ti.xcom_pull(task_ids=['load_data'])
    
    # Loading comparison metrics into variables from Xcom list
    end_time = dt[0][0]
    memory_used = dt[0][1]
    
    # Lists for variable to be written to the data files
    time_list = []
    memory_list = []
    
    ########### Writing to data files ###########
    # Opening/creating Time and Memoery data files
    time_file = open(AIRFLOW_HOME + '/DAGs/Masters_Project_Time_Airflow.csv', 'a')
    memory_file = open(AIRFLOW_HOME + '/DAGs/Masters_Project_Memory_Airflow.csv', 'a')

    # Creating csv readers
    time_reader = csv.reader(time_file)
    memory_reader = csv.reader(memory_file)

    # Creating .csv writer object for time_file and memory_file
    time_writer_obj = writer(time_file)
    memory_writer_obj = writer(memory_file)

    # Adding data to the list for insertion to .csv file
    time_list.append(end_time)
    memory_list.append(memory_used)

    # Writing data to file
    time_writer_obj.writerow(time_list)
    memory_writer_obj.writerow(memory_list)

    # Closing files
    time_file.close()
    memory_file.close()

    #Resetting list for new run
    time_list = []
    memory_list = []

# For the project
with DAG("Masters_Project_DAG", start_date=datetime(2020, 7, 7), schedule_interval="@daily", max_active_runs=1) as dag:
#with DAG("Masters_Project_DAG", start_date=datetime(2023, 4, 1), schedule_interval="@daily", max_active_runs=3) as dag:

    load_data = PythonOperator(
        task_id = "load_data",
        python_callable = _load_data
    )
    
    send_data_to_file = PythonOperator(
        task_id = "send_data_to_file",
        python_callable = _send_data_to_file
    )
    
    load_data >> send_data_to_file