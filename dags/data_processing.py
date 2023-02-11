from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta
import pendulum


default_args = {
    'owner': 'jazzdung',
    'retries':5,
    'retry_delay': timedelta(minutes=20)
}

with DAG(
    dag_id='data_processing',
    description='Process data',
    start_date=pendulum.yesterday(),
    schedule_interval='0 0 * * *',
    catchup=True
) as dag:

    task_crawl_shopee_url = BashOperator(
        task_id='crawl_shopee_url',
        bash_command='python3 /home/jazzdung/E-Commerce-Support-System/main.py --site shopee --type url --num_page 1'
    )

    task_crawl_lazada_url = BashOperator(
        task_id='crawl_lazada_url',
        bash_command='python3 /home/jazzdung/E-Commerce-Support-System/main.py --site lazada --type url --num_page 1'
    )

    task_crawl_shopee_data = BashOperator(
        task_id='crawl_shopee_data',
        bash_command='python3 /home/jazzdung/E-Commerce-Support-System/main.py --site shopee --type info'
    )

    task_crawl_lazada_data = BashOperator(
        task_id='crawl_lazada_data',
        bash_command='python3 /home/jazzdung/E-Commerce-Support-System/main.py --site lazada --type info'
    )

    task_clean_shopee_data = BashOperator(
        task_id='clean_shopee_data',
        bash_command='python3 /home/jazzdung/E-Commerce-Support-System/script/shopee_data.py --origin hdfs://localhost:9000/user/hadoop/test_file --destination hdfs://localhost:9000/user/hadoop/shopee_full_data.csv'
    )

    task_clean_lazada_data = BashOperator(
        task_id='clean_lazada_data',
        bash_command='python3 /home/jazzdung/E-Commerce-Support-System/script/lazada_data.py --origin hdfs://localhost:9000/user/hadoop/test_file --destination hdfs://localhost:9000/user/hadoop/lazada_full_data.csv'
    )

    task_create_visualize_data = BashOperator(
        task_id='create_visualize_data',
        bash_command='python3 /home/jazzdung/E-Commerce-Support-System/script/visualize_data.py --origin hdfs://localhost:9000/user/hadoop/full_data.csv --destination hdfs://localhost:9000/user/hadoop/visualiza_data.csv'
    )

    task_create_model_data = BashOperator(
        task_id='create_model_data',
        bash_command='python3 /home/jazzdung/E-Commerce-Support-System/script/model_data.py --shopee hdfs://localhost:9000/user/hadoop/shopee_full_data.csv --lazada hdfs://localhost:9000/user/hadoop/lazada_full_data.csv --destination hdfs://localhost:9000/user/hadoop/model_data.csv'
    )


    task_crawl_shopee_url >> task_crawl_shopee_data >> task_clean_shopee_data
    task_crawl_lazada_url >> task_crawl_lazada_data >> task_clean_lazada_data
    task_clean_shopee_data >> task_create_visualize_data
    [task_clean_shopee_data, task_clean_lazada_data] >> task_create_model_data