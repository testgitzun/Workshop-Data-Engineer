
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

import os
import pymysql.cursors
import pandas as pd
import requests


# path
mysql_output_path = "/home/airflow/data/transaction.csv"
conversion_rate_output_path = "/home/airflow/data/conversion_rate.csv"
final_output_path = "/home/airflow/data/result_databook.csv"

class Config:
    MYSQL_HOST = os.getenv("MYSQL_HOST")
    MYSQL_PORT = int(os.getenv("MYSQL_PORT"))
    MYSQL_USER = os.getenv("MYSQL_USER")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
    MYSQL_DB = os.getenv("MYSQL_DB")
    MYSQL_CHARSET = os.getenv("MYSQL_CHARSET")


def get_data_from_mysql(transaction_path):

    connection = pymysql.connect(host=Config.MYSQL_HOST,
                             port=Config.MYSQL_PORT,
                             user=Config.MYSQL_USER,
                             password=Config.MYSQL_PASSWORD,
                             db=Config.MYSQL_DB,
                             charset=Config.MYSQL_CHARSET,
                             cursorclass=pymysql.cursors.DictCursor)


    # table ที่ดึงจาก Database มีตาราง audible_data, audible_transaction
    # audible_data
    with connection.cursor() as cursor:
        cursor.execute('SELECT * FROM audible_data')
        result_audible_data = cursor.fetchall()
    
    # audible_transaction
    with connection.cursor() as cursor:
        cursor.execute('SELECT * FROM audible_transaction')
        result_audible_transaction = cursor.fetchall()

    # pandas DataFrame
    audible_data = pd.DataFrame(result_audible_data)
    audible_transaction = pd.DataFrame(result_audible_transaction)

    # Join 
    df = audible_transaction.merge(audible_data,
                                        how='left',
                                        left_on='book_id',
                                        right_on='Book_ID')
    
    # save ไฟล์ csv transaction_path
    # '/home/airflow/gcs/data/audible_data_merged.csv'
    #df.to_csv("/home/airflow/data/transaction.csv", index=False)
    df.to_csv(transaction_path, index=False)
    print(f"Output to {transaction_path}")

"""
def get_data_from_api(conversion_rate_path):
    # อ่านข้อมูลจาก API
    CONVERSION_RATE_URL = 'https://r2de2-workshop-vmftiryt6q-ts.a.run.app/usd_thb_conversion_rate'
    r = requests.get(CONVERSION_RATE_URL)
    result_conversion_rate = r.json()
    df = pd.DataFrame(result_conversion_rate)

    # เปลี่ยน index เป็น data พร้อมชื่อคอลัมน์ว่า date
    df = df.reset_index().rename(columns={"index": "date"})
    #df.to_csv("/home/airflow/data/conversion_rate.csv", index=False)
    df.to_csv(conversion_rate_path, index=False)
    print(f"Output to {conversion_rate_path}")

def merge_data(transaction_path, conversion_rate_path, output_path):
    # ทำการอ่านไฟล์ทั้ง 3 ไฟล์ ที่โยนเข้ามาจาก task 
    transaction = pd.read_csv(transaction_path)
    conversion_rate = pd.read_csv(conversion_rate_path)

    # สร้างคอลัมน์ใหม่ data ใน transaction
    # แปลง transaction['date'] เป็น timestamp
    transaction['date'] = transaction['timestamp']
    transaction['date'] = pd.to_datetime(transaction['date']).dt.date
    conversion_rate['date'] = pd.to_datetime(conversion_rate['date']).dt.date

    # merge 2 datframe transaction, conversion_rate
    fianl_databook = transaction.merge(conversion_rate, how="left", left_on="date", right_on="date")
    
    # ลบเครื่องหมาย $ ในคอลัมน์ Price และแปลงเป็น float
    fianl_databook['Price'] = fianl_databook.apply(lambda x: x["Price"].replace("$",""), axis=1)
    fianl_databook['Price'] = fianl_databook['Price'].astype(float)

    # สร้างคอลัมน์ใหม่ชื่อว่า THBPrice เอา price * conversion_rate
    fianl_databook['THBPrice'] = fianl_databook['Price'] * fianl_databook['conversion_rate']
    fianl_databook = fianl_databook.drop(["date", "book_id"], axis=1)

    # save ไฟล์ fianl_databook เป็น csv
    #fianl_databook.to_csv("/home/airflow/data/result.csv", index=False)
    fianl_databook.to_csv(output_path, index=False)
    print(f"Output to {output_path}")
"""

# Instantiate a Dag
with DAG(
    dag_id = "Book_dag_V2",
    start_date=days_ago(1),
    schedule_interval="@once",
    tags=["workshop"]
) as dag:


    t1 = PythonOperator(
        task_id="get_data_from_mysql",
        python_callable=get_data_from_mysql,
        op_kwargs={
            "transaction_path": mysql_output_path,
        },
    )
    """
    t2 = PythonOperator(
        task_id="get_data_from_api",
        python_callable=get_data_from_api,
        op_kwargs={
            "conversion_rate_path": conversion_rate_output_path,
        },
    )

    t3 = PythonOperator(
        task_id="merge_data",
        python_callable=merge_data,
        op_kwargs={
            "transaction_path": mysql_output_path,
            "conversion_rate_path": conversion_rate_output_path,
            "output_path" : final_output_path,
        }
    )
    """
    #[t1, t2] >> t3
    t1
