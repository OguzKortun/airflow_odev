from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
import pandas as pd
import requests

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

from datetime import datetime, timedelta
import psycopg2


with DAG(
    dag_id="homework",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    schedule_interval="0 0 * * *"
) as dag:

    client = MongoClient(
        "mongodb+srv://cetingokhan:cetingokhan@cluster0.ff5aw.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0",
        server_api=ServerApi("1"),
    )
    db = client["bigdata_training"]

    def generate_random_heat_and_humidity_data(dummy_record_count: int):
        import random
        import datetime
        from models.heat_and_humidity import HeatAndHumidityMeasureEvent

        records = []
        for i in range(dummy_record_count):
            temperature = random.randint(10, 40)
            humidity = random.randint(10, 100)
            timestamp = datetime.datetime.now()
            creator = "oguzkortun"  
            record = HeatAndHumidityMeasureEvent(temperature, humidity, timestamp, creator)
            records.append(record)
        return records

    def save_data_to_mongodb(records):
        collection = db["user_coll_oguzkortun"] 
        for record in records:
            collection.insert_one(record.__dict__)

    def create_sample_data_on_mongodb():
        """Her çalıştığında 10 adet rastgele kayıt üretir ve MongoDB'ye yazar."""
        records = generate_random_heat_and_humidity_data(10)
        save_data_to_mongodb(records)

    def copy_anomalies_into_new_collection():
        """
        user_coll_oguzkortun içinden temperature>30 olanları alıp
        user_coll_oguzkortun_anomalies koleksiyonuna kopyalar, creator güncellenir.
        """
        src = db["user_coll_oguzkortun"]
        dst = db["user_coll_oguzkortun_anomalies"]
        for doc in src.find({"temperature": {"$gt": 30}}):
            doc.pop("_id", None)
            doc["creator"] = "oguzkortun"
            dst.insert_one(doc)

    def copy_airflow_logs_into_new_collection():
        """
        Airflow Postgres log tablosundan son 1 dakikadaki
        event bazlı kayıt sayısını alıp MongoDB'ye yazar.
        """
        conn = psycopg2.connect(
            host="postgres",
            port="5432",
            database="airflow",
            user="airflow",
            password="airflow"
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT event, COUNT(*) AS record_count
            FROM log
            WHERE dttm >= NOW() - INTERVAL '1 minute'
            GROUP BY event
        """)
        rows = cur.fetchall()
        coll = db["user_coll_oguzkortun_logs"]
        for event_name, count in rows:
            coll.insert_one({
                "event_name":   event_name,
                "record_count": count,
                "created_at":   datetime.now(),
            })
        cur.close()
        conn.close()

    dag_start = DummyOperator(task_id="start")

    create_sample_task = PythonOperator(
        task_id="create_sample_data_on_mongodb",
        python_callable=create_sample_data_on_mongodb,
    )
    copy_anomalies_task = PythonOperator(
        task_id="copy_anomalies_into_new_collection",
        python_callable=copy_anomalies_into_new_collection,
    )
    copy_logs_task = PythonOperator(
        task_id="copy_airflow_logs_into_new_collection",
        python_callable=copy_airflow_logs_into_new_collection,
    )
    finaltask = DummyOperator(task_id="finaltask")


    dag_start >> [create_sample_task, copy_logs_task]
    create_sample_task >> copy_anomalies_task >> finaltask
    copy_logs_task   >> finaltask
