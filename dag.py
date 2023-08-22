from airflow.decorators import task , dag
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import json
import pandas as pd


#รายชื่อหมู่บ้านที่เข้าร่วมกิจกรรมส่งเสริมและพัฒนาการมีส่วนร่วมของชุมชนในพื้นที่ป่าอนุรักษ์ (สสอ.)
url_1 = 'https://gdcatalog.go.th/api/3/action/datastore_search?limit=5000&resource_id=7e7df749-3d96-4c66-ac50-cfef7d1a323b'
#ข้อมูลร้านค้า/กิจการท่องเที่ยวที่ได้รับอนุญาตในอุทยานแห่งชาติ ปี 2565
url_2 = 'https://catalog.dnp.go.th/api/3/action/datastore_search?resource_id=bd7e0154-d9b2-4c7e-b813-d4c52de69252'
#พื้นที่คุ้มครองทางทะเลและชายฝั่ง XLS
url_3 = 'https://gdcatalog.go.th/api/3/action/datastore_search?&resource_id=21d91385-2e9a-4eea-80ea-e4a11079ba6a'

postgres_conn = "postgres"

default_args = {
    'owner': 'aomji',
    'start_date': days_ago(0),
}


@task()
def api_to_dataframe(api:str):
    url = requests.get(api)
    json = url.json()
    records = json["result"]["records"]
    df = pd.DataFrame(records)
    return df
@task()
def load_to_database(df,tablename):
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    df.to_sql(name=f'{tablename}', con=pg_hook.get_sqlalchemy_engine(), if_exists='append') 
    print('Success')

@dag(default_args=default_args)
def taskflow_dag():
    df1 = api_to_dataframe(url_1)
    lo_1 = load_to_database(df1,"test01")
    df2 = api_to_dataframe(url_2)
    lo_2 = load_to_database(df2,"test02")
    df3 = api_to_dataframe(url_3)
    lo_3 = load_to_database(df3,"test03")

    [df1 >> lo_1],[df2 >> lo_2],[df3 >> lo_3]

dag_flow = taskflow_dag()