from datetime import datetime, timedelta, timezone
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from sqlalchemy.sql.operators import endswith_op
from airflow import DAG 
from botocore.exceptions import ClientError
import boto3
import json
import requests
import logging

units = {
    'rain':'mm',
    'temperature':'degC',
    'air'  :'µg/m³',
    'sound': 'dB(A)'
    }
ulrs = {
    'rain':"https://www.daggegevens.knmi.nl/klimatologie/daggegevens",
    'air'  : "https://u50g7n0cbj.execute-api.us-east-1.amazonaws.com/v2/measurements?location_id=2306&date_from={}&date_to={}&limit=45000&page=1&offset=0&sort=desc&parameter=no2&parameter=pm10&radius=1000&city=Eindhoven&order_by=datetime",
    'sound': "https://opendata.munisense.net/api/v2/eindhoven2-geluid/soundmeasurementpoints/476/laeq/query/presets/last_day"
    }

S3_BUCKET = "estar"
# S3_INPUT_KEY = S3_BUCKET + "/star_input" + "{0}.csv"
S3_STAGING_KEY = "/star_staging/star_data.json"

def save_data_to_s3(star_data):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """    
    from io import StringIO    
    LOGGER = logging.getLogger("airflow.task")
    LOGGER.info("airflow.task >>> save_data_to_s3")  

    json_buffer = StringIO()

    star_data.to_json(json_buffer)

    s3 = boto3.resource('s3')
    star_bucket = s3.Bucket(S3_BUCKET)
    s3_key = S3_STAGING_KEY.format('star_data')
    try:
        star_bucket.put_object(Key=s3_key, Body=json_buffer.getvalue())
    except Exception as e:
        LOGGER.info("airflow.task >>> Error >> {}".format(e)) 
        pass
    LOGGER.info("airflow.task >>> Successfully uploaded STAR data to S3")


def get_rain_temperature_data(**context):
    import pandas as pd

    execution_date = context['execution_date']

    LOGGER = logging.getLogger("airflow.task")
    LOGGER.info("airflow.task >>> get_rain_temperature_data")    

    execution_date = datetime.fromtimestamp(execution_date.timestamp()).date().strftime("%Y-%m-%d")
    LOGGER.info("airflow.task >>> execution_date {}".format(execution_date))    
    from_date = execution_date  - timedelta(days=1)
    to_date = execution_date  - timedelta(days=1)

    data_to_send = {
        "stns":"370",
        "vars":"TG:RH",
        "start":from_date,
        "end":to_date,
        "inseason":'N',
        "fmt":'json'
    }
    url = ulrs['rain']
    response = requests.post(url, data = data_to_send)
    if response.status_code != 200:
        LOGGER.info("airflow.task >>> Error getting response >> code: {}".format(response.status_code))  
        return

    rain_data = pd.DataFrame(response.json()) 

    rain_data = rain_data.rename({'TG':'avg_temp'}, axis=1)
    rain_data = rain_data.rename({'RH':'avg_rain'}, axis=1)
    rain_data = rain_data.drop(['station_code'],axis=1)
    rain_data['date'] = pd.to_datetime(rain_data['date'], dayfirst = True).dt.strftime("%Y-%m-%d")

    return rain_data

def get_air_data(**context):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """    
    import pandas as pd
    from datetime import timedelta

    execution_date = context['execution_date']

    LOGGER = logging.getLogger("airflow.task")
    LOGGER.info("airflow.task >>> get_air_data")    
    execution_date = datetime.fromtimestamp(execution_date.timestamp()).date()
    LOGGER.info("airflow.task >>> execution_date {}".format(execution_date)) 
    from_date = execution_date - timedelta(days=1)
    to_date = execution_date
    
    url = ulrs['air'].format(from_date, to_date)
    LOGGER.info("airflow.task >>> execution URL {}".format(url))
    air_data = pd.DataFrame()
    #Retrieve the 'results' attribute using a JSON interpreter
    air_list = pd.DataFrame(requests.get(url, timeout=None).json()['results'])
    for index, value in air_list["date"].to_dict().items():
        air_list["date"].iloc[index] = datetime.fromisoformat(value['utc'])
        
    air_list["date"] = pd.to_datetime(air_list["date"], format='%d-%m-%Y %H:%M').dt.date
    air_no2 = air_list[(air_list['parameter']=='no2') & (air_list['value']>0)]
    air_no2 = air_no2.groupby(['date'], as_index=False)['value'].mean()
    air_no2 = air_no2.rename({'value':'avg_no2'}, axis=1)
    air_pm10 = air_list[(air_list['parameter']=='pm10') & (air_list['value']>0)]
    air_pm10 = air_pm10.groupby(['date'], as_index=False)['value'].mean()
    air_pm10 = air_pm10.rename({'value':'avg_pm10'}, axis=1)
    air_data= pd.merge(air_no2, air_pm10, how='outer', on='date')
    air_data['date'] = pd.to_datetime(air_data['date'], dayfirst = True).dt.strftime("%Y-%m-%d")

    return air_data

def get_sound_data(ti, **context):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """    

    import pandas as pd
    from datetime import date
    from airflow.exceptions import AirflowSkipException

    execution_date = context['execution_date'] 

    LOGGER = logging.getLogger("airflow.task")
    LOGGER.info("airflow.task >>> get_sound_data") 
    execution_date = datetime.fromtimestamp(execution_date.timestamp()).date().strftime("%Y-%m-%d")
    execution_date -=  timedelta(days=1)
    LOGGER.info("airflow.task >>> execution_date {}".format(execution_date))  

    LOGGER.info("airflow.task >>> Getting today's data")
    url = ulrs['sound']

    #Retrieve the 'results' attribute using a JSON interpreter
    sound_data = pd.DataFrame(requests.get(url).json()['results'])
    sound_data['timestamp'] = pd.to_datetime(sound_data['timestamp'], dayfirst = True).dt.strftime("%Y-%m-%d")
    sound_data = sound_data.rename({'avg':'avg_laeq','timestamp':'date'}, axis=1)
    sound_data = sound_data.drop(['min','max'],axis=1)

    return sound_data

def merge_all_data(ti, **context):
    import pandas as pd
    LOGGER = logging.getLogger("airflow.task")
    LOGGER.info("airflow.task >>> Merging all data") 

    retrieved_data = ti.xcom_pull(task_ids=['collect_air_data', 'collect_sound_data', 'collect_temperature_rain_data'])

    star_data = pd.concat([retrieved_data[0],retrieved_data[1],retrieved_data[2]])

    star_data = star_data.sort_values(by='date')
    star_data = star_data.groupby('date').agg({'avg_no2':'sum', 'avg_pm10':'sum', 'avg_laeq':'sum', 'avg_temp':'sum', 'avg_rain':'sum'}).reset_index()
    star_data['date'] = pd.to_datetime(star_data['date'], dayfirst = True).dt.strftime("%Y-%m-%d")

    save_data_to_s3(star_data)

    return star_data

def store_in_db(ti, **context):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    import psycopg2
    import psycopg2.extras    

    conn = None
    star_data = ti.xcom_pull(task_ids=['merge_all_data'])
    LOGGER = logging.getLogger("airflow.task")

    try:
        conn = psycopg2.connect(
            host="postgres.xxxxxxxxxxxx.xx-xxxx-x.rds.amazonaws.com",
            database="postgres",
            user="postgres",
            password="postgres")

        cur = conn.cursor()

        cur.execute("""CREATE TABLE IF NOT EXISTS estar (
            id SERIAL PRIMARY KEY,
            date DATE,
            avg_no2 FLOAT,
            avg_pm10 FLOAT,
            avg_laeq FLOAT,
            avg_temp INTEGER,
            avg_rain INTEGER)""")
        conn.commit()
        psycopg2.extras.execute_batch(cur, """
            INSERT INTO starTemp(date, avg_no2, avg_pm10, avg_laeq, avg_temp, avg_rain) VALUES (
                %(date)s,
                %(avg_no2)s,
                %(avg_pm10)s,
                %(avg_laeq)s,
                %(avg_temp)s,
                %(avg_rain)s
            );
        """, star_data)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        LOGGER.info("airflow.task >>> DB store error: {}".format(error))
    finally:
        if conn is not None:
            conn.close()
            LOGGER.info("airflow.task >>> Database connection closed.")

# Set Schedule: Run pipeline once a day.
# Use cron to define exact time (UTC). Eg. 8:15 AM would be '15 08 * * *'
schedule_interval = '35 20 * * *'

dag = DAG('eindhoven-star_daily',
        default_args={
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        schedule_interval="@daily",
        start_date=datetime(2018,1,1),
        catchup=False
)

dag.doc_md = __doc__

# Echo task start
task_start = BashOperator(
    task_id = 'start_task',
    bash_command = 'echo start',
    dag = dag
)

# Task 1.1.1: Get daily sound data
collect_sound_data = PythonOperator(
    task_id = 'collect_sound_data',
    provide_context = True,
    python_callable = get_sound_data,
    dag = dag
)

# Task 1.2.1: Get temperature and rain data
collect_temperature_rain_data = PythonOperator(
    task_id = 'collect_temperature_rain_data',
    provide_context = True,
    python_callable = get_rain_temperature_data,
    dag = dag
)

# Task 1.3.1: Get air data
collect_air_data = PythonOperator(
    task_id = 'collect_air_data',
    provide_context = True,
    python_callable = get_air_data,
    dag = dag
)

# Task 2: Merge all data
merge_all_data = PythonOperator(
    task_id = 'merge_all_data',
    python_callable = merge_all_data,
    dag = dag
)

# Task 3: Store all data
store_in_db = PythonOperator(
    task_id = 'store_in_db',
    python_callable = store_in_db,
    dag = dag
)

# Echo task finish
finish_start = BashOperator(
    task_id = 'finish_task',
    bash_command = 'echo finish',
    dag = dag
)

# Set up the dependencies
task_start >> collect_sound_data >> merge_all_data >> store_in_db
task_start >> collect_temperature_rain_data >> merge_all_data >> store_in_db
task_start >> collect_air_data >> merge_all_data >> store_in_db
store_in_db >> finish_start