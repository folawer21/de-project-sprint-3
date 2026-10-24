"""Daily sales increment: replace the day, then rebuild its complete ISO week."""
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path
import time

import pandas as pd
import requests
from psycopg2.extras import execute_values
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

API_CONNECTION = 'http_conn_id'
PG_CONNECTION = 'postgresql_de'
SQL_DIR = Path(__file__).parent / 'sql'
COLUMNS = ['uniq_id', 'date_time', 'city_id', 'city_name', 'customer_id',
           'first_name', 'last_name', 'item_id', 'item_name', 'quantity',
           'payment_amount', 'status']


def api_request(method, endpoint, **kwargs):
    connection = BaseHook.get_connection(API_CONNECTION)
    response = requests.request(
        method, connection.host.rstrip('/') + endpoint,
        headers={'X-Nickname': Variable.get('project_nickname'),
                 'X-Cohort': Variable.get('project_cohort'), 'X-Project': 'True',
                 'X-API-KEY': connection.extra_dejson['api_key']},
        timeout=60, **kwargs)
    response.raise_for_status()
    return response.json()


def get_report(ti, **context):
    report_id = (context['dag_run'].conf or {}).get('report_id') or Variable.get(
        'project_report_id', default_var=None)
    if not report_id:
        task_id = api_request('POST', '/generate_report')['task_id']
        for _ in range(30):
            result = api_request('GET', '/get_report', params={'task_id': task_id})
            if result['status'] == 'SUCCESS':
                report_id = result['data']['report_id']
                Variable.set('project_report_id', report_id)
                break
            if result['status'] != 'RUNNING':
                raise ValueError('Report generation failed: ' + str(result))
            time.sleep(10)
        else:
            raise TimeoutError('Report was not ready in 300 seconds')
    ti.xcom_push(key='report_id', value=report_id)


def get_increment(ds, ti, **context):
    report_id = ti.xcom_pull(task_ids='get_report', key='report_id')
    result = api_request('GET', '/get_increment', params={
        'report_id': report_id, 'date': ds + 'T00:00:00'})
    if result.get('status') != 'SUCCESS':
        raise ValueError('Increment is unavailable: ' + str(result))
    ti.xcom_push(key='increment_id', value=result['data']['increment_id'])


def prepare_orders(frame, day):
    frame = frame.copy()
    if 'status' not in frame:
        frame['status'] = 'shipped'
    frame['status'] = frame['status'].fillna('shipped')
    missing = set(COLUMNS) - set(frame.columns)
    if missing:
        raise ValueError('Missing CSV columns: ' + ', '.join(sorted(missing)))
    if not frame['status'].isin(['shipped', 'refunded']).all():
        raise ValueError('Unknown order status')
    frame['date_time'] = pd.to_datetime(frame['date_time'], errors='raise')
    if not frame['date_time'].dt.strftime('%Y-%m-%d').eq(day).all():
        raise ValueError('Increment contains dates outside the requested day')
    if frame['uniq_id'].isna().any():
        raise ValueError('Empty uniq_id')
    frame = frame[COLUMNS].drop_duplicates()
    if frame['uniq_id'].duplicated().any():
        raise ValueError('Conflicting rows share uniq_id')
    return frame.astype(object).where(pd.notna(frame), None)


def bootstrap_report(ti, **context):
    report_id = ti.xcom_pull(task_ids='get_report', key='report_id')
    conn = PostgresHook(postgres_conn_id=PG_CONNECTION).get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute('SELECT 1 FROM staging.etl_reports WHERE report_id=%s', (report_id,))
                if cur.fetchone():
                    return
                nickname = Variable.get('project_nickname')
                cohort = Variable.get('project_cohort')
                url = (f'https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/'
                       f'{nickname}/project/{report_id}/user_order_log.csv')
                response = requests.get(url, timeout=120)
                response.raise_for_status()
                source = pd.read_csv(StringIO(response.text))
                days = pd.to_datetime(source['date_time'], errors='raise').dt.strftime('%Y-%m-%d')
                if source.empty:
                    raise ValueError('Initial report is empty')
                for day in sorted(days.unique()):
                    frame = prepare_orders(source.loc[days.eq(day)], day)
                    write_orders(cur, frame, day)
                    apply_marts(cur, day)
                cur.execute('INSERT INTO staging.etl_reports (report_id) VALUES (%s)', (report_id,))
    finally:
        conn.close()


def write_orders(cur, frame, day):
    cur.execute('DELETE FROM staging.user_order_log WHERE date_time::date = %s::date', (day,))
    if not frame.empty:
        execute_values(cur,
            'INSERT INTO staging.user_order_log (' + ','.join(COLUMNS) + ') VALUES %s',
            list(frame.itertuples(index=False, name=None)), page_size=1000)


def apply_marts(cur, day):
    cur.execute('SELECT 1 FROM mart.d_calendar WHERE date_actual = %s::date', (day,))
    if cur.fetchone() is None:
        raise ValueError('Requested date is missing from mart.d_calendar: ' + day)
    for filename in ['mart.dimensions.sql', 'mart.f_sales.sql', 'mart.f_customer_retention.sql']:
        cur.execute((SQL_DIR / filename).read_text(), {'date': day})


def load_increment(ds, ti, **context):
    increment_id = ti.xcom_pull(task_ids='get_increment', key='increment_id')
    nickname = Variable.get('project_nickname')
    cohort = Variable.get('project_cohort')
    url = (f'https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/'
           f'{nickname}/project/{increment_id}/user_order_log_inc.csv')
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    frame = prepare_orders(pd.read_csv(StringIO(response.text)), ds)
    conn = PostgresHook(postgres_conn_id=PG_CONNECTION).get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                write_orders(cur, frame, ds)
    finally:
        conn.close()


def update_marts(ds, **context):
    conn = PostgresHook(postgres_conn_id=PG_CONNECTION).get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                apply_marts(cur, ds)
    finally:
        conn.close()


with DAG('sales_mart', start_date=datetime(2020, 1, 1), schedule_interval='@daily',
         catchup=False, max_active_runs=1,
         default_args={'owner': 'student', 'retries': 2,
                       'retry_delay': timedelta(seconds=30)},
         dagrun_timeout=timedelta(minutes=30)) as dag:
    report = PythonOperator(task_id='get_report', python_callable=get_report, provide_context=True)
    bootstrap = PythonOperator(task_id='bootstrap_report', python_callable=bootstrap_report, provide_context=True)
    increment = PythonOperator(task_id='get_increment', python_callable=get_increment, provide_context=True)
    load = PythonOperator(task_id='load_increment', python_callable=load_increment, provide_context=True)
    marts = PythonOperator(task_id='update_marts', python_callable=update_marts, provide_context=True)
    report >> bootstrap >> increment >> load >> marts
