import requests
import pandas as pd
import awswrangler as wr
import boto3
import os
import json
from datetime import datetime, timedelta

SECRETS_NAME = os.environ.get('SKYCOP_TOKEN_SECRETS_NAME')
secrets_manager = boto3.client('secretsmanager')
analytics_bucket_name = os.environ.get("ANALYTICS_BUCKET_NAME")
bucket_prefix = 'py/skycop/eventos_ent_sal'


def get_token_skycop():
    try:
        response = secrets_manager.get_secret_value(SecretId=SECRETS_NAME)
        secrets = json.loads(response['SecretString'])
        return secrets.get("key-skycop", {}).get("token")
    except Exception as e:
        print(f"Error al obtener el token de Skycop: {e}")
        return None

def lambda_handler(event, context):
    try:
        token = get_token_skycop()
        if not token:
            return {"statusCode": 500, "body": "No se pudo obtener el token de Skycop"}

        dateFrom = (datetime.today() - timedelta(days=1)).date()
        year = dateFrom.strftime('%Y')
        month = dateFrom.strftime('%m')
        day = dateFrom.strftime('%d')

        dateTo = datetime.today().date()
        api_url = f"https://api.skycop.com.py/external/fleet_geofence_events2?key={token}&dateFrom={dateFrom}&dateTo={dateTo}"
        data = []
        try:
            response = requests.get(api_url)

            if response.status_code == 200:
                results = response.json()
                data = results['results']
        except Exception as e:
            print(f'Error when calling the API:  {e}')
            raise e
        df = pd.DataFrame(data)
        if df.empty:
            return {"statusCode": 204, "body": "Sin datos para procesar"}

        df = df.rename(columns={'date': 'date_time'})

        df['date_time'] = df['date_time'].astype(str)
        df['fecha'] = pd.to_datetime(df['date_time'], errors='coerce').dt.strftime('%Y-%m-%d')
        df['description'] = df['description'].astype(str)
        df['deviceId'] = df['deviceId'].astype(str)
        df['deviceNumber'] = df['deviceNumber'].astype(str)
        df['driverName'] = df['driverName'].astype(str)
        df['event'] = df['event'].astype(str)
        df['geofence'] = df['geofence'].astype(str)
        df['heading'] = pd.to_numeric(df['heading'], errors='coerce').astype(float)
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce').astype(float)
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce').astype(float)
        df['speed'] = pd.to_numeric(df['speed'], errors='coerce').astype(float)
        df['title'] = df['title'].astype(str)

        df['year'] = year
        df['month'] = month
        df['day'] = day

        df = df[['fecha', 
                'deviceId', 
                'date_time', 
                'description', 
                'deviceNumber', 
                'driverName',
                'event', 
                'geofence', 
                'heading', 
                'latitude', 
                'longitude', 
                'speed', 
                'title',
                'year', 
                'month', 
                'day']].copy()        

        wr.s3.to_parquet(
            df=df,
            path=f"s3://{analytics_bucket_name}/{bucket_prefix}",
            dataset=True,
            partition_cols=['year', 'month', 'day'],
            mode='overwrite_partitions'
        )

        return {
            "statusCode": 200,
            "body": f"Datos guardados exitosamente en S3://{analytics_bucket_name}/{bucket_prefix}/year={year}/month={month}/day={day}/ "
        }

    except Exception as e:
        return {"statusCode": 500, "body": f"Error: {str(e)}"}
