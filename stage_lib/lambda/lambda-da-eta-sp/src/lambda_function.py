import requests
import time
import pandas as pd
import numpy as np
import math
import boto3
import os
import pytz
import json
from datetime import datetime, timedelta
import awswrangler as wr
import re
import uuid

#TOKEN_PUENTE_ALTO = "232efc5b2944c4dc24663d33c725572555231c75"
TOKEN_PUENTE_ALTO = "d391b9977d3911c9dc905b560bc4626a3b1f4133"
HEADERS = {"Authorization": f"Token {TOKEN_PUENTE_ALTO}"}
#BUCKET_TARGET = os.getenv('BUCKET_NAME')  
bucket_target = 'da-ecodistribucion-stage-file-dev'  
path_target = 'output/regional/eta/simplyroute/'  
tz_santiago = pytz.timezone('America/Santiago')
fecinformacion = datetime.now(tz_santiago).strftime('%Y-%m-%d')
#sd_fecinformacion = datetime.now(tz_santiago).strftime('%Y%m%d')
s_fecinformacion = datetime.now(tz_santiago).strftime('%Y%m%d%H%M%S')
start_time_route = datetime.now(tz_santiago).strftime('%Y-%m-%dT%H:%M:%S')
table_target = 'vw_dim_material_eta'
# Inicializar cliente de S3
s3_client = boto3.client('s3')
database = 'db_ecodistribucion_regional_stage'
table_dynamo = 'table-da-eta'
dynamodb = boto3.client('dynamodb')
sqs = boto3.client('sqs')

QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/149536487709/test-sqs-da-eta-sp.fifo'

query = f'''
select * from {database}.{table_target}
'''
def format_dynamodb_item(value):
    """Convierte un valor de pandas en el formato correcto de DynamoDB"""
    if pd.isna(value):
        return {"NULL": True}
    elif isinstance(value, (int, float)):
        return {"N": str(value)}
    elif isinstance(value, bool):
        return {"BOOL": value}
    else:
        return {"S": str(value)}

def dataframe_to_dynamo_json(df):
    route_id = df["route"].iloc[0]  
    planned_date = df["planned_date"].iloc[0]  
    
    driver_data = []
    
    for _, row in df.iterrows():
        item = {
            "M": {
                "driver_id": format_dynamodb_item(row["driver"]),
                "latitude": format_dynamodb_item(row["latitude"]),
                "longitude": format_dynamodb_item(row["longitude"]),
                "address": format_dynamodb_item(row["address"]),
                "id": format_dynamodb_item(row["id"]),
                "order": format_dynamodb_item(row["order"]),
                "reorder": format_dynamodb_item(row["reorder"]),
                "checkout_time": format_dynamodb_item(row["checkout_time"]),
                "visit": format_dynamodb_item(row["visit"]),
                "start_time_route": format_dynamodb_item(row["start_time_route"]),
                "tiempo_descarga": format_dynamodb_item(row["tiempo_descarga"]),
                "travel_time_minutes": format_dynamodb_item(row["travel_time_minutes"])
                
            }
        }
        driver_data.append(item)
    
    dynamo_json = {
        "route_id": {"S": route_id},
        "planned_date": format_dynamodb_item(planned_date),
        "driver_data": {"L": driver_data}
    }
    
    return dynamo_json

def save_to_dynamodb(table_name, item):
    response = dynamodb.put_item(TableName=table_name, Item=item)
    return response

def realizar_solicitud_get(url, params=None, headers=HEADERS, delay=2):
    try:
        respuesta = requests.get(url, headers=headers, params=params)
        time.sleep(delay)
        if respuesta.status_code == 200:
            return respuesta.json()
        else:
            print(f"Error {respuesta.status_code}: {respuesta.text}")
            return 
    except Exception as e:
        print(f"Error al conectar con el API: {e}")
        return 

def obtener_usuarios():
    url_usuarios = "https://api.simpliroute.com/v1/accounts/users/"
    usuarios = realizar_solicitud_get(url_usuarios)
    if usuarios:
        return [
            {
                "id": usuario["id"],
                "es_conductor": usuario["is_driver"],
                "bloqueado": usuario["blocked"],
                "estado": usuario["status"],
            }
            for usuario in usuarios
        ]
    return []

def calcular_tiempo_descarga(peso_kg, num_cajas):
    tiempo_base = 5
    tiempo_peso = math.ceil(peso_kg / 150) * 3
    tiempo_cajas = math.ceil(num_cajas / 20) * 1
    tiempo_total = tiempo_base + tiempo_peso + tiempo_cajas
    return round(tiempo_total)

def lambda_handler(event, context):
    print(event)
    msn_event= event["Records"][0]["Sns"]["Message"]
    msn_event = json.loads(msn_event)
    print(msn_event)
    type_event = msn_event["event_name"]
    input_event = msn_event["content"]["input"]
    route_id = None
    
    if type_event == 'registerEventRoute':
        type =  input_event["type"]
        if type == 'FINISH':
            print((f'El evento enviado {type} solo finaliza la ruta'))
            return 
        else:
            route_id = input_event["routeId"]
            print(route_id)
            
            #driver_id = event.get('driver_id')
            fecha_planificada = fecinformacion
            #fecha_planificada = '2025-02-11'
            
            #usuarios = obtener_usuarios()
            #usuario = next((u for u in usuarios if u["id"] == int(driver_id)), None)
            #f870e735-eb26-4afb-8953-7e0f144144ea
            url_visitas = f'https://api.simpliroute.com/v1/routes/visits?route={route_id}'
            #url_visitas = f'https://api.simpliroute.com/v1/routes/visits/?driver={driver_id}&planned_date={fecha_planificada}'
            respuesta = requests.get(url_visitas, headers=HEADERS)
            print(respuesta)
            df_visitas = pd.DataFrame(respuesta.json())
            print(df_visitas)
            df_visitas['id_client'] = df_visitas['title'].apply(lambda x: re.search(r'(\d+)', str(x)).group(1) if pd.notna(x) and re.search(r'(\d+)', str(x)) else None)
            df_visitas = df_visitas[['driver','planned_date','latitude','longitude','id','order','address','checkout_time','route','id_client']]

            url_base_facturas = "https://api.simpliroute.com/v1/accounting/invoices?route_id={}"
            df_facturas = pd.DataFrame()

            for ruta in df_visitas['route'].unique():
                url_factura = url_base_facturas.format(ruta)
                respuesta = requests.get(url_factura, headers=HEADERS)
                if respuesta.status_code == 200:
                    datos_api = respuesta.json()
                    df_api_datos = pd.DataFrame(datos_api)
                    df_facturas = pd.concat([df_facturas, df_api_datos], ignore_index=True)

            df_facturas_normalizado = df_facturas.explode('items')
            df_items = pd.json_normalize(df_facturas_normalizado['items'])
            df_items.rename(columns={'id': 'id_producto', 'reference': 'referencia_producto'}, inplace=True)

            df_final = pd.concat([df_facturas_normalizado.drop(columns=['items']).reset_index(drop=True), df_items.reset_index(drop=True)], axis=1)

            df_dim_material = wr.athena.read_sql_query(query,database = database)
            df_dim_material['material'] = df_dim_material['material'].astype(str).str.lstrip('0')
            df_dim_material['flag_retorable'] = np.where(df_dim_material['return_pack_text']=='No Retornable','NR','R')

            df_unido = df_final.merge(df_dim_material[['material','flag_retorable','peso_neto']], how='left', left_on='referencia_producto', right_on='material')

            df_visita = df_unido.groupby('visit').apply(
                lambda x: pd.Series({
                    'peso_total': (x['peso_neto'] * x['planned_units']).sum(),
                    'unidades_planificadas': x['planned_units'].sum(),
                    'flag_retorable_NR': (x['flag_retorable'] == 'NR').max(),
                    'flag_retorable_R': (x['flag_retorable'] == 'R').max()
                })
            ).reset_index()

            df_visita['tiempo_descarga'] = df_visita.apply(
                lambda fila: calcular_tiempo_descarga(fila['peso_total'], fila['unidades_planificadas']), axis=1
            )

            df_rutas = df_visitas.merge(df_visita, how='left', left_on='id', right_on='visit')
            df_rutas = df_rutas.sort_values(['order'])
            df_rutas = df_rutas.reset_index(drop=True)
            df_rutas['start_time_route'] = None
            df_rutas['reorder'] = None
            df_rutas['travel_time_minutes'] = None
            df_rutas.loc[0, 'start_time_route'] = start_time_route
            df_rutas = df_rutas[['route','planned_date','driver','latitude','longitude','address','id','order','reorder','checkout_time','visit','start_time_route','tiempo_descarga','travel_time_minutes']]
            dynamo_json = dataframe_to_dynamo_json(df_rutas)
            save_to_dynamodb(table_dynamo, dynamo_json)

            try:
                message_body = json.dumps({
                    "route_id": route_id
                })
            
                message_deduplication_id = str(uuid.uuid4())  # Genera un UUID único

                # Enviar el mensaje a SQS FIFO
                response = sqs.send_message(
                    QueueUrl=QUEUE_URL,
                    MessageBody=message_body,
                    MessageGroupId="defaultGroup",  # Obligatorio para colas FIFO
                    MessageDeduplicationId=message_deduplication_id  # Obligatorio si no está activada la deduplicación basada en contenido
                )
                response_data = {
                    "message": "Mensaje enviado con éxito",
                    "messageId": response['MessageId']
                }
                print("Mensaje enviado con éxito:", response_data)
            except Exception as e:
                error_message = {"error": str(e)}
                print("Error al enviar el mensaje:", error_message)

            return {
                'statusCode': 200,
                'body': f'Archivo cargado exitosamente en table dynamo:{table_dynamo}'
            }
    elif type_event == 'saveVisitsOrder':
        route_id = input_event["routeId"]
        j_visits = input_event["visits"]
        df_visita_reorder = pd.DataFrame(j_visits)
        df_visita_reorder = df_visita_reorder[['id','order']]
        df_visita_reorder = df_visita_reorder.rename(columns={"order": "reorder"})
            # 1. Consultar solo los elementos con ese route_id
        response = table_dynamo.query(
            KeyConditionExpression="route_id = :route_id",
            ExpressionAttributeValues={":route_id": '2bec68a2-95de-43ca-9c3d-8752b210b237'})
        items = response.get("Items", [])
        #response = table_dynamo.scan(
        #    FilterExpression="route_id = :route_id",
        #    ExpressionAttributeValues={":route_id": route_id}
        #)
        #items = response.get("Items", [])

        if not items:
            return {
                "statusCode": 404,
                "body": f"No se encontraron elementos para route_id {route_id}"
            }
        
        df = pd.DataFrame(items)
        df = df.sort_values(by="id").reset_index(drop=True)
        df_visita_reorder["id"] = df_visita_reorder["id"].astype("int64")
        df["id"] = df["id"].astype("int64")
        df_updated = df.merge(df_visita_reorder, on="id", how="left")
        df_updated["reorder"] = df_updated["reorder_new"].combine_first(df_updated["reorder"])
        df_updated.drop(columns=["reorder_new"], inplace=True)
        with table_dynamo.batch_writer() as batch:
            for _, row in df_updated.iterrows():
                batch.put_item(Item=row.to_dict())

        return {
            "statusCode": 200,
            "body": f"Actualizados {len(df_updated)} elementos de route_id {route_id}"
        }
    else:
        return(f'el evento:{type_event}, no ha sido reconocido')

        