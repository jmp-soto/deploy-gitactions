import os
import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb')
glue = boto3.client('glue')

DYNAMO_TABLE = os.environ['DYNAMO_TABLE']
ACCOUNT_ID = os.environ['ACCOUNT_ID']
IAM_ROLE = os.environ['IAM_ROLE']

def lambda_handler(event, context):
    
    
    GLUE_ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/{IAM_ROLE}"
    
    try:
        config_table = dynamodb.Table(DYNAMO_TABLE)
        tables_to_process = []
        response = config_table.scan()
        tables_to_process.extend(response['Items'])
        
        while 'LastEvaluatedKey' in response:
            response = config_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            tables_to_process.extend(response['Items'])
        
        results = []
        for table_config in tables_to_process:
            try:
                # Validar y obtener parámetros de dynamo
                db_name = table_config.get('DatabaseName')
                tbl_name = table_config.get('Table_name')
                
                if not db_name or not tbl_name:
                    raise ValueError("Configuración incompleta en DynamoDB")
                
                sample_size = float(table_config.get('sample_size', 10.0))
                
    
                tbl_def = glue.get_table(DatabaseName=db_name, Name=tbl_name)
                all_columns = [col['Name'] for col in tbl_def['Table']['StorageDescriptor']['Columns']]
                
                if not all_columns:
                    raise ValueError("No se encontraron columnas en la tabla")
                
                response = glue.start_column_statistics_task_run(
                    DatabaseName=db_name,
                    TableName=tbl_name,
                    Role=GLUE_ROLE_ARN,
                    ColumnNameList=all_columns,
                    SampleSize=sample_size
                )
                
                results.append({
                    'database': db_name,
                    'table': tbl_name,
                    'task_id': response['ColumnStatisticsTaskRunId'],
                    'columns_processed': len(all_columns),
                    'status': 'STARTED'
                })
                
                print(f"Tarea iniciada para {db_name}.{tbl_name}, ID: {response['ColumnStatisticsTaskRunId']}")
                print(f"Rol utilizado: {GLUE_ROLE_ARN}")
                
            except Exception as e:
                error_msg = f"Error procesando {table_config.get('table_name', 'tabla desconocida')}: {str(e)}"
                print(error_msg)
                results.append({
                    'database': db_name,
                    'table': tbl_name,
                    'error': error_msg,
                    'status': 'FAILED'
                })
        
        return {
            'statusCode': 200,
            'body': {
                'processed_tables': len(results),
                'success_count': len([r for r in results if r['status'] == 'STARTED']),
                'results': results,
                'execution_role': GLUE_ROLE_ARN
            }
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f"Error general: {str(e)}",
            'execution_role': GLUE_ROLE_ARN
        }
