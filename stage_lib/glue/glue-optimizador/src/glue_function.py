import boto3
import pandas as pd
import json

pd.set_option("display.max_columns", 25)
pd.set_option("display.max_rows", 25)
# Initialize DynamoDB client################
# Initialize DynamoDB client#####
dynamodb = boto3.client('dynamodb')

# Define table name
TABLE_NAME = "table-da-eta"

def save_to_dynamodb(table_name, item):
    response = dynamodb.put_item(TableName=table_name, Item=item)
    return response

def parse_dynamodb_item(item):
    """
    Convert a single DynamoDB item into a readable Python dictionary.
    """
    parsed_item = {}
    for key, value in item.items():
        if "S" in value:
            parsed_item[key] = value["S"]  # Extract string values
        elif "N" in value:
            parsed_item[key] = float(value["N"]) if "." in value["N"] else int(value["N"])  # Convert numbers
        elif "BOOL" in value:
            parsed_item[key] = value["BOOL"]  # Extract boolean values
        elif "NULL" in value:
            parsed_item[key] = None  # Convert NULLs
        elif "M" in value:
            parsed_item[key] = parse_dynamodb_item(value["M"])  # Recursively parse nested maps
        elif "L" in value:
            parsed_item[key] = [parse_dynamodb_item(i["M"]) for i in value["L"] if "M" in i]  # Convert lists of maps
    return parsed_item

def dynamodb_to_dataframe():
    """
    Reads data from DynamoDB and converts it into a well-formatted Pandas DataFrame.
    """
    try:
        # Query the entire DynamoDB table
        response = dynamodb.scan(TableName=TABLE_NAME)
        
        # Extract items from response
        items = response.get("Items", [])

        # Convert DynamoDB format to a readable format
        parsed_items = [parse_dynamodb_item(item) for item in items]

        # Convert list to Pandas DataFrame
        df = pd.DataFrame(parsed_items)

        # ✅ Normalize the 'driver_data' column if it exists and contains a list
        if "driver_data" in df.columns and df["driver_data"].notna().any():
            df = df.explode("driver_data").reset_index(drop=True)  # Expand list into separate rows
            df_driver = pd.json_normalize(df["driver_data"])  # Flatten the nested JSON

            # Ensure unique column names
            df_driver.columns = [col.split('.')[-1] for col in df_driver.columns]

            # ✅ Reset index before merging to avoid reindexing issues
            df = df.reset_index(drop=True)
            df_driver = df_driver.reset_index(drop=True)

            # ✅ Merge DataFrames safely
            df = pd.concat([df.drop(columns=["driver_data"]), df_driver], axis=1, ignore_index=False)

        return df

    except Exception as e:
        print(f"Error reading from DynamoDB: {str(e)}")
        return None

def lambda_handler(event, context):
    # Read DynamoDB table and convert to DataFrame
    df = dynamodb_to_dataframe()

    if df is not None and not df.empty:
        print("\n--- Formatted DynamoDB Data as DataFrame ---")
        print(df)  # Print formatted DataFrame
        print(df.shape) # Matrix order
        print(df['checkout_time'].value_counts(dropna=False))
        print(df.columns)



        return {
            "statusCode": 200,
            "body": df.to_json(orient="records")
        }
    else:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "No data found or error in processing"})
        }
