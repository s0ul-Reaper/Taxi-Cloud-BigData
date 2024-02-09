import logging
import json
import os
from dotenv import load_dotenv

from azure.storage.blob import BlobServiceClient
import azure.functions as func


app = func.FunctionApp()
load_dotenv()

BLOB_STORAGE_CONNECTION_STRING = os.getenv('BLOB_STORAGE_CONNECTION_STRING')
CONTAINER_RESULT = os.getenv('CONTAINER_RESULT')

blob_service_client = BlobServiceClient.from_connection_string(conn_str=BLOB_STORAGE_CONNECTION_STRING)

def process_json(json_content):
    # Your processing logic goes here
    container_result_client = blob_service_client.get_container_client(CONTAINER_RESULT)

    outbox = 'results.json'
    
    logging.info(f"Processing JSON content: {json_content}")
    data = json.loads(json_content)


    blob_result_client = container_result_client.get_blob_client(outbox)
    blob_result_content = blob_result_client.download_blob()
    json_string = blob_result_content.readall().decode('utf-8')

    result = json.loads(json_string)
    
    logging.info(f"Current result: {result}")

    if (float(data['pickup_latitude']) >= 40.735923  and float(data['pickup_longitude']) >= -73.990294):
        result['Q1'] += 1
    elif (float(data['pickup_latitude']) >= 40.735923  and float(data['pickup_longitude']) < -73.990294):
        result['Q2'] +=1
    elif (float(data['pickup_latitude']) < 40.735923  and float(data['pickup_longitude']) < -73.990294):
        result['Q3'] +=1
    else:        
        result['Q4'] +=1

    blob_result_client.upload_blob(json.dumps(result), overwrite=True)

@app.blob_trigger(arg_name="myblob", path="streamcheck/events/{name}.json", connection="AzureWebJobsStorage") 
def preprocess(myblob: func.InputStream):
    try:
        # Accessing the content of the blob
        json_content = myblob.read().decode('utf-8')
        
        # Call the function to process the JSON
        process_json(json_content)

        logging.info(f"Python blob trigger function processed blob\n"
                     f"Name: {myblob.name}\n"
                     f"Blob Size: {myblob.length} bytes")

    except Exception as e:
        logging.error(f"Error processing blob: {str(e)}")