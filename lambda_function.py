import boto3
import json
from datetime import datetime
import numpy as np
from pdf2image import convert_from_path
import firebase_admin
from firebase_admin import credentials, db
import io
from PIL import Image
import requests
import urllib.request
cred = credentials.Certificate('kaleem-fiverr-firebase-adminsdk-9pxjc-be860963e9.json')
firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://kaleem-fiverr-default-rtdb.firebaseio.com/'
})
url = "http://13.211.47.185/"


def lambda_handler(event, context):
    # TODO implement
    print(event)
    print("context", context)
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    try:
        input_file_key = key
        print("step 1")
        input_file_url = f"https://{bucket}.s3.ap-southeast-2.amazonaws.com/{input_file_key}"
        print("1 complete")

        print("step 2")
        ext = input_file_key.split(".")[-1]
        print("1",ext)
        ext = f".{ext}"
        print("ext", ext)
        data_out = process_file(input_file_url, ext)
        print(data_out)
        print("2 complete")

        print("step3")
        json_name_key = input_file_key.split(".")[0]
        datetime_str = datetime.now().strftime("%Y%m%d%H%M%S%f")
        json_file_name_key = f'{json_name_key}_{datetime_str}.json'
        upload_json_response = upload_json_to_s3(data_out, json_file_name_key)
        print("upload_json_response", upload_json_response)
        print("3 complete")

        print("step 4")
        save_output_url = f"https://output-json-datas.s3.ap-southeast-2.amazonaws.com/{json_file_name_key}"
        print("save_output_url", save_output_url)
        print("4 complete")

        print("step 5")
        json_data_for_firebase = {
            'actFileUrl': input_file_url,
            'extFileUrl': save_output_url,
            'dateTime': str(datetime.now()),
            'actFilename': str(key)
        }
        result = store_json_in_firebase(json_data_for_firebase)
        print(result)
        print("5 complete")

    except Exception as e:
        print("error", str(e))
        return {
            'statusCode': 500,
            'body': json.dumps(str(e))

        }
    return {
        'statusCode': 200,
        'body': json.dumps('Process completed successfully!!!')
    }


def store_json_in_firebase(json_data):
    try:
        # Reference to your Firebase Realtime Database
        ref = db.reference('/')

        # Push the JSON data to the specified path
        new_ref = ref.push()
        new_ref.set(json_data)
        print("Data stored successfully on firebase")
        return {'success': True, 'message': 'Data stored successfully'}

    except Exception as e:
        return {'success': False, 'error': str(e)}


def upload_json_to_s3(json_data, object_key):
    try:
        """
        Uploads a JSON object to an S3 bucket.

        Parameters:
        - json_data: The JSON object to be uploaded.
        - bucket_name: The name of the S3 bucket.
        - object_key: The key (path) under which the JSON object will be stored in the S3 bucket.
        """
        # Convert the JSON object to a string
        bucket_name = 'output-json-data'
        json_string = json.dumps(json_data, indent=4)

        # Create an S3 client (no need to provide access key ID and secret access key)
        s3 = boto3.client('s3')

        # Upload the JSON string to S3
        s3.put_object(Body=json_string, Bucket=bucket_name, Key=object_key)
        return "Json saved on bucket"
    except Exception as e:
        return f"File not saved due to : {str(e)}"


def process_file(input_path, ext):
    try:
        payload = {'input_path': input_path,
                   'ext': ext}
        headers = {}
        response = requests.request("POST", url, headers=headers, data=payload)
        print(response.text)
        response_text = json.loads(response.text)
        data = response_text['data']
        return data
    except Exception as e:
        print(e)
        return ''
