#!/usr/bin/python3

import boto3
import json

def js_print(arg):
    print(json.dumps(arg, indent=2))

# endpoint and keys from vstart
endpoint = 'http://127.0.0.1:8000'
access_key="0555b35654ad1656d804"
secret_key="h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=="

conn = boto3.resource('s3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key)


bucket = conn.Bucket('my-new-bucket')
bucket.create()

for i in range(100):
    bucket.put_object(Bucket="my-new-bucket",
            Key="obj" + str(i),
            Body="hello",
            )
