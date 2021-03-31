#!/usr/bin/python3
import boto3
import json
import random
import time
import numpy as np
import pickle
import os

def js_print(arg):
    print(json.dumps(arg, indent=2))


def makeRandomBytes(size):
    random.seed(None)
    return os.urandom(size)

# endpoint and keys from vstart
endpoint = 'http://127.0.0.1:8000'
access_key="0555b35654ad1656d804"
secret_key="h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=="
writeData = bytes()
latencyResults = {}

# input
obj_prefix = "obj"
size = 4*2**10 # 4KB
num_op = 10

conn = boto3.resource('s3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key)

bucket = conn.Bucket('my-new-bucket')
bucket.create()

writeData = makeRandomBytes(size)

for i in range(num_op):
    start = time.perf_counter()
    bucket.put_object(Bucket="my-new-bucket",
            Key=obj_prefix + str(size) + "-" + str(i),
            Body=writeData,
            )
    end = time.perf_counter() - start
    latencyResults[str(i)] = str(end)            

test_name = obj_prefix + " - write"
print(latencyResults)
with open('latency', 'ab') as fw:
    fw.write(test_name.encode())
    fw.write('\n'.encode())
    fw.write(str(size).encode())
    fw.write('\n'.encode())
    for key, value in latencyResults.items():
        fw.write(key.encode())
        fw.write('\t'.encode())
        fw.write(value.encode())
        fw.write('\n'.encode())

test_name = obj_prefix + " - read"
latencyResults = {}
for i in range(num_op):
    start = time.perf_counter()
    obj = conn.Object("my-new-bucket",
            obj_prefix + str(size) + "-" + str(i),
            )
    body = obj.get()['Body'].read()
    end = time.perf_counter() - start
    latencyResults[str(i)] = str(end)

print(latencyResults)
with open('latency', 'ab') as fw:
    fw.write(test_name.encode())
    fw.write('\n'.encode())
    for key, value in latencyResults.items():
        fw.write(key.encode())
        fw.write('\t'.encode())
        fw.write(value.encode())
        fw.write('\n'.encode())
