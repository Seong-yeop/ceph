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
endpoint = 'http://172.31.4.82:80'
access_key="YG27U5VQH45T5EGC0DD9"
secret_key="JKQGPJtKV56pTFHZ6JDKGB6ImIgDM1pu9ofnWl4Y"
writeData = bytes()
size = 2**20 # 4KB
num_op = 1
latencyResults = {}

conn = boto3.resource('s3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key)


bucket = conn.Bucket('my-new-bucket')
#bucket.create()

writeData = makeRandomBytes(size)

for i in range(num_op):
    start = time.perf_counter()
    bucket.put_object(Bucket="my-new-bucket",
            Key="test" + str(i),
            Body=writeData,
            )
    end = time.perf_counter() - start
    latencyResults[str(i)] = str(end)            

print(latencyResults)
with open('latency', 'ab') as fw:
    for key, value in latencyResults.items():
        fw.write(key.encode())
        fw.write('\t'.encode())
        fw.write(value.encode())
        fw.write('\n'.encode())
