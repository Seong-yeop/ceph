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
###endpoint = 'http://127.0.0.1:8000'
access_key="CJ2518AEYXXX6S5TUESG"
secret_key="kGRP7sKXiT3uxGpBm7lNDAr4iC1ZUkJmBSLN4H28"
writeData = bytes()
latencyResults = {}

# input
obj_prefix = "2-obj"
size = 16*2**20 # 4KB
num_op = 100


client = boto3.client("s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
        )


resp = client.create_bucket(Bucket="my-new-bucket")

writeData = makeRandomBytes(size)
'''
for i in range(num_op):
    start = time.perf_counter()
    resp = client.put_object(Bucket="my-new-bucket",
            Key=obj_prefix + str(i),
            Body=writeData,
            )
    end = time.perf_counter() - start
    latencyResults[str(i)] = end            

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
        fw.write(str(value).encode())
        fw.write('\n'.encode())
'''
rand_op = [ random.randint(1,1000) for _ in range(num_op) ]

test_name = obj_prefix + " - read"
latencyResults = {}
for i in rand_op:
    start = time.perf_counter()
    resp =  client.get_object(Bucket="my-new-bucket",
        Key=obj_prefix + str(size) + "-" + str(i),
        )
    end = time.perf_counter() - start
    latencyResults[str(i)] = end

count = 0
_sum = 0
for key in latencyResults:
    count += 1
    _sum += latencyResults[key]
print("latency:", _sum/count) 

'''
with open('latency', 'ab') as fw:
    fw.write(test_name.encode())
    fw.write('\n'.encode())
    for key, value in latencyResults.items():
        fw.write(key.encode())
        fw.write('\t'.encode())
        fw.write(value.encode())
        fw.write('\n'.encode())
'''
