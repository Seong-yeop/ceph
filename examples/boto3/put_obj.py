#!/usr/bin/python3

import boto3
import json
import random
import time
import numpy as np
import pickle
import os
import parmap
import multiprocessing

def js_print(arg):
    print(json.dumps(arg, indent=2))


def makeRandomBytes(size):
    random.seed(None)
    return os.urandom(size)

# endpoint and keys from vstart
endpoint = 'http://172.31.4.82:7480'
###endpoint = 'http://127.0.0.1:8000'
access_key="2TN5LW0UDC88UB8YOG0V"
secret_key="Cad17lZQcxRe7vhA60xPuSHEhR2akedLl8jbroG8"
writeData = bytes()
sizes = [4*2**10, 16*2**10, 64*2**10, 256*2**10, 2**20, 4*2**20]
num_op = 10000
latencyResults = {}

client = boto3.client('s3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key)

resp = client.create_bucket(Bucket="my-new-bucket")

for size in sizes:
    writeData = makeRandomBytes(size)
    print("PUTOBJ SIZE: ", size)
    for i in range(num_op):
        start = time.perf_counter()
        resp = client.put_object(Bucket="my-new-bucket",
                Key="obj"+ str(size) + "-" +str(i),
                Body=writeData,
                )
        elasped_time = time.perf_counter() - start
        latencyResults[str(i)] = elasped_time
    count = 0
    _sum = 0
    for key in latencyResults:
        count += 1
        _sum += latencyResults[key]
    latencyResults = {}
        
    print("objectSize %d, latency (ms): %f" % (size, (_sum/count)*10**3 ))
