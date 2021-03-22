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

def put_object(input_list):
    print("put... start ...")
    client = boto3.client('s3',
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key)

    resp = client.create_bucket(Bucket="my-new-bucket")
    
    latencyResults = []
    for i in input_list:
        start = time.perf_counter()
        resp = client.put_object(Bucket="my-new-bucket",
                Key="obj"+ str(size) + "-" +str(i),
                Body=writeData,
                )
        elasped_time = time.perf_counter() - start
        latencyResults.append(elasped_time)

    return latencyResults

# endpoint and keys from vstart
endpoint = 'http://172.31.4.82:80'
access_key="14PQ0HEUG876CL989VMF"
secret_key="tY2vafwxBWjszKkAHvCu0szTihBoUZL7N5nflAUQ"
writeData = bytes()
sizes = [4*2**10, 16*2**10, 64*2**10, 256*2**10, 2**20, 4*2**20]
num_op = 10000


num_cores = 100
data = list(range(num_op))
splited_data = np.array_split(data, num_cores)
splited_data = [x.tolist() for x in splited_data]


for size in sizes:
    latencyResults = []
    writeData = makeRandomBytes(size)
    print("PUTOBJ SIZE: ", size)

    latencyResults = parmap.map(put_object, splited_data, pm_processes=num_cores)
        
    latencyResult = []
    for element in latencyResults:
        latencyResult += element

    avg = np.mean(latencyResult)
    
    print("Done op: %d" % (len(latencyResult)))
    print("objectSize %d, latency (ms): %f, throughput (MB/s): %f" % (size, avg*10**3, num_op*size/(np.sum(latencyResult))/2**20))

