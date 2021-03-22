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


def get_object(input_list):
    print("get... start...")
    client = boto3.client("s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
            )
    
    latencyResults = []
    for i in input_list:
        start = time.perf_counter()
        resp =  client.get_object(Bucket="my-new-bucket",
            Key=obj_prefix + str(size) + "-" + str(i),
            )
        end = time.perf_counter() - start
        elasped_time = time.perf_counter() - start
        latencyResults.append(elasped_time)
        
    return latencyResults
    

# endpoint and keys from vstart
endpoint = 'http://172.31.4.82:80'
#endpoint = 'http://127.0.0.1:8000'
access_key="14PQ0HEUG876CL989VMF"
secret_key="tY2vafwxBWjszKkAHvCu0szTihBoUZL7N5nflAUQ"

sizes = [ 4*2**10,] #16*2**10, 64*2**10, 256*2**10, 2**20, 4*2**20 ]
num_op = 1000
obj_prefix = "obj"

num_cores = 1000

data = [ random.randint(1,9999) for _ in range(num_op) ]
splited_data = np.array_split(data, num_cores)
splited_data = [x.tolist() for x in splited_data]

for size in sizes:
    latencyResults = []
    
    latencyResults = parmap.map(get_object, splited_data, pm_processes=num_cores)

    latencyResult = []
    for element in latencyResults:
        latencyResult += element

    avg = np.mean(latencyResult)

    print("Done op %d" % (len(latencyResult)))
    print("ObjectSize %d ,Latency (ms): %f, Throughput (MB/s)" % (size, avg*10**3, num_op*size/(np.sum(latencyResult))/2**20)) 
