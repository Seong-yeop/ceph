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
access_key="WCJUNLBZHMP71O15NTMW"
secret_key="9gMNRJLlt58xnV5kBKS4L0E6SEDnCraVFgvWbHy5"

sizes = [ 4*2**10, 16*2**10, 64*2**10, 256*2**10, 2**20, 4*2**20 ]
obj_prefix = "obj"

num_cores = [1]

for num_core in num_cores:
    print("=" * 50)
    print("client :", num_core)
    num_op = 1000
    data = [ random.randint(1,9999) for _ in range(num_op) ]
    splited_data = np.array_split(data, num_core)
    splited_data = [x.tolist() for x in splited_data]
    for size in sizes:
        latencyResults = []
        start = time.perf_counter() 
        latencyResults = parmap.map(get_object, splited_data, pm_processes=num_core)
        elasped_time = time.perf_counter() - start

        latencyResult = []
        for element in latencyResults:
            latencyResult += element

        avg = np.mean(latencyResult)

        print("Done op %d" % (len(latencyResult)))
        print("Total time (s): %f" % (elasped_time))
        print("ObjectSize %d ,Latency (ms): %f, Throughput (MB/s) %f " % (size, avg*10**3, num_op*(size/2**20)/elasped_time)) 
    print("=" * 50)
