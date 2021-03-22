#!/usr/bin/python3

import boto3
import json
import random
import time
import numpy as np
import pickle
import os


# endpoint and keys from vstart
endpoint = 'http://172.31.4.82:80'
#endpoint = 'http://127.0.0.1:8000'
access_key="14PQ0HEUG876CL989VMF"
secret_key="tY2vafwxBWjszKkAHvCu0szTihBoUZL7N5nflAUQ"

sizes = [ 4*2**10,] #16*2**10, 64*2**10, 256*2**10, 2**20, 4*2**20 ]

# input
obj_prefix = "test"
num_op = 10000

client = boto3.client("s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
        )


rand_op = [ random.randint(1,9999) for _ in range(num_op) ]

test_name = obj_prefix + " - read"
latencyResults = {}

for size in sizes:
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
    print("ObjectSize %d ,latency (ms): %f" % (size, (_sum/count)*10**3)) 
