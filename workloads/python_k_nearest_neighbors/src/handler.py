import datetime
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))
import json
import time
import random
from multiprocessing import Process, Pipe
import operator
from threading import Thread
from queue import Queue

import numpy as np
import couchdb
from monitor import monitor_peak

interval = 0.02


def upload_stream_to_couchdb(db, doc_id, content, filename, content_type=None):
    try:
        db.put_attachment(
            doc=db[doc_id],
            content=content,
            filename=filename,
            content_type=content_type
        )
    except couchdb.http.ResourceConflict:
        pass

def create_dataset(dataset_size, feature_dim):
    dataset = []
    labels = []
    for s in range(dataset_size):
        data = []
        for i in range(feature_dim):
            data.append(random.random())
        dataset.append(data)
        labels.append(random.randint(0, 1))

    dataset = np.array(dataset)
    return dataset, labels

def knn(per_size, dataset_size, feature_dim, k, childConn, clientId):
    maxIndex = -1
    for i in range(per_size):
        # Create dataset and labels
        dataset, labels = create_dataset(dataset_size, feature_dim)

        # Create test item
        newInput = []
        for i in range(feature_dim):
            newInput.append(random.random())

        # Predict
        numSamples = dataset.shape[0]   
        diff = np.tile(newInput, (numSamples, 1)) - dataset 
        squaredDiff = diff ** 2  
        squaredDist = np.sum(squaredDiff, axis = 1)   
        distance = squaredDist ** 0.5  

        sortedDistIndices = np.argsort(distance)
        classCount = {} 
        for i in range(k):
            voteLabel = labels[sortedDistIndices[i]]
            classCount[voteLabel] = classCount.get(voteLabel, 0) + 1

        maxCount = 0
        for key, value in classCount.items():
            if value > maxCount:
                maxCount = value
                maxIndex = key

    childConn.send("Prediction {}: max index {}".format(clientId, maxIndex))
    childConn.close()

def handler(event, context=None):
    q_cpu = Queue()
    q_mem = Queue()
    t = Thread(
        target=monitor_peak,
        args=(interval, q_cpu, q_mem),
        daemon=True
    )
    t.start()

    dataset_size = event.get('dataset_size')
    feature_dim = event.get('feature_dim')
    k = event.get('k')
    size = event.get('size')
    parallel = event.get('parallel')
    couch_link = event.get('couch_link')
    db_name = event.get('db_name')

    couch = couchdb.Server(couch_link)
    db = couch[db_name]

    process_begin = datetime.datetime.now()
    per_size = int(size/parallel)
    tail_size = size % parallel
    childConns = []
    parentConns = []
    jobs = []
    for i in range(parallel+1):
        parentConn, childConn = Pipe()
        parentConns.append(parentConn)
        childConns.append(childConn)
        if i == parallel:
            p = Process(target=knn, args=(tail_size, dataset_size, feature_dim, k, childConn, i))
        else:
            p = Process(target=knn, args=(per_size, dataset_size, feature_dim, k, childConn, i))
        jobs.append(p)
    for p in jobs:
        p.start()
    for p in jobs:
        p.join()
    
    results = []
    for con in parentConns:
        results.append(con.recv())

    upload_stream_to_couchdb(db, "result", '\n'.join(results).encode("utf-8"), "result.txt")
    process_end = datetime.datetime.now()
    process_time = (process_end - process_begin) / datetime.timedelta(microseconds=1)

    cpu_timestamp = []
    cpu_usage = []
    while q_cpu.empty() is False:
        (timestamp, cpu) = q_cpu.get()
        cpu_timestamp.append(timestamp)
        cpu_usage.append(cpu)

    mem_timestamp = []
    mem_usage = []
    while q_mem.empty() is False:
        (timestamp, mem) = q_mem.get()
        mem_timestamp.append(timestamp)
        mem_usage.append(mem)

    return {
        "cpu_timestamp": [str(x) for x in cpu_timestamp],
        "cpu_usage": [str(x) for x in cpu_usage],
        "mem_timestamp": [str(x) for x in mem_timestamp],
        "mem_usage": [str(x) for x in mem_usage]
    }
    