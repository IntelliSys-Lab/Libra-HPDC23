import datetime
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))
import json
import time
import os
import random
from multiprocessing import Process, Pipe
from threading import Thread
from queue import Queue

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

def alu(per_size, childConn, clientId):
    a = random.randint(100, 10000)
    b = random.randint(100, 10000)
    temp = 0
    for i in range(per_size):
        if i % 4 == 0:
            temp = a + b
        elif i % 4 == 1:
            temp = a - b
        elif i % 4 == 2:
            temp = a * b
        else:
            temp = a / b

    childConn.send("Calulation {} result {}".format(clientId, temp))
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
            p = Process(target=alu, args=(tail_size, childConn, i))
        else:
            p = Process(target=alu, args=(per_size, childConn, i))
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
    