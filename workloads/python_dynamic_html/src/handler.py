import datetime
import os
import sys
import random
from heapq import merge
from multiprocessing import Process, Pipe
from threading import Thread
from queue import Queue

from jinja2 import Template
import couchdb
from monitor import monitor_peak

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))
interval = 0.02
file_name = "template.html"

def download_stream_from_couchdb(db, doc_id, filename):
    return db.get_attachment(doc_id, filename).read().decode("utf-8") 

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

def generate_html(per_size, template_str, username, childConn, clientId):
    
    def random_int_list(start, stop, length):
        start, stop = (int(start), int(stop)) if start <= stop else (int(stop), int(start))
        length = int(abs(length)) if length else 0
        random_list = []
        for i in range(length):
            random_list.append(random.randint(start, stop))

        return random_list

    def merge_sort(unsorted_list):
        if len(unsorted_list) <= 1:
            return unsorted_list          
        middle = len(unsorted_list) // 2
        left = merge_sort(unsorted_list[:middle])    
        right = merge_sort(unsorted_list[middle:])
        return list(merge(left, right))

    random_numbers = merge_sort(random_int_list(0, per_size, per_size))
    template = Template(template_str)
    cur_time = datetime.datetime.now()
    html = template.render(username=username, cur_time=cur_time, random_numbers=random_numbers)

    childConn.send("{}: {}".format(username, html))
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

    username = event.get('username')
    size = event.get('size')
    parallel = event.get('parallel')
    couch_link = event.get('couch_link')
    db_name = event.get('db_name')

    couch = couchdb.Server(couch_link)
    db = couch[db_name]

    template_str = download_stream_from_couchdb(db, file_name, file_name)
    
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
            p = Process(target=generate_html, args=(tail_size, template_str, username+"_{}".format(i), childConn, i))
        else:
            p = Process(target=generate_html, args=(per_size, template_str, username+"_{}".format(i), childConn, i))
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


if __name__ == "__main__":
    event = {}
    event["username"] = "hanfeiyu"
    event["size"] = 7
    event["parallel"] = 8
    event["couch_link"] = "http://whisk_admin:some_passw0rd@130.39.92.101:5984"
    event["db_name"] = "dh"

    print(handler(event))
