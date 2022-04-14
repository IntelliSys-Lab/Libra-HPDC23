import datetime
import io
import os
import sys
from multiprocessing import Process, Pipe
from threading import Thread
from queue import Queue

from PIL import Image
import couchdb
from monitor import monitor_peak

interval = 0.02

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))

filename = "the_screamer_yui.png"
local_path = "/tmp/"


def download_from_couchdb(db, doc_id, filename, local_path):
    with open(local_path + filename, "wb") as out:
        out.write(db.get_attachment(doc_id, filename).read())

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

# Memory-based solution
def resize_image(db, per_size, w, h, childConn, clientId):
    out = io.StringIO("default")
    for i in range(per_size):
        with Image.open(open(local_path + filename, "rb")) as image:
            image.thumbnail((w,h))
            out = io.BytesIO()
            image.save(out, format='png')
            # necessary to rewind to the beginning of the buffer
            out.seek(0)

    if clientId == 0:
        upload_stream_to_couchdb(db, "result", out, "resized_{}_".format(clientId) + filename)
        
    childConn.send("Image {} processed!".format(clientId))
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
    width = event.get('width')
    height = event.get('height')
    parallel = event.get('parallel')
    couch_link = event.get('couch_link')
    db_name = event.get('db_name')

    couch = couchdb.Server(couch_link)
    db = couch[db_name]
    download_from_couchdb(db, filename, filename, local_path)
    
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
            p = Process(target=resize_image, args=(db, tail_size, width, height, childConn, i))
        else:
            p = Process(target=resize_image, args=(db, per_size, width, height, childConn, i))
        jobs.append(p)
    for p in jobs:
        p.start()
    for p in jobs:
        p.join()
    
    results = []
    for con in parentConns:
        results.append(con.recv())

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
    