import datetime
import os
import sys
import stat
import random
import subprocess
from multiprocessing import Process, Pipe
from threading import Thread
from queue import Queue

import ffmpeg
import couchdb
from monitor import monitor_peak

interval = 0.02

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))

image_name = "watermark.png"
video_name = "hi_chitanda_eru.mp4"
local_path = "/tmp/"


def download_from_couchdb(db, doc_id, filename, local_path):
    with open(local_path + filename, "wb") as out:
        out.write(db.get_attachment(doc_id, filename).read())

def upload_to_couchdb(db, doc_id, filename, local_path, content_type=None):
    try:
        db.put_attachment(
            doc=db[doc_id],
            content=open(local_path + filename, "rb"),
            filename=filename,
            content_type=content_type
        )
    except couchdb.http.ResourceConflict:
        pass

def call_ffmpeg(args):
    ret = subprocess.run([os.path.join("./", 'ffmpeg', 'ffmpeg'), '-y'] + args,
            #subprocess might inherit Lambda's input for some reason
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    # if ret.returncode != 0:
    #     print('Invocation of ffmpeg failed!')
    #     print('Out: ', ret.stdout.decode('utf-8'))
    #     raise RuntimeError()

# https://github.com/kkroening/ffmpeg-python
def to_video(db, per_size, duration, childConn, clientId):
    output = 'processed_{}_hi_chitanda_eru.mp4'.format(clientId)
    for i in range(per_size):
        call_ffmpeg([
            "-i", local_path + video_name,
            "-i", local_path + image_name,
            "-t", "{}".format(duration),
            "-filter_complex", "[0]trim=start_frame=0:end_frame=50[v0];\
            [0]trim=start_frame=100:end_frame=150[v1];[v0][v1]concat=n=2[v2];[1]hflip[v3];\
            [v2][v3]overlay=eof_action=repeat[v4];[v4]drawbox=50:50:120:120:red:t=5[v5]",
            "-map", "[v5]",
            local_path + output])

    if clientId == 0:
        upload_to_couchdb(db, "result", output, local_path)
        
    childConn.send("Video {} finished!".format(clientId))
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
    duration = event.get('duration')
    parallel = event.get('parallel')
    couch_link = event.get('couch_link')
    db_name = event.get('db_name')

    couch = couchdb.Server(couch_link)
    db = couch[db_name]

    # Download overlay image
    download_from_couchdb(db, image_name, image_name, local_path)
    # Download input video
    download_from_couchdb(db, video_name, video_name, local_path)

    # Restore executable permission
    ffmpeg_binary = os.path.join("./", 'ffmpeg', 'ffmpeg')
    st = os.stat(ffmpeg_binary)
    os.chmod(ffmpeg_binary, st.st_mode | stat.S_IEXEC)

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
            p = Process(target=to_video, args=(db, tail_size, duration, childConn, i))
        else:
            p = Process(target=to_video, args=(db, per_size, duration, childConn, i))
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
    