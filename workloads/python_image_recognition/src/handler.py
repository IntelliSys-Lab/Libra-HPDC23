import datetime
import io
import os
import sys
import json
import torch
from torchvision import transforms
from torchvision.models import resnet50
from threading import Thread
from queue import Queue

from PIL import Image
import couchdb
from monitor import monitor_peak

interval = 0.02

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))

image_name = "tesla.jpg"
model_name = "resnet50.pth"
dataset_name = "imagenet_class_index.json"
local_path = "/tmp/"

model = None


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

    # Download dataset
    download_from_couchdb(db, dataset_name, dataset_name, local_path)
    class_idx = json.load(open(os.path.join(local_path, dataset_name), 'r'))
    idx2label = [class_idx[str(k)][1] for k in range(len(class_idx))]
    
    # Download image
    download_from_couchdb(db, image_name, image_name, local_path)

    global model
    if not model:
        # Download model checkpoint
        download_from_couchdb(db, model_name, model_name, local_path)
        model = resnet50(pretrained=False)
        model.load_state_dict(torch.load(local_path + model_name))
        model.eval()
   
    process_begin = datetime.datetime.now()
    input_image = Image.open(local_path + image_name)
    preprocess = transforms.Compose([
        transforms.Resize(256),
        transforms.CenterCrop(224),
        transforms.ToTensor(),
        transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
    ])
    input_tensor = preprocess(input_image)
    input_batch = input_tensor.unsqueeze(0) # create a mini-batch as expected by the model 

    output = model(input_batch)
    _, index = torch.max(output, 1)
    # The output has unnormalized scores. To get probabilities, you can run a softmax on it.
    prob = torch.nn.functional.softmax(output[0], dim=0)
    _, indices = torch.sort(output, descending=True)
    ret = idx2label[index]
    results = "Prediction: index {}, class {}".format(index.item(), ret)

    upload_stream_to_couchdb(db, "result", results.encode("utf-8"), "result.txt")
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
    