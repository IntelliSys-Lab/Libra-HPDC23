import datetime
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))
import json
import time
import os
import random
from heapq import merge
from multiprocessing import Process, Pipe
from threading import Thread
from queue import Queue

from functools import partial
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

class MyLoss:
    def __init__(self, x, y):
        """
        y = w @ x
        """
        self.x = x
        self.y = y
        
    def value(self, w):
        return 0.5 * np.linalg.norm(self.y - w @ self.x)
    
    def gradient(self, w):
        return w @ self.x @ self.x.T - self.y @ self.x.T

def flatten(x):
    original_shape = x.shape
    return x.flatten(), partial(np.reshape, newshape=original_shape)

def unflatten_optimizer_step(step):
    """
    Wrap an optimizer step function that operates on flat 1D arrays
    with a version that handles trees of nested containers,
    i.e. (lists/tuples/dicts), with arrays/scalars at the leaves.
    """
    def _step(value_and_grad, x, itr, state=None, *args, **kwargs):
        _x, unflatten = flatten(x)
        
        def _value_and_grad(x):
            v, g = value_and_grad(unflatten(x))
            return v, flatten(g)[0]
        
        _next_x, _next_val, _next_g, _next_state = step(_value_and_grad, _x, itr, state=state, *args, **kwargs)
        return unflatten(_next_x), _next_val, _next_g, _next_state
    
    return _step

def _generic_sgd(method, loss, x0, callback=None, num_iters=200, state=None, history=False, **kwargs):
    """
    Generic stochastic gradient descent step.
    """
    step = dict(sgd=sgd_step, rmsprop=rmsprop_step, adam=adam_step)[method]
    
    def loss_value_and_grad(loss, x):
        return loss.value(x), loss.gradient(x)
    value_and_grad = partial(loss_value_and_grad, loss)
    
    # Initialize outputs
    x, losses, grads = x0, [], []
    for itr in range(num_iters):
        x, val, g, state = step(value_and_grad, x, itr, state, **kwargs)
        losses.append(val)
        grads.append(g)

    if history:
        return x, losses, grads
    else:
        return x

@unflatten_optimizer_step
def sgd_step(value_and_grad, x, itr, state=None, step_size=0.1, mass=0.9):
    # Stochastic gradient descent with momentum.
    velocity = state if state is not None else np.zeros(len(x))
    val, g = value_and_grad(x)
    velocity = mass * velocity - (1.0 - mass) * g
    x = x + step_size * velocity
    return x, val, g, velocity

@unflatten_optimizer_step
def rmsprop_step(value_and_grad, x, itr, state=None, step_size=0.1, gamma=0.9, eps=10**-8):
    # Root mean squared prop: See Adagrad paper for details.
    avg_sq_grad = np.ones(len(x)) if state is None else state
    val, g = value_and_grad(x)
    avg_sq_grad = avg_sq_grad * gamma + g**2 * (1 - gamma)
    x = x - (step_size * g) / (np.sqrt(avg_sq_grad) + eps)
    return x, val, g, avg_sq_grad

@unflatten_optimizer_step
def adam_step(value_and_grad, x, itr, state=None, step_size=0.001, b1=0.9, b2=0.999, eps=10**-8):
    """
    Adam as described in http://arxiv.org/pdf/1412.6980.pdf.
    It's basically RMSprop with momentum and some correction terms.
    """
    m, v = (np.zeros(len(x)), np.zeros(len(x))) if state is None else state
    val, g = value_and_grad(x)
    m = (1 - b1) * g      + b1 * m    # First  moment estimate.
    v = (1 - b2) * (g**2) + b2 * v    # Second moment estimate.
    mhat = m / (1 - b1**(itr + 1))    # Bias correction.
    vhat = v / (1 - b2**(itr + 1))
    x = x - (step_size * mhat) / (np.sqrt(vhat) + eps)
    return x, val, g, (m, v)

def gradient_descent(per_size, x_row, x_col, w_row, childConn, clientId):
    # Define optimizers
    sgd = partial(_generic_sgd, "sgd")
    rmsprop = partial(_generic_sgd, "rmsprop")
    adam = partial(_generic_sgd, "adam")

    for _ in range(per_size):
        # Create two random tensors and the loss
        x = np.random.random((x_row, x_col))
        w = np.random.random((w_row, x_row))
        y = w @ x

        y += np.random.randn(*y.shape) * 0.001
        loss = MyLoss(x,y)
        w0 = np.random.random(w.shape)

        # Collect history
        history_list = []
        _, history, _ = sgd(loss, w0,num_iters=1000, history=True,step_size=0.001, mass=0.3)
        history_list.append(history)
        _, history, _ = adam(loss, w0,num_iters=1000, history=True,step_size=0.1, b1=0.9, b2=0.999,)
        history_list.append(history)
        _, history, _ = rmsprop(loss, w0,num_iters=1000, history=True,step_size=0.01, gamma=0.9)
        history_list.append(history)

    childConn.send("Gradient descent {} finished!".format(clientId))
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

    x_row = event.get('x_row')
    x_col = event.get('x_col')
    w_row = event.get('w_row')
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
            p = Process(target=gradient_descent, args=(tail_size, x_row, x_col, w_row, childConn, i))
        else:
            p = Process(target=gradient_descent, args=(per_size, x_row, x_col, w_row, childConn, i))
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
    