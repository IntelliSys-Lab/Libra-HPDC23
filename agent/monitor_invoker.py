import socket
import time
import redis
import psutil

import params
from run_command import run_cmd


# Connect to redis pool
def get_redis_client(
    redis_host, 
    redis_port, 
    redis_password
):
    pool = redis.ConnectionPool(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)
    redis_client = redis.Redis(connection_pool=pool)

    return redis_client

# Get invoker name
def get_invoker_name():
    invoker_hostname = socket.gethostname()
    invoker_ip = socket.gethostbyname(invoker_hostname)
    invoker_name = run_cmd('cat ../ansible/environments/distributed/hosts | grep "invoker" | grep "{}" | awk {}'.format("={}$".format(invoker_ip), "{'print $1'}"))

    return invoker_name

# Get invoker dict
def get_invoker_dict():
    invoker_dict = {}

    # CPU and memory utilization
    # invoker_dict["memory_util"] = psutil.virtual_memory().percent
    # invoker_dict["cpu_util"] = psutil.cpu_percent()

    # Calculate MWS load
    invoker_dict["mws_load"] = params.MWS_CPU_WEIGHT * psutil.cpu_percent()/100 + (1 - params.MWS_CPU_WEIGHT) * psutil.virtual_memory().percent/100

    return invoker_dict

# Monitoring
def monitor(interval):
    redis_client = get_redis_client(params.REDIS_HOST, params.REDIS_PORT, params.REDIS_PASSWORD)

    while True:
        # psutil collection interval
        time.sleep(interval) 
        print("Write ") 
        print(get_invoker_dict())
        print(" to ")
        print(get_invoker_name())
        # Write into Redis
        redis_client.hmset(get_invoker_name(), get_invoker_dict())


if __name__ == "__main__":
    # Start monitoring
    monitor(params.INVOKER_MONITOR_INTERVAL)
