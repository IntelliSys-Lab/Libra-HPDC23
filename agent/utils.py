import time
import numpy as np
import copy as cp
import heapq
import couchdb
import redis
import csv

from params import WSK_CLI
import params
from run_command import run_cmd


#
# Encode and decode action
#

def decode_action(index):
    if index is not None:
        action = {}
        action["cpu"] = int(index / params.MEMORY_CAP_PER_FUNCTION) + 1
        action["memory"] = int(index % params.MEMORY_CAP_PER_FUNCTION) + 1
    else:
        action = None

    return action

def encode_action(cpu, memory):
    return (cpu - 1) * params.MEMORY_CAP_PER_FUNCTION + memory

#
# Class utilities
#

class Function():
    """
    Function wrapper
    """
    
    def __init__(self, params):
        self.params = params
        self.function_id = self.params.function_id

    def set_function(self, pool, action):
        redis_client = redis.Redis(connection_pool=pool)

        action["cpu"] = np.clip(int(action["cpu"]), self.params.cpu_clip[0], self.params.cpu_clip[1])
        action["memory"] = np.clip(int(action["memory"]), self.params.memory_clip[0], self.params.memory_clip[1])

        redis_client.hset("predict_peak", self.function_id, str(encode_action(action["cpu"], action["memory"])))
        redis_client.hset("predict_duration", self.function_id, str(int(action["duration"]*1000))) # sec to millisec
        
        self.cpu = action["cpu"]
        self.memory = action["memory"]
        self.predict_duration = action["duration"]

    def set_invoke_params(self, params):
        self.params.invoke_params = params

    def get_function_id(self):
        return self.function_id

    def get_cpu(self):
        return self.cpu

    def get_memory(self):
        return self.memory

    def get_predict_duration(self):
        return self.predict_duration
    
    def get_cpu_user_defined(self):
        return self.params.cpu_user_defined

    def get_memory_user_defined(self):
        return self.params.memory_user_defined

    def invoke_openwhisk(self, input_size):
        cmd = '{} action invoke {} {}'.format(WSK_CLI, self.function_id, self.params.invoke_params.format(input_size))
        result = str(run_cmd(cmd))
        request_id = result.split(" ")[-1]
        
        return request_id

    def update_openwhisk(self):
        cmd = '{} action update {} -m {}'.format(
            WSK_CLI, 
            self.function_id, 
            encode_action(self.get_cpu_user_defined(), self.get_memory_user_defined())
        )
        run_cmd(cmd)


class Request():
    """
    An invocation of a function
    """
    def __init__(
        self, 
        index,
        function_id, 
        request_id, 
        invoke_time,
        input_size,
        cpu_user_defined,
        memory_user_defined,
        cpu,
        memory,
        predict_duration
    ):
        self.index = index
        self.function_id = function_id
        self.request_id = request_id
        self.invoke_time = invoke_time
        self.input_size = input_size
        self.cpu_user_defined = cpu_user_defined
        self.memory_user_defined = memory_user_defined
        self.cpu = cpu
        self.memory = memory
        self.predict_duration = predict_duration

        self.is_done = False
        self.done_time = 0
        self.init_time = 0
        self.wait_time = 0
        self.duration = 0
        self.is_timeout = False
        self.is_success = False
        self.is_cold_start = False
        self.cpu_timestamp = []
        self.cpu_usage = []
        self.mem_timestamp = []
        self.mem_usage = []
        self.history = ""
        self.cpu_peak = 0
        self.mem_peak = 0
        self.is_safeguard = False
        self.memory_delta = 0
        self.cpu_delta = 0
        self.memory_idle = 0
        self.cpu_idle = 0
        self.budget = {}
        self.budget["memory"] = {}
        self.budget["cpu"] = {}

    def get_index(self):
        return self.index

    def get_function_id(self):
        return self.function_id

    def get_request_id(self):
        return self.request_id

    def get_input_size(self):
        return self.input_size

    def get_cpu_user_defined(self):
        return self.cpu_user_defined

    def get_memory_user_defined(self):
        return self.memory_user_defined

    def get_cpu(self):
        return self.cpu

    def get_memory(self):
        return self.memory

    def get_is_done(self):
        return self.is_done

    def get_invoke_time(self):
        return self.invoke_time

    def get_done_time(self):
        return self.done_time

    def get_predict_duration(self):
        return self.predict_duration

    def get_duration(self):
        return self.duration

    def get_completion_time(self):
        return (self.init_time + self.wait_time + self.duration)

    def get_is_timeout(self):
        return self.is_timeout

    def get_is_success(self):
        return self.is_success
        
    def get_is_cold_start(self):
        return self.is_cold_start

    def get_cpu_peak(self):
        return self.cpu_peak

    def get_mem_peak(self):
        return self.mem_peak

    def get_history(self):
        return self.history

    def get_is_safeguard(self):
        return self.is_safeguard

    def get_memory_delta(self):
        return self.memory_delta
    
    def get_cpu_delta(self):
        return self.cpu_delta
    
    def get_memory_idle(self):
        return self.memory_idle
    
    def get_cpu_idle(self):
        return self.cpu_idle

    def get_budget(self):
        return self.budget
    
    # Multiprocessing
    def try_update(self, result_dict, system_runtime, couch_link, pool):
        # Update couchdb
        couch_client = couchdb.Server(couch_link)
        couch_activations = couch_client["whisk_distributed_activations"]
        doc = couch_activations.get("guest/{}".format(self.request_id))
        result = result_dict[self.function_id][self.request_id]

        if doc is not None: 
            try:
                result["is_done"] = True
                result["done_time"] = system_runtime
                result["wait_time"] = doc["annotations"][1]["value"] / 1000
                result["duration"] = doc["duration"] / 1000
                if doc["response"]["statusCode"] == 0:
                    result["is_success"] = True
                    result["cpu_timestamp"] = [float(x) for x in doc["response"]["result"]["cpu_timestamp"]]
                    result["cpu_usage"] = [float(x) for x in doc["response"]["result"]["cpu_usage"]]
                    result["mem_timestamp"] = [float(x) for x in doc["response"]["result"]["mem_timestamp"]]
                    result["mem_usage"] = [float(x) for x in doc["response"]["result"]["mem_usage"]]
                else:
                    result["is_success"] = False
                    result["cpu_timestamp"] = []
                    result["cpu_usage"] = []
                    result["mem_timestamp"] = []
                    result["mem_usage"] = []

                result["is_timeout"] = doc["annotations"][3]["value"]
                
                if len(doc["annotations"]) == 6:
                    result["is_cold_start"] = True
                    result["init_time"] = doc["annotations"][5]["value"] / 1000
                else:
                    result["is_cold_start"] = False
                    result["init_time"] = 0 
            except Exception as e:
                # print("Exception {} occurred when processing request {}".format(e, self.request_id))
                result["is_done"] = False
        else:
            result["is_done"] = False

        # Update redis
        redis_client = redis.Redis(connection_pool=pool)
        result["history"] = redis_client.hget("invocations", self.request_id)

    def set_updates(
        self, 
        is_done,
        done_time,
        is_timeout,
        is_success,
        init_time,
        wait_time,
        duration,
        is_cold_start,
        cpu_timestamp,
        cpu_usage,
        mem_timestamp,
        mem_usage,
        history
    ):
        self.is_done = is_done
        self.done_time = done_time
        self.is_timeout = is_timeout
        self.is_success = is_success
        self.init_time = init_time
        self.wait_time = wait_time
        self.duration = duration
        self.is_cold_start = is_cold_start
        self.cpu_timestamp = cpu_timestamp
        self.cpu_usage = cpu_usage
        self.mem_timestamp = mem_timestamp
        self.mem_usage = mem_usage
        self.history = history

        if self.history is not None:
            parts = self.history.split(";")
            self.is_safeguard = False if int(parts[0]) == 0 else True
            # memory_delta = int(parts[1].split(" ")[0])
            # cpu_delta = int(parts[1].split(" ")[1])
            self.memory_idle = float(parts[2].split(" ")[0])/1000
            self.cpu_idle = float(parts[2].split(" ")[1])/1000

            memory_traj = parts[3]
            if memory_traj != "":
                pairs = memory_traj.split(" ")
                for p in pairs:
                    self.budget["memory"][p.split(":")[0]] = int(p.split(":")[1])
            cpu_traj = parts[4]
            if cpu_traj != "":
                pairs = cpu_traj.split(" ")
                for p in pairs:
                    self.budget["cpu"][p.split(":")[0]] = int(p.split(":")[1])
        else:
            self.memory_delta = (self.get_memory() - self.get_memory_user_defined()) * self.get_duration()
            self.cpu_delta = (self.get_cpu() - self.get_cpu_user_defined()) * self.get_duration()
            self.memory_idle = (self.get_memory_user_defined() - self.get_memory()) * self.get_duration() if self.get_memory() < self.get_memory_user_defined() else 0
            self.cpu_idle = (self.get_cpu_user_defined() - self.get_cpu()) * self.get_duration() if self.get_cpu() < self.get_cpu_user_defined() else 0

        if len(self.cpu_usage) > 0:
            self.cpu_peak = max(self.cpu_usage)
        if len(self.mem_usage) > 0:
            self.mem_peak = max(self.mem_usage)


class RequestRecord():
    """
    Recording of requests both in total and per Function
    """

    def __init__(self, function_profile):
        # General records
        self.total_request_record = []
        self.success_request_record = []
        self.undone_request_record = []
        self.timeout_request_record = []
        self.error_request_record = []

        # Records hashed by function id
        self.total_request_record_per_function = {}
        self.success_request_record_per_function = {}
        self.undone_request_record_per_function = {}
        self.timeout_request_record_per_function = {}
        self.error_request_record_per_function = {}

        for function_id in function_profile.keys():
            self.total_request_record_per_function[function_id] = []
            self.success_request_record_per_function[function_id] = []
            self.undone_request_record_per_function[function_id] = []
            self.timeout_request_record_per_function[function_id] = []
            self.error_request_record_per_function[function_id] = []

        # Records hashed by request id
        self.request_record_per_request = {}

    def put_requests(self, request):
        function_id = request.get_function_id()
        request_id = request.get_request_id()
        self.total_request_record.append(request)
        self.total_request_record_per_function[function_id].append(request)
        self.undone_request_record.append(request)
        self.undone_request_record_per_function[function_id].append(request)
        self.request_record_per_request[request_id] = request

    def update_requests(self, done_request_list):
        for request in done_request_list:
            function_id = request.get_function_id()

            if request.get_is_success() is True: # Success?
                self.success_request_record.append(request)
                self.success_request_record_per_function[function_id].append(request)
            else: # Not success
                if request.get_is_timeout() is True: # Timeout?
                    self.timeout_request_record.append(request)
                    self.timeout_request_record_per_function[function_id].append(request)
                else: # Error
                    self.error_request_record.append(request)
                    self.error_request_record_per_function[function_id].append(request)

            self.undone_request_record.remove(request)
            self.undone_request_record_per_function[function_id].remove(request)

    def label_all_undone_error(self, done_time):
        done_request_list = []
        for request in self.undone_request_record:
            request.set_updates(
                is_done=True,
                done_time=done_time,
                is_timeout=False,
                is_success=False,
                init_time=60,
                wait_time=60,
                duration=60,
                is_cold_start=True,
                cpu_timestamp=[],
                cpu_usage=[],
                mem_timestamp=[],
                mem_usage=[],
                history=None
            )
            done_request_list.append(request)

        self.update_requests(done_request_list)

    def update_all_delta(self):
        for request in self.total_request_record:
            budget = request.get_budget()

            for src_id in budget["memory"].keys():
                src_request = self.request_record_per_request[src_id]
                lasting = np.minimum(request.get_duration(), src_request.get_duration())
                request.memory_delta = request.memory_delta + budget["memory"][src_id] * lasting

            for src_id in budget["cpu"].keys():
                src_request = self.request_record_per_request[src_id]
                lasting = np.minimum(request.get_duration(), src_request.get_duration())
                request.cpu_delta = request.cpu_delta + budget["cpu"][src_id] * lasting

    def get_couch_key_list(self):
        key_list = []
        for request in self.undone_request_record:
            key_list.append("guest/{}".format(request.get_request_id()))

        return key_list

    def get_last_n_done_request_per_function(self, function_id, n):
        last_n_request = []

        for request in reversed(self.total_request_record_per_function[function_id]):
            if request.get_is_done() is True:
                last_n_request.append(request)
            
            if len(last_n_request) == n:
                break

        return last_n_request

    def get_total_size(self):
        return len(self.total_request_record)

    def get_undone_size(self):
        return len(self.undone_request_record)

    def get_success_size(self):
        return len(self.success_request_record)

    def get_timeout_size(self):
        return len(self.timeout_request_record)
        
    def get_error_size(self):
        return len(self.error_request_record)

    def get_avg_completion_time(self):
        request_num = 0
        total_completion_time = 0

        for request in self.success_request_record:
            request_num = request_num + 1
            total_completion_time = total_completion_time + request.get_completion_time()
        # for request in self.timeout_request_record:
        #     request_num = request_num + 1
        #     total_completion_time = total_completion_time + request.get_completion_time()
        # for request in self.error_request_record:
        #     request_num = request_num + 1
        #     total_completion_time = total_completion_time + request.get_completion_time()
        
        if request_num == 0:
            avg_completion_time = 0
        else:
            avg_completion_time = total_completion_time / request_num

        return avg_completion_time

    def get_avg_interval(self):
        total_interval = 0
        num = 0
        for i, request in enumerate(self.total_request_record):
            if i < len(self.total_request_record) - 1:
                next_request = self.total_request_record[i+1]
                interval = next_request.get_invoke_time() - request.get_invoke_time()

                if interval > 0:
                    total_interval = total_interval + interval
                    num = num + 1

        if num == 0:
            avg_interval = 0
        else:
            avg_interval = total_interval / num

        return avg_interval

    def get_csv_trajectory(self):
        csv_trajectory = []
        header = ["index", "request_id", "function_id", "input_size", "success", "timeout", \
            "cpu", "memory", "cpu_peak", "mem_peak", "history", "predict_duration", "duration", "completion_time"]
        csv_trajectory.append(header)
        
        for request in self.total_request_record:
            row = [
                request.get_index(),
                request.get_request_id(),
                request.get_function_id(),
                request.get_input_size(),
                request.get_is_success(),
                request.get_is_timeout(),
                request.get_cpu(),
                request.get_memory(),
                request.get_cpu_peak(),
                request.get_mem_peak(),
                request.get_history(),
                request.get_predict_duration(),
                request.get_duration(),
                request.get_completion_time()
            ]
            csv_trajectory.append(row)
        
        return csv_trajectory

    def get_csv_delta(self):
        csv_delta = []
        header = ["index", "request_id", "is_safeguard", "cpu_delta", "mem_delta", "cpu_idle", "memory_idle", "duration", "completion_time"]
        csv_delta.append(header)
        
        for request in self.total_request_record:
            row = [
                request.get_index(),
                request.get_request_id(),
                request.get_is_safeguard(),
                request.get_cpu_delta(),
                request.get_memory_delta(),
                request.get_cpu_idle(),
                request.get_memory_idle(),
                request.get_duration(),
                request.get_completion_time()
            ]
            csv_delta.append(row)
        
        return csv_delta

    def get_csv_cpu_usage(self, system_up_time):
        csv_usage = []
        header = ["time", "util", "alloc"]
        
        # Find the start and end time
        start_time = 1e16
        end_time = 0
        for request in self.total_request_record:
            if request.get_is_success() is True:
                if start_time > request.cpu_timestamp[0]:
                    start_time = request.cpu_timestamp[0]
                if end_time < request.cpu_timestamp[-1]:
                    end_time = request.cpu_timestamp[-1]
            # else:
            #     if end_time < request.get_invoke_time() + system_up_time + request.get_duration():
            #         end_time = request.get_invoke_time() + system_up_time + request.get_duration()
        
        time_range = int(end_time - start_time) + 1
        usage = [(0, 0) for _ in range(time_range)]

        # Calculate usage
        for request in self.total_request_record:
            usage_list = [([], []) for _ in range(time_range)]

            if request.get_is_success() is True:
                for (timestamp, util) in zip(request.cpu_timestamp, request.cpu_usage):
                    index = int(timestamp - start_time)
                    if index < len(usage_list):
                        (util_list, alloc_list) = usage_list[index]
                        util_list.append(np.clip(util, 1, request.get_cpu()))
                        alloc_list.append(request.get_cpu())

                for t, (total_util, total_alloc) in enumerate(usage):
                    (util_list, alloc_list) = usage_list[t]
                    if len(util_list) > 0:
                        util_avg = np.mean(util_list)
                    else:
                        util_avg = 0
                    if len(alloc_list) > 0:
                        alloc_avg = np.mean(alloc_list) 
                    else:
                        alloc_avg = 0
                    usage[t] = (np.clip(total_util + util_avg, 1, 8), np.clip(total_alloc + alloc_avg, 1, 8))
            # else:
            #     base = request.get_invoke_time() + system_up_time
            #     if base < start_time:
            #         base = start_time

            #     for i in range(int(request.get_duration())):
            #         index = int(i + base - start_time)
            #         if index < len(usage_list):
            #             (util_list, alloc_list) = usage_list[index]
            #             util_list.append(0.5)
            #             alloc_list.append(request.get_cpu())

            #     for t, (total_util, total_alloc) in enumerate(usage):
            #         (util_list, alloc_list) = usage_list[t]
            #         if len(util_list) > 0:
            #             util_avg = np.mean(util_list)
            #         else:
            #             util_avg = 0
            #         if len(alloc_list) > 0:
            #             alloc_avg = np.mean(alloc_list)
            #         else:
            #             alloc_avg = 0
            #         usage[t] = (total_util + util_avg, total_alloc + alloc_avg)

        # Generate csv
        csv_usage.append(header)
        for i, (util, alloc) in enumerate(usage):
            csv_usage.append([i + 1, util, alloc])

        return csv_usage

    def get_csv_mem_usage(self, system_up_time):
        csv_usage = []
        header = ["time", "util", "alloc"]
        
        # Find the start and end time
        start_time = 1e16
        end_time = 0
        for request in self.total_request_record:
            if request.get_is_success() is True:
                if start_time > request.cpu_timestamp[0]:
                    start_time = request.cpu_timestamp[0]
                if end_time < request.cpu_timestamp[-1]:
                    end_time = request.cpu_timestamp[-1]
            # else:
            #     if end_time < request.get_invoke_time() + system_up_time + request.get_duration():
            #         end_time = request.get_invoke_time() + system_up_time + request.get_duration()
        
        time_range = int(end_time - start_time) + 1
        usage = [(0, 0) for _ in range(time_range)]

        # Calculate usage
        for request in self.total_request_record:
            usage_list = [([], []) for _ in range(time_range)]

            if request.get_is_success() is True:
                for (timestamp, util) in zip(request.mem_timestamp, request.mem_usage):
                    index = int(timestamp - start_time)
                    if index < len(usage_list):
                        (util_list, alloc_list) = usage_list[index]
                        util_list.append(np.clip(util, 1, request.get_memory()*params.MEMORY_UNIT))
                        alloc_list.append(request.get_memory())

                for t, (total_util, total_alloc) in enumerate(usage):
                    (util_list, alloc_list) = usage_list[t]
                    if len(util_list) > 0:
                        util_avg =np.mean(util_list)
                    else:
                        util_avg = 0
                    if len(alloc_list) > 0:
                        alloc_avg = np.mean(alloc_list)
                    else:
                        alloc_avg = 0
                    usage[t] = (np.clip(total_util + util_avg, 1, 4096), np.clip(total_alloc + alloc_avg*params.MEMORY_UNIT, 1, 4096))
            # else:
            #     base = request.get_invoke_time() + system_up_time
            #     if base < start_time:
            #         base = start_time
                    
            #     for i in range(int(request.get_duration())):
            #         index = int(i + base - start_time)
            #         if index < len(usage_list):
            #             (util_list, alloc_list) = usage_list[index]
            #             util_list.append(64)
            #             alloc_list.append(request.get_memory())

            #     for t, (total_util, total_alloc) in enumerate(usage):
            #         (util_list, alloc_list) = usage_list[t]
            #         if len(util_list) > 0:
            #             util_avg = np.mean(util_list)
            #         else:
            #             util_avg = 0
            #         if len(alloc_list) > 0:
            #             alloc_avg = np.mean(alloc_list)
            #         else:
            #             alloc_avg = 0
            #         usage[t] = (total_util + util_avg, total_alloc + alloc_avg*params.MEMORY_UNIT)

        # Generate csv
        csv_usage.append(header)
        for i, (util, alloc) in enumerate(usage):
            csv_usage.append([i + 1, util, alloc])

        return csv_usage

    def get_total_size_per_function(self, function_id):
        return len(self.total_request_record_per_function[function_id])

    def get_undone_size_per_function(self, function_id):
        return len(self.undone_request_record_per_function[function_id])

    def get_success_size_per_function(self, function_id):
        return len(self.success_request_record_per_function[function_id])

    def get_timeout_size_per_function(self, function_id):
        return len(self.timeout_request_record_per_function[function_id])

    def get_avg_completion_time_per_function(self, function_id):
        request_num = 0
        total_completion_time = 0

        for request in self.success_request_record_per_function[function_id]:
            request_num = request_num + 1
            total_completion_time = total_completion_time + request.get_completion_time()
        for request in self.timeout_request_record_per_function[function_id]:
            request_num = request_num + 1
            total_completion_time = total_completion_time + request.get_completion_time()
        # for request in self.error_request_record_per_function[function_id]:
        #     request_num = request_num + 1
        #     total_completion_time = total_completion_time + request.get_completion_time()
        
        if request_num == 0:
            avg_completion_time_per_function = 0
        else:
            avg_completion_time_per_function = total_completion_time / request_num

        return avg_completion_time_per_function

    def get_avg_init_time_per_function(self, function_id):
        request_num = 0
        total_init_time = 0

        for request in self.success_request_record_per_function[function_id]:
            request_num = request_num + 1
            total_init_time = total_init_time + request.get_init_time()
        for request in self.timeout_request_record_per_function[function_id]:
            request_num = request_num + 1
            total_init_time = total_init_time + request.get_init_time()
        # for request in self.error_request_record_per_function[function_id]:
        #     request_num = request_num + 1
        #     total_init_time = total_init_time + request.get_init_time()
        
        if request_num == 0:
            avg_init_time_per_function = 0
        else:
            avg_init_time_per_function = total_init_time / request_num

        return avg_init_time_per_function

    def get_avg_interval_per_function(self, function_id):
        total_interval = 0
        num = 0
        for i, request in enumerate(self.total_request_record_per_function[function_id]):
            if i < len(self.total_request_record_per_function[function_id]) - 1:
                next_request = self.total_request_record_per_function[function_id][i+1]
                interval = next_request.get_invoke_time() - request.get_invoke_time()

                if interval > 0:
                    total_interval = total_interval + interval
                    num = num + 1

        if num == 0:
            avg_interval_per_function = 0
        else:
            avg_interval_per_function = total_interval / num

        return avg_interval_per_function

    def get_cold_start_num_per_function(self, function_id):
        cold_start_num_per_function = 0

        for request in self.success_request_record_per_function[function_id]:
            if request.get_is_cold_start() is True:
                cold_start_num_per_function = cold_start_num_per_function + 1
        for request in self.timeout_request_record_per_function[function_id]:
            if request.get_is_cold_start() is True:
                cold_start_num_per_function = cold_start_num_per_function + 1
        # for request in self.error_request_record_per_function[function_id]:
        #     if request.get_is_cold_start() is True:
        #         cold_start_num_per_function = cold_start_num_per_function + 1
        
        return cold_start_num_per_function

    def get_avg_invoke_num_per_function(self, function_id, system_runtime):
        avg_invoke_num_per_function = 0
        if system_runtime > 0:
            avg_invoke_num_per_function = len(self.total_request_record_per_function[function_id]) / system_runtime

        return avg_invoke_num_per_function

    def get_is_cold_start_per_function(self, function_id):
        if self.get_total_size_per_function(function_id) == 0:
            is_cold_start = True
        else:
            is_cold_start = self.total_request_record_per_function[function_id][-1].get_is_cold_start()

        if is_cold_start is False:
            return 0
        else:
            return 1

    # Only focus on latest recalibration
    def get_avg_duration_per_function(self, function_id):
        request_num = 0
        total_duration = 0

        for request in reversed(self.success_request_record_per_function[function_id]):
            request_num = request_num + 1
            total_duration = total_duration + request.get_duration()

            if request.get_baseline() == 0:
                break
        
        if request_num == 0:
            avg_duration_per_function = 0
        else:
            avg_duration_per_function = total_duration / request_num

        return avg_duration_per_function

    # Only focus on latest recalibration
    def get_avg_cpu_peak_per_function(self, function_id):
        request_num = 0
        total_cpu_peak = 0

        for request in reversed(self.success_request_record_per_function[function_id]):
            request_num = request_num + 1
            total_cpu_peak = total_cpu_peak + request.get_cpu_peak()

            if request.get_baseline() == 0:
                break

        if request_num == 0:
            avg_cpu_peak_per_function = 0
        else:
            avg_cpu_peak_per_function = total_cpu_peak / request_num

        return avg_cpu_peak_per_function

    # Only focus on latest recalibration
    def get_avg_memory_peak_per_function(self, function_id):
        request_num = 0
        total_mem_peak = 0

        for request in reversed(self.success_request_record_per_function[function_id]):
            request_num = request_num + 1
            total_mem_peak = total_mem_peak + request.get_mem_peak()

            if request.get_baseline() == 0:
                break

        if request_num == 0:
            avg_mem_peak_per_function = 0
        else:
            avg_mem_peak_per_function = total_mem_peak / request_num

        return avg_mem_peak_per_function

    def get_total_request_record(self):
        return self.total_request_record

    def get_success_request_record(self):
        return self.success_request_record

    def get_undone_request_record(self):
        return self.undone_request_record

    def get_timeout_request_record(self):
        return self.timeout_request_record

    def get_error_request_record(self):
        return self.error_request_record

    def get_total_request_record_per_function(self, function_id):
        return self.total_request_record_per_function[function_id]

    def get_success_request_record_per_function(self, function_id):
        return self.success_request_record_per_function[function_id]

    def get_undone_request_record_per_function(self, function_id):
        return self.undone_request_record_per_function[function_id]

    def get_timeout_request_record_per_function(self, function_id):
        return self.timeout_request_record_per_function[function_id]

    def get_error_request_record_per_function(self, function_id):
        return self.error_request_record_per_function[function_id]

    def get_request_per_request(self, request_id):
        return self.request_record_per_request[request_id]

    def reset(self):
        self.total_request_record = []
        self.success_request_record = []
        self.undone_request_record = []
        self.timeout_request_record = []
        self.error_request_record = []

        for function_id in self.total_request_record_per_function.keys():
            self.total_request_record_per_function[function_id] = []
            self.success_request_record_per_function[function_id] = []
            self.undone_request_record_per_function[function_id] = []
            self.timeout_request_record_per_function[function_id] = []
            self.error_request_record_per_function[function_id] = []

        self.request_record_per_request = {}


class InvokerUtilRecord():
    """
    Recording of CPU and memory utilizations per invoker
    """

    def __init__(self, n_invoker):
        self.n_invoker = n_invoker

        self.record = {}
        self.record["timeline"] = []

        for i in range(self.n_invoker):
            invoker = "invoker{}".format(i)
            self.record[invoker] = {}
            self.record[invoker]["cpu_util"] = []
            self.record[invoker]["memory_util"] = []

    def put_resource_utils(self, system_runtime, invoker, cpu_util, memory_util):
        if system_runtime not in self.record["timeline"]:
            self.record["timeline"].append(system_runtime)

        self.record[invoker]["cpu_util"].append(cpu_util)
        self.record[invoker]["memory_util"].append(memory_util)
    
    def get_record(self):
        return self.record

    def get_csv_invoker_util(self):
        csv_invoker_util_cpu = []
        csv_invoker_util_memory = []
        header = ["invoker_id"] + self.record["timeline"]
        csv_invoker_util_cpu.append(header)
        csv_invoker_util_memory.append(header)
        
        for invoker_id in self.record.keys():
            invoker = self.record[invoker_id]
            if "timeline" not in invoker_id:
                csv_invoker_util_cpu.append([invoker_id] +  invoker["cpu_util"])
                csv_invoker_util_memory.append([invoker_id] + invoker["memory_util"])

        return csv_invoker_util_cpu, csv_invoker_util_memory

    def reset(self):
        self.record = {}
        self.record["timeline"] = []
        
        for i in range(self.n_invoker):
            invoker = "invoker{}".format(i)
            self.record[invoker] = {}
            self.record[invoker]["cpu_util"] = []
            self.record[invoker]["memory_util"] = []


class Profile():
    """
    Record settings of functions
    """
    
    def __init__(self, function_profile):
        self.function_profile = function_profile
        self.default_function_profile = cp.deepcopy(function_profile)

    def put_function(self, function):
        function_id = function.get_function_id()
        self.function_profile[function_id] = function
    
    def get_size(self):
        return len(self.function_profile)

    def get_function_profile(self):
        return self.function_profile

    def reset(self):
        self.function_profile = cp.deepcopy(self.default_function_profile)
        
        
class EventPQ():
    """
    A priority queue of events, dictates which and when function will be invoked
    """
    
    def __init__(
        self, 
        pq,
        max_timestep
    ):
        self.pq = pq
        self.default_pq = cp.deepcopy(pq)
        self.max_timestep = max_timestep
        
    def get_event(self):
        if self.is_empty() is True:
            return None, (None, None)
        else:
            (timestep, counter, (function_id, input_size)) = heapq.heappop(self.pq)
            return timestep, counter, function_id, input_size
    
    def get_current_size(self):
        return len(self.pq)

    def get_total_size(self):
        return len(self.default_pq)

    def get_pq(self):
        return self.pq

    def get_max_timestep(self):
        return self.max_timestep

    def is_empty(self):
        if len(self.pq) == 0:
            return True
        else:
            return False

    def reset(self):
        self.pq = cp.deepcopy(self.default_pq)
    

class SystemTime():
    """
    Time module wrapper
    """
    
    def __init__(self, interval_limit=1):
        self.system_up_time = time.time()
        self.system_runtime = 0
        self.system_step = 0

        self.interval_limit = interval_limit

    def get_system_up_time(self):
        return self.system_up_time

    def get_system_runtime(self):
        return self.system_runtime

    def get_system_step(self):
        return self.system_step

    def update_runtime(self, increment):
        old_runtime = self.system_runtime
        target_runtime = self.system_runtime + increment
        while self.system_runtime <= target_runtime:  
            self.system_runtime = time.time() - self.system_up_time
        return self.system_runtime - old_runtime

    def step(self, increment):
        self.system_step = self.system_step + increment
        return self.update_runtime(increment)

    def reset(self):
        self.system_up_time = time.time()
        self.system_runtime = 0
        self.system_step = 0

#
# Log utilities
#

def export_csv_trajectory(
    rm_name,
    exp_id,
    episode,
    csv_trajectory
):
    file_path = "logs/"
    file_name = "{}_{}_{}_trajectory.csv".format(rm_name, exp_id, episode)

    with open(file_path + file_name, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(csv_trajectory)

def export_csv_delta(
    rm_name,
    exp_id,
    episode,
    csv_delta
):
    file_path = "logs/"
    file_name = "{}_{}_{}_delta.csv".format(rm_name, exp_id, episode)

    with open(file_path + file_name, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(csv_delta)

def export_csv_usage(
    rm_name,
    exp_id,
    episode,
    csv_cpu_usage,
    csv_memory_usage
):
    file_path = "logs/"

    with open(file_path + "{}_{}_{}_cpu_usage.csv".format(rm_name, exp_id, episode), "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(csv_cpu_usage)

    with open(file_path + "{}_{}_{}_memory_usage.csv".format(rm_name, exp_id, episode), "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(csv_memory_usage)

def export_csv_invoker_util(
    rm_name,
    exp_id,
    episode,
    csv_invoker_util_cpu,
    csv_invoker_util_memory
):
    file_path = "logs/"

    with open(file_path + "{}_{}_{}_invoker_util_cpu.csv".format(rm_name, exp_id, episode), "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(csv_invoker_util_cpu)

    with open(file_path + "{}_{}_{}_invoker_util_memory.csv".format(rm_name, exp_id, episode), "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(csv_invoker_util_memory)
