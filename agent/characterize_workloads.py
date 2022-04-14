import time
import json
import numpy as np
import csv
from utils import run_cmd
from params import WSK_CLI, USER_CONFIG


def refresh_invoker():
    run_cmd("./refresh_invoker.sh")

def update(function_id, memory):
    cmd = '{} action update {} -m {}'.format(WSK_CLI, function_id, memory)
    run_cmd(cmd)

def invoke(function_id, param):
    try:
        cmd = '{} action invoke {} -b {} | grep -v "ok:"'.format(WSK_CLI, function_id, param)
        response = str(run_cmd(cmd))
        doc = json.loads(response)

        # Status
        is_success = doc["response"]["success"]
        is_timeout = doc["annotations"][3]["value"]

        cpu_peak = 0
        for usage in doc["response"]["result"]["cpu_usage"]:
            if cpu_peak < float(usage):
                cpu_peak = float(usage)

        mem_peak = 0
        for usage in doc["response"]["result"]["mem_usage"]:
            if mem_peak < float(usage):
                mem_peak = float(usage)

        # Times
        if len(doc["annotations"]) == 6:
            is_cold_start = True
            init_time = int(doc["annotations"][5]["value"]) / 1000
        else:
            is_cold_start = False
            init_time = 0
        wait_time = int(doc["annotations"][1]["value"]) / 1000
        duration = int(doc["duration"]) / 1000

    except Exception:
        is_success = False
        is_timeout = True
        is_cold_start = False
        init_time = 0
        wait_time = 0
        duration = 0
        cpu_peak = 0
        mem_peak = 0

    return is_success, is_timeout, is_cold_start, init_time, wait_time, duration, cpu_peak, mem_peak

def cold_start(interval, size_range, alloc_range, loop_time, function_id, param):
    result = []
    header = ["alloc", "size", "success", "timeout", "duration", "completion_time", "cpu_peak", "mem_peak"]
    result.append(header)

    cnt = 0
    for i in alloc_range:
        # Update resource allocation
        update(function_id, i)
        print("Update {} with alloc {}".format(function_id, i))

        for size in size_range:
            init_time_list = []
            wait_time_list = []
            duration_list = []
            completion_time_list = []
            cpu_peak_list = []
            mem_peak_list = []

            for loop in range(loop_time):
                is_success, is_timeout, is_cold_start, init_time, wait_time, duration, cpu_peak, mem_peak = invoke(function_id, param.format(size))
                completion_time = init_time + wait_time + duration
                if is_cold_start is True and completion_time > 0: # Only record cold invocations
                    init_time_list.append(init_time)
                    wait_time_list.append(wait_time)
                    duration_list.append(duration)
                    completion_time_list.append(completion_time)
                    cpu_peak_list.append(cpu_peak)
                    mem_peak_list.append(mem_peak)

                print("")
                print("Finish {} with size {} in loop {}".format(function_id, size, loop))
                print("")

                cnt = cnt + 1
                if cnt >= interval:
                    refresh_invoker()
                    cnt = 0
                    print("Refresh invoker")

            # Calculate average
            if len(duration_list) == 0:
                avg_duration = 0
            else:
                avg_duration = np.average(duration_list)

            if len(completion_time_list) == 0:
                avg_completion_time = 0
            else:
                avg_completion_time = np.average(completion_time_list)

            if len(cpu_peak_list) == 0:
                avg_cpu_peak = 0
            else:
                avg_cpu_peak = np.average(cpu_peak_list)

            if len(mem_peak_list) == 0:
                avg_mem_peak = 0
            else:
                avg_mem_peak = np.average(mem_peak_list)

            row = [
                i, 
                size,
                is_success,
                is_timeout,
                avg_duration,
                avg_completion_time,
                avg_cpu_peak,
                avg_mem_peak
            ]
            result.append(row)

    return result

def warm_start(interval, size_range, alloc_range, loop_time, function_id, param):
    result = []
    header = ["actual_id", "size", "success", "timeout", "duration", "cpu_peak", "mem_peak"]
    result.append(header)

    for i in alloc_range:
        actual_id = "{}_{}".format(function_id, i)

        for size in size_range:
            init_time_list = []
            wait_time_list  = []
            duration_list  = []
            completion_time_list  = []
            cpu_peak_list  = []
            mem_peak_list  = []

            for loop in range(loop_time):
                is_success, is_timeout, is_cold_start, init_time, wait_time, duration, cpu_peak, mem_peak = invoke(actual_id, param.format(size))
                completion_time = init_time + wait_time + duration
                if is_cold_start is False and completion_time > 0: # Only record warm invocations
                    init_time_list.append(init_time)
                    wait_time_list.append(wait_time)
                    duration_list.append(duration)
                    completion_time_list.append(completion_time)
                    cpu_peak_list.append(cpu_peak)
                    mem_peak_list.append(mem_peak)

            # Calculate average
            if len(duration_list) == 0:
                avg_duration = 0
            else:
                avg_duration = np.average(duration_list)

            if len(cpu_peak_list) == 0:
                avg_cpu_peak = 0
            else:
                avg_cpu_peak = np.average(cpu_peak_list)

            if len(mem_peak_list) == 0:
                avg_mem_peak = 0
            else:
                avg_mem_peak = np.average(mem_peak_list)

            row = [
                actual_id, 
                size,
                is_success,
                is_timeout,
                avg_duration,
                avg_cpu_peak,
                avg_mem_peak
            ]
            result.append(row)

            time.sleep(interval)

    return result

def characterize_functions(
    interval,
    loop_time,
    is_cold_start,
    file_path,
    function_invoke_params
):
    for function_id in function_invoke_params.keys():
        size_range = function_invoke_params[function_id]["size_range"]
        alloc_range = function_invoke_params[function_id]["alloc_range"]
        param = function_invoke_params[function_id]["param"]
        if is_cold_start is True:
            result = cold_start(interval, size_range, alloc_range, loop_time, function_id, param)
        else:
            result = warm_start(interval, size_range, alloc_range, loop_time, function_id, param)

        # Log results
        with open(file_path + "{}_usage.csv".format(function_id), "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(result)


if __name__ == "__main__":
    interval = 500
    # (action["cpu"] - 1) * 8 + action["memory"]
    # alloc_range = [8, 16, 24, 32, 40, 48, 56, 57, 58, 59, 60, 61, 62, 63, 64] 
    loop_time = 5
    is_cold_start = True
    file_path = "logs/"

    function_invoke_params = {}
    for function_id in USER_CONFIG.keys():
        function_invoke_params[function_id] = {}
        function_invoke_params[function_id]["alloc_range"] = [64]
        function_invoke_params[function_id]["size_range"] = [x * USER_CONFIG[function_id]["base"] for x in range(USER_CONFIG[function_id]["range"][0], USER_CONFIG[function_id]["range"][1])]
        function_invoke_params[function_id]["param"] = USER_CONFIG[function_id]["param"]
        
    characterize_functions(
        interval=interval,
        loop_time=loop_time,
        is_cold_start=is_cold_start,
        file_path=file_path,
        function_invoke_params=function_invoke_params
    )

    print("")
    print("Workload characterization finished!")
    print("")
