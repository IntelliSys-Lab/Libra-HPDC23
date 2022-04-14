import time

clockTicksPerSecond  = 100
nanoSecondsPerSecond = 1e9
megabytesPerByte = 1e6


def monitor_peak(interval, queue_cpu, queue_mem):
    
    while True:
        # CPU percentage
        prev_cpu_usage = 0
        prev_system_usage = 0
        prev_cgroup_cpu_times = open('/sys/fs/cgroup/cpuacct/cpuacct.usage_percpu', 'r').readline().split()
        prev_system_cpu_times = open('/proc/stat', 'r').readline().split()

        for per_cpu in prev_cgroup_cpu_times:
            prev_cpu_usage = prev_cpu_usage + int(per_cpu)
        for i in range(1, 9):
            prev_system_usage = prev_system_usage + int(prev_system_cpu_times[i])

        time.sleep(interval) 
        
        after_cpu_usage = 0
        after_system_usage = 0
        after_cgroup_cpu_times = open('/sys/fs/cgroup/cpuacct/cpuacct.usage_percpu', 'r').readline().split()
        after_system_cpu_times = open('/proc/stat', 'r').readline().split()

        cpu_timestamp = time.time()

        for per_cpu in after_cgroup_cpu_times:
            after_cpu_usage = after_cpu_usage + int(per_cpu)
        for i in range(1, 9):
            after_system_usage = after_system_usage + int(after_system_cpu_times[i])

        delta_cpu_usage = after_cpu_usage - prev_cpu_usage
        delta_system_usage = (after_system_usage - prev_system_usage) * nanoSecondsPerSecond / clockTicksPerSecond
        online_cpus = max(len(prev_cgroup_cpu_times), len(after_cgroup_cpu_times))

        cpu_core_busy = delta_cpu_usage / delta_system_usage * online_cpus

        # Memory percentage
        mem_total = int(open('/sys/fs/cgroup/memory/memory.usage_in_bytes', 'r').read())
        mem_cache = int(open('/sys/fs/cgroup/memory/memory.stat', 'r').readlines()[-3].split()[-1])

        mem_timestamp = time.time()

        mem_mb_busy = (mem_total - mem_cache) / megabytesPerByte
        
        queue_cpu.put((cpu_timestamp, cpu_core_busy))
        queue_mem.put((mem_timestamp, mem_mb_busy))
        

if __name__ == "__main__":
    from queue import Queue
    monitor_peak(0.1, Queue(), Queue())