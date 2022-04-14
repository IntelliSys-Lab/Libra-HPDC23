import pandas as pd
import csv
import heapq
import itertools
import copy as cp
from utils import Function, Profile, EventPQ
from params import FunctionParameters, EventPQParameters, \
    COUCH_LINK, INVOCATION_FILE, DURATION_FILE, INPUT_SIZE_FILE, \
    TRACE_FILE_SUFFIX, CPU_CAP_PER_FUNCTION, MEMORY_CAP_PER_FUNCTION


class WorkloadGenerator():
    """
    Generate workfloads
    """
    def __init__(
        self,
        user_config,
        exp_id,
        azure_file_path,
        trace_file_prefix,
    ):
        self.azure_file_path = azure_file_path
        self.trace_file_prefix = trace_file_prefix
        self.user_config = user_config
        self.exp_id = exp_id

    def generate_event_pq_params(self):
        invocation_traces = pd.read_csv(self.azure_file_path + self.trace_file_prefix + "_" + INVOCATION_FILE + "_" + str(self.exp_id) + TRACE_FILE_SUFFIX)
        duration_traces = pd.read_csv(self.azure_file_path + self.trace_file_prefix + "_" + DURATION_FILE + "_" + str(self.exp_id) + TRACE_FILE_SUFFIX)
        event_pq_params = EventPQParameters(
            azure_invocation_traces=invocation_traces,
            azure_duration_traces=duration_traces
        )
        return event_pq_params

    def generate_profile(self):
        function_profile = {}
        for function_id in self.user_config.keys():
            param = FunctionParameters(
                function_id=function_id,
                invoke_params=self.user_config[function_id]["param"],
                cpu_user_defined=self.user_config[function_id]["cpu"],
                memory_user_defined=self.user_config[function_id]["memory"],
                cpu_cap_per_function=CPU_CAP_PER_FUNCTION,
                memory_cap_per_function=MEMORY_CAP_PER_FUNCTION,
                cpu_clip=self.user_config[function_id]["cpu_clip"],
                memory_clip=self.user_config[function_id]["memory_clip"]
            )

            # To test functions: -p couch_link http://whisk_admin:some_passw0rd@96.125.114.191:5984/ -p db_name eg
            function = Function(param)

            # Add to profile
            function_profile[function_id] = function
        
        profile = Profile(function_profile=function_profile)

        return profile
    
    def generate_event_pq(self):
        event_pq_params = self.generate_event_pq_params()
        invocation_traces = event_pq_params.azure_invocation_traces
        max_timestep = len(invocation_traces.columns) - 2
        input_size_dict = {}

        # Read input sizes
        with open(self.azure_file_path + self.trace_file_prefix + "_" + INPUT_SIZE_FILE + "_" + str(self.exp_id) + TRACE_FILE_SUFFIX, mode='r') as f:
            reader = csv.reader(f)
            input_size_dict = [int(x) for x in list(reader)[1]]
                
        # Create discrete invocation events
        pq = []
        counter = itertools.count()
        for timestep in range(max_timestep):
            for _, row in invocation_traces.iterrows():
                function_id = row["HashFunction"]
                invoke_num = row["{}".format(timestep+1)]
                for i in range(invoke_num):
                    t = timestep * 60 + int(60/invoke_num)*i
                    index = next(counter)
                    heapq.heappush(pq, (t, index, (function_id, input_size_dict[index])))
        event_pq = EventPQ(pq=pq, max_timestep=max_timestep)
        print("Incoming invocation events: {}".format(pq))
        return event_pq
    