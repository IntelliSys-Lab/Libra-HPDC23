import time
import redis
import couchdb
import numpy as np
import torch
import multiprocessing

from utils import SystemTime, Request, RequestRecord, InvokerUtilRecord, encode_action, decode_action
from run_command import run_cmd
from params import WSK_CLI
from workload_generator import WorkloadGenerator


class Environment():
    """ 
    Environment for running experiments
    """

    def __init__(
        self,
        workload_params,
        env_params
    ):
        self.workload_params = workload_params
        self.env_params = env_params

        # Set up workloads
        self.workload_generator = WorkloadGenerator(
            azure_file_path=self.workload_params.azure_file_path,
            trace_file_prefix=self.workload_params.trace_file_prefix,
            user_config=self.workload_params.user_config,
            exp_id=self.workload_params.exp_id
        )
        self.profile = self.workload_generator.generate_profile()
        self.event_pq = self.workload_generator.generate_event_pq()

        # Get total number of invokers
        self.n_invoker = self.env_params.n_invoker
        
        # Set up Redis client
        self.pool = redis.ConnectionPool(
            host=self.env_params.redis_host, 
            port=self.env_params.redis_port, 
            password=self.env_params.redis_password, 
            decode_responses=True
        )
        self.redis_client = redis.Redis(connection_pool=self.pool)

        # Set up CouchDB client
        self.couch_client = couchdb.Server(self.env_params.couch_link)

        # Set up time module
        self.system_time = SystemTime(self.env_params.interval_limit)

        # Set up request record
        self.request_record = RequestRecord(self.profile.get_function_profile())

        # Set up resource utils record
        self.invoker_util_record = InvokerUtilRecord(self.n_invoker)

        # Misc
        self.cool_down = self.env_params.cool_down
        self.eps = np.finfo(np.float32).eps.item()

    #
    # Process action
    #
    
    def update_function_profile(self, next_function_id, action):
        function = self.profile.get_function_profile()[next_function_id]
        function.set_function(self.pool, action)

    def init_user_config(self):
        for function_id in self.profile.get_function_profile().keys():
            function = self.profile.get_function_profile()[function_id]
            action = {}
            action["cpu"] = function.get_cpu_user_defined()
            action["memory"] = function.get_memory_user_defined()
            action["duration"] = 0 # Default set to 0
            self.update_function_profile(function_id, action)
            function.update_openwhisk()

    #
    # Interactions with OpenWhisk
    #

    def invoke_openwhisk(
        self, 
        index,
        function_id,
        input_size,
    ):
        function = self.profile.get_function_profile()[function_id]
        # print("Invoke {} at {}".format(function_id, time.time()*1000))
        request_id = function.invoke_openwhisk(input_size)

        # Create corresponding request
        request = Request(
            index=index,
            function_id=function_id, 
            request_id=request_id, 
            invoke_time=self.system_time.get_system_runtime(),
            input_size=input_size,
            cpu_user_defined=function.get_cpu_user_defined(),
            memory_user_defined=function.get_memory_user_defined(),
            cpu=function.get_cpu(),
            memory=function.get_memory(),
            predict_duration=function.get_predict_duration()
        )
        self.request_record.put_requests(request)

    def try_update_request_record(self):
        function_profile = self.profile.get_function_profile()
        manager = multiprocessing.Manager()
        result_dict = manager.dict()
        jobs = []

        for function_id in function_profile.keys():
            result_dict[function_id] = manager.dict()
            for request in self.request_record.get_undone_request_record_per_function(function_id):
                request_id = request.get_request_id()
                result_dict[function_id][request_id] = manager.dict()
                result_dict[function_id][request_id]["is_done"] = False
                
                p = multiprocessing.Process(
                    target=request.try_update,
                    args=(
                        result_dict, 
                        self.system_time.get_system_runtime(), 
                        self.env_params.couch_link,
                        self.pool
                    )
                )
                jobs.append(p)
                p.start()
        
        for p in jobs:
            p.join()

        # Update requests according to the result dict
        # Return rewards for requests that completes at this timestep
        done_request_list = []
        total = 0

        for function_id in result_dict.keys():
            for request_id in result_dict[function_id].keys():
                request = self.request_record.request_record_per_request[request_id]
                result = result_dict[function_id][request_id]

                # Check if done
                is_done = result["is_done"] 
                if is_done is True:
                    done_time = result["done_time"]
                    init_time = result["init_time"]
                    wait_time = result["wait_time"]
                    duration = result["duration"]
                    is_timeout = result["is_timeout"]
                    is_success = result["is_success"]
                    is_cold_start = result["is_cold_start"]
                    cpu_timestamp = result["cpu_timestamp"]
                    cpu_usage = result["cpu_usage"]
                    mem_timestamp = result["mem_timestamp"]
                    mem_usage = result["mem_usage"]
                    history = result["history"]

                    # Set updates for done requests
                    request.set_updates(
                        is_done=is_done,
                        done_time=done_time,
                        is_timeout=is_timeout,
                        is_success=is_success,
                        init_time=init_time,
                        wait_time=wait_time,
                        duration=duration,
                        is_cold_start=is_cold_start,
                        cpu_timestamp=cpu_timestamp,
                        cpu_usage=cpu_usage,
                        mem_timestamp=mem_timestamp,
                        mem_usage=mem_usage,
                        history=history
                    )
                    done_request_list.append(request)

                    total = total + request.get_duration()
            
        # Update request records
        self.request_record.update_requests(done_request_list)

        return total

    def get_n_undone_request(self):
        return int(self.redis_client.get("n_undone_request"))

    def get_available_cpu(self):
        return int(self.redis_client.get("available_cpu"))

    def get_available_memory(self):
        return int(self.redis_client.get("available_memory"))

    def refresh_invoker(self):
        cmd = "./refresh_invoker.sh"
        run_cmd(cmd)

    def refresh_openwhisk(self):
        cmd = "./refresh_openwhisk.sh"
        run_cmd(cmd)

    def refresh_couchdb_and_openwhisk(self):
        cmd = "./refresh_couchdb_and_openwhisk.sh"
        run_cmd(cmd)

    def clean_invoker_containers(self):
        cmd = "./clean_invoker_containers.sh"
        run_cmd(cmd)

    def cool_down_openwhisk(self):
        if self.cool_down == "refresh_invoker":
            self.refresh_invoker()
        elif self.cool_down == "refresh_openwhisk":
            self.refresh_openwhisk()
        elif self.cool_down == "refresh_couchdb_and_openwhisk":
            self.refresh_couchdb_and_openwhisk()
        elif self.cool_down == "clean_invoker_containers":
            self.clean_invoker_containers()
        
        time.sleep(60)

    def reset_redis(self):
        for key in self.redis_client.scan_iter("prefix:*"):
            self.redis_client.delete(key)

    def query_invoker_state(self):
        p = self.redis_client.pipeline(transaction=False)
        invoker_dict = {}

        for i in range(self.n_invoker):
            invoker = "invoker{}".format(i)
            p.hgetall(invoker)

        p_result = p.execute()

        for i, metric_dict in enumerate(p_result):
            invoker = "invoker{}".format(i)
            invoker_dict[invoker] = {}

        # Resource utilizations
        invoker_dict[invoker]["cpu_util"] = float(metric_dict["cpu_util"])
        invoker_dict[invoker]["memory_util"] = float(metric_dict["memory_util"])

        return invoker_dict

    def record_invoker_util(self):
        # Query resource utilizations of each invoker
        invoker_dict = self.query_invoker_state()

        # Record current system runtime
        current_runtime = self.system_time.get_system_runtime()

        # Record resource utilizations for each invoker
        for invoker_id in invoker_dict.keys():
            invoker = invoker_dict[invoker_id]
            self.invoker_util_record.put_resource_utils(current_runtime, invoker_id, invoker["cpu_util"], invoker["memory_util"])
                                        
    def get_mask(
        self, 
        next_function_id,
        cpu_range,
        memory_range
    ):
        batch_size = self.env_params.cpu_cap_per_function * self.env_params.memory_cap_per_function
        mask = torch.zeros(batch_size)

        if len(cpu_range) == 1:
            for i in range(mask.size(0)):
                cpu = decode_action(i)["cpu"]
                if cpu != cpu_range[0]:
                    mask[i] = -1e8
        else:
            for i in range(mask.size(0)):
                cpu = decode_action(i)["cpu"]
                if cpu < cpu_range[0] or cpu > cpu_range[1]:
                    mask[i] = -1e8

        if len(memory_range) == 1:
            for i in range(mask.size(0)):
                mem = decode_action(i)["memory"]
                if mem != memory_range[0]:
                    mask[i] = -1e8
        else:
            for i in range(mask.size(0)):
                mem = decode_action(i)["memory"]
                if mem < memory_range[0] or mem > memory_range[1]:
                    mask[i] = -1e8

        return mask

    # Observation space size: [batch_size, 11]
    def get_observation(
        self, 
        next_function_id,
        next_input_size
    ):
        profile = self.profile.get_function_profile()
        function = profile[next_function_id]

        # Init observation
        function_index = list(profile.keys()).index(next_function_id)
        n_undone_request = self.get_n_undone_request()
        available_cpu = self.get_available_cpu()
        available_memory = self.get_available_memory()
        function_avg_interval = self.request_record.get_avg_interval_per_function(next_function_id)

        state_batch = []
        for cpu in range(1, self.env_params.cpu_cap_per_function + 1):
            for memory in range(1, self.env_params.memory_cap_per_function + 1):
                state = []
                state.append(function_index)
                state.append(n_undone_request)
                state.append(available_cpu)
                state.append(available_memory)
                state.append(function_avg_interval)
                state.append(next_input_size)
                state.append(cpu)
                state.append(memory)

                state_batch.append(state)

        observation = torch.Tensor(state_batch)

        #
        # Safeguard
        #

        # Init mask
        cpu_cap_per_function = self.env_params.cpu_cap_per_function
        memory_cap_per_function = self.env_params.memory_cap_per_function
        cpu_user_defined = function.get_cpu_user_defined()
        memory_user_defined = function.get_memory_user_defined()
        
        last_request = self.request_record.get_last_n_done_request_per_function(next_function_id, 1)
        threshold = 0.8

        if len(last_request) == 0:
            cpu_range = [cpu_user_defined]
            memory_range = [memory_user_defined]
        else:
            last_request = last_request[0]

            if last_request.get_is_success() is False:
                cpu_range = [cpu_user_defined]
                memory_range = [memory_user_defined]
            else:
                last_cpu_alloc = last_request.get_cpu()
                last_mem_alloc = last_request.get_memory()
                last_cpu_peak = last_request.get_cpu_peak()
                last_mem_peak = last_request.get_mem_peak()

                if last_cpu_peak < cpu_user_defined: # Over-provisioned
                    if last_cpu_peak / last_cpu_alloc >= threshold: # Usage spike
                        cpu_range = [cpu_user_defined]
                        # print("{}, last_cpu_peak {}, last_cpu_alloc {}, cpu safeguard activate".format(next_function_id, last_cpu_peak, last_cpu_alloc))
                    else:
                        cpu_range = [1, cpu_user_defined]
                else: # Under-provisioned
                    cpu_range = [cpu_user_defined, cpu_cap_per_function]
                        
                if last_mem_peak <= (memory_user_defined * self.env_params.memory_unit): # Over-provisioned
                    if last_mem_peak / (last_mem_alloc * self.env_params.memory_unit) >= threshold: # Usage spike
                        memory_range = [memory_user_defined]
                        # print("{}, last_mem_peak {}, last_mem_alloc {}, memory safeguard activate".format(next_function_id, last_mem_peak, last_mem_alloc * self.env_params.memory_unit))
                    else:
                        memory_range = [1, memory_user_defined]
                else: # Under-provisioned
                    memory_range = [memory_user_defined, memory_cap_per_function]

                # if len(cpu_range) > 1 and cpu_range[0] >= cpu_range[1]:
                #     print("last_cpu_peak: {}, last_cpu_alloc: {}, cpu_user_defined: {}".format(last_cpu_peak, last_cpu_alloc, cpu_user_defined))
                #     print("cpu_range: {} - {}".format(cpu_range[0], cpu_range[1]))
                # if len(memory_range) > 1 and memory_range[0] >= memory_range[1]:
                #     print("last_mem_peak: {}, last_mem_alloc: {}, memory_user_defined: {}".format(last_mem_peak / self.env_params.memory_unit, last_mem_alloc, memory_user_defined))
                #     print("memory_range: {} - {}".format(memory_range[0], memory_range[1]))

        mask = self.get_mask(
            next_function_id=next_function_id,
            cpu_range=cpu_range,
            memory_range=memory_range
        )

        observation = observation.unsqueeze(0)
        mask = mask.unsqueeze(0)

        # print("observation:")
        # print(observation)
        # print("mask:")
        # print(mask)

        return observation, mask

    def get_reward(
        self, 
        total,
    ):
        reward = -total
        return reward

    def get_done(self):
        if self.event_pq.is_empty() is True and \
        self.system_time.get_system_step() > self.event_pq.get_max_timestep() - 1 and \
        self.get_n_undone_request() == 0:
            return True
        else:
            return False

    def get_info(self, done):
        info = {
            "system_step": self.system_time.get_system_step(),
            "system_runtime": self.system_time.get_system_runtime(),
            "n_undone_request": self.get_n_undone_request(),
            "total_available_cpu": self.get_available_cpu(),
            "total_available_memory": self.get_available_memory(),
            "request_record": self.request_record,
        }

        if done is True:
            info["timeout_num"] = self.request_record.get_timeout_size()
            info["error_num"] = self.request_record.get_error_size()

        return info

    def step(
        self, 
        current_timestep,
        current_index,
        current_function_id,
        current_input_size,
        action,
        update_record
    ):
        # Update function configuration according to the action
        if action is not None:
            self.update_function_profile(current_function_id, action)

        # Get next event
        if self.event_pq.is_empty() is False:
            # Events proceed
            if self.system_time.get_system_step() < current_timestep:
                self.system_time.step(current_timestep - self.system_time.get_system_step())

            # Invoke functions according to the given function
            self.invoke_openwhisk(current_index, current_function_id, current_input_size)

            # Try update all inflight requests
            if update_record is True:
                total = self.try_update_request_record()

                # Get reward
                reward = self.get_reward(total)
            else:
                reward = None

            # Get next event
            next_timestep, next_index, next_function_id, next_input_size = self.event_pq.get_event()

            # Get observation for next state
            observation, mask = self.get_observation(next_function_id=next_function_id, next_input_size=next_input_size)

            # Update invoker utilization
            # self.record_invoker_util()
       
        else:
            # Invoke the last event
            self.invoke_openwhisk(current_index, current_function_id, current_input_size)

            # Proceed to the end
            if self.system_time.get_system_step() < self.event_pq.get_max_timestep():
                self.system_time.step(self.event_pq.get_max_timestep() -  self.system_time.get_system_step())
                # self.record_invoker_util()
                
            # Wait until no inflight requests
            while True:
                if self.get_done() is True:
                    break 
            
            # Retrieve tail rewards
            reward = 0

            retry = 0
            while retry < self.env_params.update_retry_time:
                if self.request_record.get_undone_size() == 0:
                    break

                # Try update all inflight requests
                total = self.try_update_request_record()

                # Get reward
                reward = reward + self.get_reward(total)

                retry = retry + 1

            # Force updating the rest
            self.request_record.label_all_undone_error(self.system_time.get_system_runtime())
            self.request_record.update_all_delta()

            observation = None
            mask = None
            next_timestep = None
            next_index = None
            next_function_id = None
            next_input_size = None

        # Done?
        done = self.get_done()

        # Return information
        info = self.get_info(done)

        return observation, mask, reward, done, info, next_timestep, next_index, next_function_id, next_input_size

    def reset(self):
        self.system_time.reset()
        self.profile.reset()
        self.event_pq.reset()
        self.request_record.reset()
        self.invoker_util_record.reset()
        self.reset_redis()
        self.init_user_config()
        
        next_timestep, next_index, next_function_id, next_input_size = self.event_pq.get_event()
        observation, mask = self.get_observation(next_function_id=next_function_id, next_input_size=next_input_size)
        
        return observation, mask, next_timestep, next_index, next_function_id, next_input_size
