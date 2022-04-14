from run_command import run_cmd

#
# Global variables
#

# Environment parameters
WSK_CLI = "wsk -i"
N_INVOKER = int(run_cmd('cat ../ansible/environments/distributed/hosts | grep "invoker" | grep -v "\[invokers\]" | wc -l'))
REDIS_HOST = run_cmd('cat ../ansible/environments/distributed/hosts | grep -A 1 "\[edge\]" | grep "ansible_host" | awk {}'.format("{'print $1'}"))
REDIS_PORT = 6379
REDIS_PASSWORD = "openwhisk"
COUCH_PROTOCOL = "http"
COUCH_USER = "whisk_admin"
COUCH_PASSWORD = "some_passw0rd"
COUCH_HOST = run_cmd('cat ../ansible/environments/distributed/hosts | grep -A 1 "\[db\]" | grep "ansible_host" | awk {}'.format("{'print $1'}"))
COUCH_PORT = "5984"
COUCH_LINK = "{}://{}:{}@{}:{}/".format(COUCH_PROTOCOL, COUCH_USER, COUCH_PASSWORD, COUCH_HOST, COUCH_PORT)
COOL_DOWN = "refresh_openwhisk"
ENV_INTERVAL_LIMIT = 1
INVOKER_MONITOR_INTERVAL = 0.1 # 100 ms
MWS_CPU_WEIGHT = 0.8 # MWS memory weight = 1 - MWS cpu weight 
UPDATE_RETRY_TIME = 100
CPU_CAP_PER_FUNCTION = 8
MEMORY_CAP_PER_FUNCTION = 8
MEMORY_UNIT = 128

# Training parameters
# AZURE_FILE_PATH = "azurefunctions-dataset2019/"
AZURE_FILE_PATH = "../demo/azurefunctions-dataset2019/"
TRACE_FILE_PREFIX = "multi_1"
# MULTI_TRACES = [1, 2, 3, 4, 5, 6, 12, 18, 24, 30]
DEMO_TRACES = [1]
INVOCATION_FILE = "invocation_traces"
DURATION_FILE = "duration_traces"
INPUT_SIZE_FILE = "input_size_traces"
TRACE_FILE_SUFFIX = ".csv"
MAX_EPISODE_TRAIN = 100
MAX_EPISODE_EVAL = 1
EXP_TRAIN = [0]
EXP_EVAL = [0]
CPU_MODEL_NAME = "clas_rand"
MEMORY_MODEL_NAME = "clas_rand"
DURATION_MODEL_NAME = "reg_rand"
USER_CONFIG = {
    "dh": {
        "cpu": 2,
        "memory": 8,
        "cpu_clip": [1, 2],
        "memory_clip": [2, 8],
        "base": 100,
        "range": [1, 101],
        "multi": 1000,
        "param": "-p username hanfeiyu -p size {} -p parallel 2 " +  "-p couch_link {} -p db_name dh".format(COUCH_LINK)
    },
    "eg": {
        "cpu": 2,
        "memory": 8,
        "cpu_clip": [1, 2],
        "memory_clip": [2, 8],
        "base": 2000,
        "range": [1, 101],
        "multi": 20000,
        "param": "-p size {} " + "-p couch_link {} -p db_name eg".format(COUCH_LINK)
    },
    "ip": {
        "cpu": 1,
        "memory": 2,
        "cpu_clip": [1, 2],
        "memory_clip": [2, 8],
        "base": 5,
        "range": [1, 101],
        "multi": 100,
        "param": "-p width 1000 -p height 1000 -p size {} -p parallel 8 " + "-p couch_link {} -p db_name ip".format(COUCH_LINK)
    },
    "vp": {
        "cpu": 2,
        "memory": 4,
        "cpu_clip": [1, 2],
        "memory_clip": [4, 8],
        "base": 1,
        "range": [1, 11],
        "multi": 1,
        "param": "-p duration 20 -p size {} -p parallel 1 " + "-p couch_link {} -p db_name vp".format(COUCH_LINK)
    },
    "ir": {
        "cpu": 1,
        "memory": 8,
        "cpu_clip": [1, 2],
        "memory_clip": [4, 8],
        "base": 10,
        "range": [1, 101],
        "multi": 100,
        "param": "-p size {} -p parallel 1 " + "-p couch_link {} -p db_name ir".format(COUCH_LINK)
    }, 
    "knn": {
        "cpu": 2,
        "memory": 8,
        "cpu_clip": [1, 2],
        "memory_clip": [4, 8],
        "base": 5,
        "range": [1, 101],
        "multi": 50,
        "param": "-p dataset_size 1000 -p feature_dim 800 -p k 3 -p size {} -p parallel 8 " + "-p couch_link {} -p db_name knn".format(COUCH_LINK)
    }, 
    "alu": {
        "cpu": 2,
        "memory": 8,
        "cpu_clip": [1, 2],
        "memory_clip": [1, 8],
        "base": 10000,
        "range": [1, 101],
        "multi": 100000,
        "param": "-p size {} -p parallel 2 " + "-p couch_link {} -p db_name alu".format(COUCH_LINK)
    }, 
    "ms": {
        "cpu": 2,
        "memory": 4,
        "cpu_clip": [1, 2],
        "memory_clip": [4, 8],
        "base": 5000,
        "range": [1, 101],
        "multi": 100000,
        "param": "-p size {} -p parallel 2 " + "-p couch_link {} -p db_name ms".format(COUCH_LINK)
    },
    "gd": {
        "cpu": 1,
        "memory": 8,
        "cpu_clip": [1, 2],
        "memory_clip": [4, 8],
        "base": 2,
        "range": [1, 101],
        "multi": 10,
        "param": "-p x_row 20 -p x_col 20 -p w_row 40 -p size {} -p parallel 8 " + "-p couch_link {} -p db_name gd".format(COUCH_LINK)
    },
    "dv": {
        "cpu": 1,
        "memory": 8,
        "cpu_clip": [1, 2],
        "memory_clip": [4, 8],
        "base": 100,
        "range": [1, 101],
        "multi": 500,
        "param": "-p size {} -p parallel 8 " + "-p couch_link {} -p db_name dv".format(COUCH_LINK)
    },
}
STATE_DIM = 8
ACTION_DIM = 1
HIDDEN_DIMS = [32, 16]
LEARNING_RATE = 1e-2
DISCOUNT_FACTOR = 1
PPO_CLIP = 0.2
PPO_EPOCH = 4
VALUE_LOSS_COEF = 0.5
ENTROPY_COEF = 0.01
# MODEL_SAVE_PATH = "ckpt/"
MODEL_SAVE_PATH = "../demo/ckpt/"
MODEL_NAME = "max_reward_sum_{}.ckpt".format(MAX_EPISODE_TRAIN)
        
#
# Parameter classes
#

class EnvParameters():
    """
    Parameters used for generating Environment
    """
    def __init__(
        self,
        n_invoker,
        redis_host,
        redis_port,
        redis_password,
        couch_link,
        cool_down,
        interval_limit,
        update_retry_time,
        cpu_cap_per_function,
        memory_cap_per_function,
        memory_unit
    ):
        self.n_invoker = n_invoker
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_password = redis_password
        self.couch_link = couch_link
        self.cool_down = cool_down
        self.interval_limit = interval_limit
        self.update_retry_time = update_retry_time
        self.cpu_cap_per_function = cpu_cap_per_function
        self.memory_cap_per_function = memory_cap_per_function
        self.memory_unit = memory_unit

class WorkloadParameters():
    """
    Parameters used for workload configuration
    """
    def __init__(
        self,
        azure_file_path,
        trace_file_prefix,
        user_config,
        exp_id
    ):
        self.azure_file_path = azure_file_path
        self.trace_file_prefix = trace_file_prefix
        self.user_config = user_config
        self.exp_id = exp_id

class FunctionParameters():
    """
    Parameters used for generating Function
    """
    def __init__(
        self,
        function_id,
        invoke_params,
        cpu_user_defined,
        memory_user_defined,
        cpu_cap_per_function,
        memory_cap_per_function,
        cpu_clip,
        memory_clip,
    ):
        self.function_id = function_id
        self.invoke_params = invoke_params
        self.cpu_user_defined = cpu_user_defined
        self.memory_user_defined = memory_user_defined
        self.cpu_cap_per_function = cpu_cap_per_function
        self.memory_cap_per_function = memory_cap_per_function
        self.cpu_clip = cpu_clip
        self.memory_clip = memory_clip

class EventPQParameters():
    """
    Parameters used for generating EventPQ
    """
    def __init__(
        self,
        azure_invocation_traces,
        azure_duration_traces
    ):
        self.azure_invocation_traces = azure_invocation_traces
        self.azure_duration_traces = azure_duration_traces
