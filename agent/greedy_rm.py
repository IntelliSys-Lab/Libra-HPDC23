from env import Environment
from logger import Logger
from utils import log_trends, log_function_throughput, log_per_function, export_csv_per_invocation, export_csv_percentile, export_csv_trajectory
from params import WorkloadParameters, EnvParameters
import params


#
# Greedy
#

def greedy_rm(
    logger_wrapper
):
    rm = "GreedyRM"

    # Set up logger
    logger = logger_wrapper.get_logger(rm, True)

    # Start training
    episode = 0
    for exp_id in params.EXP_EVAL:
        # Set paramters for workloads
        workload_params = WorkloadParameters(
            azure_file_path=params.AZURE_FILE_PATH,
            trace_file_prefix=params.TRACE_FILE_PREFIX,
            user_defined_dict=params.USER_DEFINED_DICT,
            exp_id=exp_id
        )

        # Set paramters for Environment
        env_params = EnvParameters(
            n_invoker=params.N_INVOKER,
            redis_host=params.REDIS_HOST,
            redis_port=params.REDIS_PORT,
            redis_password=params.REDIS_PASSWORD,
            couch_link=params.COUCH_LINK,
            cool_down=params.COOL_DOWN,
            interval_limit=params.ENV_INTERVAL_LIMIT,
            update_retry_time=params.UPDATE_RETRY_TIME,
            cpu_cap_per_function=params.CPU_CAP_PER_FUNCTION,
            memory_cap_per_function=params.MEMORY_CAP_PER_FUNCTION,
            memory_unit=params.MEMORY_UNIT
        )
        
        # Set up environment
        env = Environment(
            workload_params=workload_params,
            env_params=env_params
        )

        # Set up records
        reward_trend = []

        for episode_per_exp in range(params.MAX_EPISODE_EVAL):
            # Record total number of events
            total_events = env.event_pq.get_total_size()

            # Reset env
            # env.cool_down_openwhisk()
            logger = logger_wrapper.get_logger(rm, False)
            observation, mask, is_safeguard, current_timestep, current_function_id, current_input_size = env.reset()

            actual_time = 0
            system_time = 0
            system_runtime = 0
            reward_sum = 0

            function_throughput_list = []

            action = {}

            episode_done = False
            while episode_done is False:
                actual_time = actual_time + 1
                next_observation, next_mask, next_is_safeguard, reward, done, info, next_timestep, next_function_id, next_input_size = env.step(
                    current_timestep=current_timestep,
                    current_function_id=current_function_id,
                    current_input_size=current_input_size,
                    is_safeguard=is_safeguard,
                    action=action
                )
                next_is_safeguard = False


                if system_time < info["system_step"]:
                    system_time = info["system_step"]
                if system_runtime < info["system_runtime"]:
                    system_runtime = info["system_runtime"]

                function_throughput_list.append(info["function_throughput"])
                    
                logger.debug("")
                logger.debug("Actual timestep {}".format(actual_time))
                logger.debug("System timestep {}".format(system_time))
                logger.debug("System runtime {}".format(system_runtime))
                logger.debug("Take action: {}".format(action))
                logger.debug("Observation: {}".format(observation))
                logger.debug("Reward: {}".format(reward))
                
                reward_sum = reward_sum + reward

                if done is False:
                    request_record = info["request_record"]
                    total_available_cpu = info["total_available_cpu"]
                    total_available_memory = info["total_available_memory"]

                    #
                    # Greedy 
                    #

                    step = 1
                    last_request = request_record.get_last_n_done_request_per_function(next_function_id, 1)

                    if len(last_request) == 0:
                        action = {}
                    else:
                        last_request = last_request[0]

                        # CPU
                        if last_request.get_cpu_peak() / last_request.get_cpu() <= 1: # Over-provisioned
                            action["cpu"] = max(int(last_request.get_cpu_peak()) + 1, 1)
                        else: # Under-provisioned
                            action["cpu"] = env.env_params.cpu_cap_per_function

                        # Memory
                        if last_request.get_mem_peak() / last_request.get_memory() <= 1: # Over-provisioned
                            action["memory"] = max(int(last_request.get_mem_peak()), 1)
                        else: # Under-provisioned
                            action["memory"] = env.env_params.memory_cap_per_function
                else:
                    if system_time < info["system_step"]:
                        system_time = info["system_step"]
                    if system_runtime < info["system_runtime"]:
                        system_runtime = info["system_runtime"]

                    avg_slowdown = info["avg_slowdown"]
                    avg_harvest_cpu_percent = info["avg_harvest_cpu_percent"]
                    avg_harvest_memory_percent = info["avg_harvest_memory_percent"]
                    slo_violation_percent = info["slo_violation_percent"]
                    acceleration_pecent = info["acceleration_pecent"]
                    timeout_num = info["timeout_num"]
                    error_num = info["error_num"]
                    
                    logger.info("")
                    logger.info("**********")
                    logger.info("**********")
                    logger.info("**********")
                    logger.info("")
                    logger.info("Running {}".format(rm))
                    logger.info("Exp {}, Episode {} finished".format(exp_id, episode))
                    logger.info("{} actual timesteps".format(actual_time))
                    logger.info("{} system timesteps".format(system_time))
                    logger.info("{} system runtime".format(system_runtime))
                    logger.info("Total events: {}".format(total_events))
                    logger.info("Total reward: {}".format(reward_sum))
                    logger.info("Avg slowdown: {}".format(avg_slowdown))
                    logger.info("Avg harvest CPU percent: {}".format(avg_harvest_cpu_percent))
                    logger.info("Avg harvest memory percent: {}".format(avg_harvest_memory_percent))
                    logger.info("SLO violation percent: {}".format(slo_violation_percent))
                    logger.info("Acceleration pecent: {}".format(acceleration_pecent))
                    logger.info("Timeout num: {}".format(timeout_num))
                    logger.info("Error num: {}".format(error_num))
                    
                    reward_trend.append(reward_sum)

                    request_record = info["request_record"]

                    # Log function throughput
                    log_function_throughput(
                        overwrite=False, 
                        rm_name=rm, 
                        exp_id=exp_id,
                        logger_wrapper=logger_wrapper,
                        episode=episode, 
                        function_throughput_list=function_throughput_list
                    )

                    # Export csv per invocation
                    csv_cpu_pos_rfet, csv_cpu_zero_rfet, csv_cpu_neg_rfet, csv_memory_pos_rfet, csv_memory_zero_rfet, csv_memory_neg_rfet = request_record.get_csv_per_invocation()
                    export_csv_per_invocation(
                        rm_name=rm,
                        exp_id=exp_id,
                        episode=episode,
                        csv_cpu_pos_rfet=csv_cpu_pos_rfet,
                        csv_cpu_zero_rfet=csv_cpu_zero_rfet,
                        csv_cpu_neg_rfet=csv_cpu_neg_rfet,
                        csv_memory_pos_rfet=csv_memory_pos_rfet,
                        csv_memory_zero_rfet=csv_memory_zero_rfet,
                        csv_memory_neg_rfet=csv_memory_neg_rfet
                    )

                    # Export csv percentile
                    export_csv_percentile(
                        rm_name=rm,
                        exp_id=exp_id,
                        episode=episode,
                        csv_percentile=request_record.get_csv_percentile()
                    )

                    # Export csv trajectory
                    export_csv_trajectory(
                        rm_name=rm,
                        exp_id=exp_id,
                        episode=episode,
                        csv_trajectory=request_record.get_csv_trajectory()
                    )

                    episode_done = True
                
                observation = next_observation
                mask = next_mask
                is_safeguard = next_is_safeguard
                current_timestep = next_timestep
                current_function_id = next_function_id
                current_input_size = next_input_size

            episode = episode + 1


if __name__ == "__main__":
    logger_wrapper = Logger()
    greedy_rm(logger_wrapper=logger_wrapper)
