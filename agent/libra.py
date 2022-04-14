import numpy as np
from env import Environment
from logger import Logger
from utils import export_csv_delta, export_csv_invoker_util, export_csv_trajectory, export_csv_usage
from params import WorkloadParameters, EnvParameters
from libra_agent import LibraAgent
import params
import time

#
# Libra's client
#

def libra(
    logger_wrapper,
    trace_file_prefix
):
    rm = "libra_{}".format(trace_file_prefix)
    
    # Set up logger
    logger = logger_wrapper.get_logger(rm, True)

    # Set up libra's agent
    agent = LibraAgent(
        user_config=params.USER_CONFIG,
        cpu_model_name=params.CPU_MODEL_NAME,
        memory_model_name=params.MEMORY_MODEL_NAME,
        duration_model_name=params.DURATION_MODEL_NAME
    )
    agent.load()

    # Start training
    episode = 0
    for exp_id in params.EXP_EVAL:
        # Set paramters for workloads
        workload_params = WorkloadParameters(
            azure_file_path=params.AZURE_FILE_PATH,
            trace_file_prefix=trace_file_prefix,
            user_config=params.USER_CONFIG,
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

        for episode_per_exp in range(params.MAX_EPISODE_EVAL):
            # Record total number of events
            total_events = env.event_pq.get_total_size()

            # Reset env
            # env.cool_down_openwhisk()
            logger = logger_wrapper.get_logger(rm, False)
            observation, mask, current_timestep, current_index, current_function_id, current_input_size = env.reset()

            actual_time = 0
            system_time = 0
            system_runtime = 0
            reward_sum = 0

            action = {}
            update_record = True

            episode_done = False
            while episode_done is False:
                actual_time = actual_time + 1

                # print("Before predict {} at {}".format(current_function_id, time.time()*1000))
                predict_cpu, predict_memory, predict_duration = agent.predict(current_function_id, current_input_size)
                # print("After predict {} at {}".format(current_function_id, time.time()*1000))
                action["cpu"] = int(predict_cpu)
                action["memory"] = int(predict_memory)
                action["duration"] = predict_duration

                next_observation, next_mask, reward, done, info, next_timestep, next_index, next_function_id, next_input_size = env.step(
                    current_timestep=current_timestep,
                    current_index=current_index,
                    current_function_id=current_function_id,
                    current_input_size=current_input_size,
                    action=action,
                    update_record=update_record
                )

                if system_time < info["system_step"]:
                    system_time = info["system_step"]
                if system_runtime < info["system_runtime"]:
                    system_runtime = info["system_runtime"]

                logger.debug("")
                logger.debug("Actual timestep {}".format(actual_time))
                logger.debug("System timestep {}".format(system_time))
                logger.debug("System runtime {}".format(system_runtime))
                logger.debug("Take action: {}".format(action))
                logger.debug("Observation: {}".format(observation))
                logger.debug("Reward: {}".format(reward))
                
                if update_record is True:
                    reward_sum = reward_sum + reward
                
                if done:
                    if system_time < info["system_step"]:
                        system_time = info["system_step"]
                    if system_runtime < info["system_runtime"]:
                        system_runtime = info["system_runtime"]

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
                    logger.info("Timeout num: {}".format(timeout_num))
                    logger.info("Error num: {}".format(error_num))

                    # Export csv files
                    export_csv_trajectory(
                        rm_name=rm,
                        exp_id=exp_id,
                        episode=episode,
                        csv_trajectory=env.request_record.get_csv_trajectory()
                    )
                    export_csv_usage(
                        rm_name=rm,
                        exp_id=exp_id,
                        episode=episode,
                        csv_cpu_usage=env.request_record.get_csv_cpu_usage(env.system_time.get_system_up_time()),
                        csv_memory_usage=env.request_record.get_csv_mem_usage(env.system_time.get_system_up_time())
                    )
                    export_csv_delta(
                        rm_name=rm,
                        exp_id=exp_id,
                        episode=episode,
                        csv_delta=env.request_record.get_csv_delta()
                    )

                    episode_done = True
                
                observation = next_observation
                mask = next_mask
                current_timestep = next_timestep
                current_index = next_index
                current_function_id = next_function_id
                current_input_size = next_input_size

            episode = episode + 1


if __name__ == "__main__":
    logger_wrapper = Logger()

    #
    # Multi-node experiments
    #

    for i in params.MULTI_TRACES:
        print("Running multi trace {}...".format(i))
        libra(
            logger_wrapper=logger_wrapper,
            trace_file_prefix="multi_{}".format(i)
        )

    print("Multi experiments finished!")