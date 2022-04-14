import numpy as np
import torch

from env import Environment
from logger import Logger
from utils import export_csv_delta, export_csv_invoker_util, export_csv_trajectory, export_csv_usage, decode_action
from ppo2_agent import PPO2Agent
from params import WorkloadParameters, EnvParameters
import params


#
# Policy gradient training
#

def freyr_train(
    logger_wrapper
):
    rm = "freyr_train"

    # Set up logger
    logger = logger_wrapper.get_logger(rm, True)

    # Set up policy gradient agent
    agent = PPO2Agent(
        observation_dim=params.STATE_DIM,
        action_dim=params.ACTION_DIM,
        hidden_dims=params.HIDDEN_DIMS,
        learning_rate=params.LEARNING_RATE,
        discount_factor=params.DISCOUNT_FACTOR,
        ppo_clip=params.PPO_CLIP,
        ppo_epoch=params.PPO_EPOCH,
        value_loss_coef=params.VALUE_LOSS_COEF,
        entropy_coef=params.ENTROPY_COEF
    )
    
    # Record max sum rewards
    max_reward_sum = -1e8
    max_reward_sum_episode = 0

    # Start training
    episode = 0
    for exp_id in params.EXP_TRAIN:
        # Set paramters for workloads
        workload_params = WorkloadParameters(
            azure_file_path=params.AZURE_FILE_PATH,
            trace_file_prefix=params.TRACE_FILE_PREFIX,
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
        
        for episode_per_exp in range(params.MAX_EPISODE_TRAIN):
            # Record total number of events
            total_events = env.event_pq.get_total_size()

            # Reset env and agent
            env.cool_down_openwhisk()
            logger = logger_wrapper.get_logger(rm, False)
            observation, mask, current_timestep, current_index, current_function_id, current_input_size = env.reset()
            agent.reset()

            actual_time = 0
            system_time = 0
            system_runtime = 0
            reward_sum = 0

            observation_history = []
            mask_history = []
            action_history = []
            reward_history = []
            value_history = []
            log_prob_history = []

            action = {}
            update_record = True

            episode_done = False
            while episode_done is False:
                actual_time = actual_time + 1

                action_pred, _, value_pred, log_prob = agent.choose_action(observation, mask)
                action = decode_action(action_pred)
                action["cpu"] = int(action["cpu"])
                action["memory"] = int(action["memory"])
                action["duration"] = 0

                next_observation, next_mask, reward, done, info, next_timestep, next_index, next_function_id, next_input_size = env.step(
                    current_timestep=current_timestep,
                    current_index=current_index,
                    current_function_id=current_function_id,
                    current_input_size=current_input_size,
                    action=action,
                    update_record=update_record
                )

                # Detach tensors
                observation = observation.detach()
                mask = mask.detach()
                action_pred = action_pred.detach()
                value_pred = value_pred.detach()
                log_prob = log_prob.detach()
                
                # Record trajectories
                observation_history.append(observation)
                mask_history.append(mask)
                action_history.append(action_pred)
                reward_history.append(reward)
                value_history.append(value_pred)
                log_prob_history.append(log_prob)

                if system_time < info["system_step"]:
                    system_time = info["system_step"]
                if system_runtime < info["system_runtime"]:
                    system_runtime = info["system_runtime"]

                if update_record is True:
                    reward_sum = reward_sum + reward
                
                if done:
                    if system_time < info["system_step"]:
                        system_time = info["system_step"]
                    if system_runtime < info["system_runtime"]:
                        system_runtime = info["system_runtime"]
                        
                    timeout_num = info["timeout_num"]
                    error_num = info["error_num"]

                    # Concatenate trajectories
                    observation_history = torch.cat(observation_history, dim=0)
                    mask_history = torch.cat(mask_history, dim=0)
                    action_history = torch.cat(action_history, dim=0)
                    value_history = torch.cat(value_history).squeeze()
                    log_prob_history = torch.cat(log_prob_history, dim=0)

                    loss = agent.update(
                        observation_history=observation_history,
                        mask_history=mask_history,
                        action_history=action_history,
                        reward_history=reward_history,
                        value_history=value_history,
                        log_prob_history=log_prob_history
                    )

                    # Save max reward sum
                    if max_reward_sum < reward_sum:
                        max_reward_sum = reward_sum
                        max_reward_sum_episode = episode
                        agent.save(params.MODEL_SAVE_PATH + "max_reward_sum_{}.ckpt".format(params.MAX_EPISODE_TRAIN))
                        
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
                    logger.info("Loss: {}".format(loss))
                    logger.info("")
                    logger.info("Current Max Reward Sum: {}, observed at episode {}".format(max_reward_sum, max_reward_sum_episode))
                    
                    episode_done = True
                
                observation = next_observation
                mask = next_mask
                current_timestep = next_timestep
                current_index = next_index
                current_function_id = next_function_id
                current_input_size = next_input_size

            episode = episode + 1


if __name__ == "__main__":
    # Set up logger wrapper
    logger_wrapper = Logger()

    print("")
    print("**********")
    print("**********")
    print("**********")
    print("")

    # Start training
    print("Start training...")
    freyr_train(logger_wrapper=logger_wrapper)

    print("")
    print("Training finished!")
    print("")
    print("**********")
    print("**********")
    print("**********")
    