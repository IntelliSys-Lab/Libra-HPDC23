from params import EnvParameters
from workload_generator import WorkloadGenerator
from env import Environment
from logger import Logger
from fixed_rm import fixed_rm
from greedy_rm import greedy_rm
from ensure_rm import ensure_rm
from freyr_eval import freyr_eval


def launch():
    # Set up logger wrapper
    logger_wrapper = Logger()

    #
    # Start experiments
    #

    # fixed_rm(
    #     logger_wrapper=logger_wrapper
    # )

    greedy_rm(
        logger_wrapper=logger_wrapper
    )

    # ensure_rm(
    #     logger_wrapper=logger_wrapper
    # )

    # freyr_eval(
    #     logger_wrapper=logger_wrapper
    # )


if __name__ == "__main__":
    
    # Launch
    launch()
