import os
import sys
sys.path.append("../agent")

from logger import Logger
import params
from libra import libra


def run_demo():
    logger_wrapper = Logger(log_path=os.path.dirname(os.getcwd())+'/demo/logs/')
    for i in params.DEMO_TRACES:
        print("")
        print("Running demo trace {}...".format(i))
        print("")
        libra(
            logger_wrapper=logger_wrapper,
            trace_file_prefix="demo_{}".format(i)
        )

    print("")
    print("***************************")
    print("Demo experiments finished!")
    print("***************************")
    print("")

if __name__ == "__main__":
    run_demo()
