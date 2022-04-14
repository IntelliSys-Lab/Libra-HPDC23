<!--
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
-->

# Libra-openwhisk

This repo contains a demo implementation of our SC 22 paper submission. Libra is built atop [Apache OpenWhisk](https://github.com/apache/openwhisk). We describe how to build and deploy Libra from scratch for this demo. We also provide a public AWS EC2 AMI for fast reproducing the demo experiment.

## Build From Scratch

### Hardware Prerequisite
- Operating systems and versions: Ubuntu 18.04 (last version of Ubuntu that supports Python2 as Python2 may be needed)
- Resource requirement
  - CPU: >= 8 cores
  - Memory: >= 15 GB
  - Disk: >= 30 GB
  - Network: no requirement since it's a single-node deployment

Equivalent AWS EC2 instance type: c4.2xlarge with 30 GB EBS storage under root volume

### Deployment and Run Demo
This demo hosts all Libra's components on a single node.   

**Instruction**

1. Download the github repo.
```
git clone https://github.com/IntelliSys-Lab/Libra-openwhisk
```
2. Specify available CPU cores and memory for executing function invocations. This can be defined in [Libra-openwhisk/ansible/group_vars/all](https://github.com/IntelliSys-Lab/Libra-openwhisk/blob/master/ansible/group_vars/all) by modifying [`__userCpu`](https://github.com/IntelliSys-Lab/Libra-openwhisk/blob/master/ansible/group_vars/all#L200) and [`__userMemory`](https://github.com/IntelliSys-Lab/Libra-openwhisk/blob/master/ansible/group_vars/all#L199). Default available CPU and memory are set to 8 cores and 8 GB.
3. Go to [`Libra-openwhisk/demo`](https://github.com/IntelliSys-Lab/Libra-openwhisk/tree/master/demo).
```
cd Libra-openwhisk/demo
```
4. Set up the environment. This could take quite a while due to building Docker images from scratch. The recommended shell to run `setup.sh` is Bash.
```
./setup.sh
```
5. Run Libra's demo. The demo experiment may take several minutes to complete.
```
python3 run_demo.py
```

### Workloads

We provide the codebase of [ten serverless applications](https://github.com/IntelliSys-Lab/Libra-openwhisk/tree/master/workloads) used in our evaluation. However, due to hardware limitations, we only provide a simple [demo invocation trace](https://github.com/IntelliSys-Lab/Libra-openwhisk/tree/master/demo/azurefunctions-dataset2019) for the demo experiment.

### Experimental Results and OpenWhisk Logs
- Experimental results are collected as CSV files under `Libra-openwhisk/demo/logs`, including CPU usage, memory usage, resource allocation details, and invocation trajectories. Note that `Libra-openwhisk/demo/logs` is not present in the initial repo. It will only be generated after running an experiment.
- OpenWhisk system logs can be found under `/var/tmp/wsklogs`.
- Each run of an experiment will output some metrics on the screen, including:
  - `Actual timesteps`: logic timestep maintained by Libra
  - `System timesteps`: logic timestep provided by Azure invocation trace
  - `System runtime`: actual wall clock time since the experiment starts
  - `Total events`: the total number of invocation events during the experiment
  - `Total rewards`: the sum of execution time for all invocations
  - `Timeout num`: the number of invocations that timed out
  - `Error num`: the number of invocations returned with errors (typically due to resource contention, this could occur with default available 8 CPU cores)

### Distributed Libra
The steps of deploying a distributed Libra are basically the same as deploying a distributed OpenWhisk cluster. For deploying a distributed Libra, please refer to the README of [Apache OpenWhisk](https://github.com/apache/openwhisk) and [Ansible](https://github.com/apache/openwhisk/tree/master/ansible). 

## Reproduce via AWS EC2 AMI

### Prerequisite
- [AWS EC2](https://aws.amazon.com/ec2/): Instance type must be **c4.2xlarge** with at least **30 GB EBS storage under root volume**
- [AWS EC2 AMI](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AMIs.html): **ami-0b5c53c5c909b8b6a**

### Run Demo
Since this AMI has preinstalled all dependencies and built all Docker images, you can directly launch the demo once your EC2 instance is up.

**Instruction**

1. Launch a c4.2xlarge instance with 30 GB EBS storage under root volume using AMI **ami-0b5c53c5c909b8b6a**. Our AMI can be found by searching the AMI ID: **EC2 Management Console** -> **Images/AMIs** -> **Public Images** -> **Search**.
2. Log into your EC2 instance and go to [`Libra-openwhisk/demo`](https://github.com/IntelliSys-Lab/Libra-openwhisk/tree/master/demo).
```
cd Libra-openwhisk/demo
```
3. Run Libra's demo. The demo experiment may take one or two minutes to complete.
```
python3 run_demo.py
```
