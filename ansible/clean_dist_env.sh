#! /bin/bash

ansible-playbook -i environments/distributed openwhisk.yml -e mode=clean
#ansible-playbook -i environments/<environment> prereq_build.yml -e mode=clean
#ansible-playbook -i environments/<environment> prereq.yml -e mode=clean
