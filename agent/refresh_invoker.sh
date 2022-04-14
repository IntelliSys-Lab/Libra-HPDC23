#! /bin/bash

cd ../ansible 
sudo ansible-playbook -i environments/distributed invoker.yml
cd ../agent
