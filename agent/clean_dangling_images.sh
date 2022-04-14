#! /bin/bash

# Prune dangling images
sudo docker rmi $(sudo docker images -a --filter "dangling=true" -q)
sudo docker image prune --force

# Prune dangling volumes
docker volume ls -qf dangling=true | xargs -r docker volume rm
