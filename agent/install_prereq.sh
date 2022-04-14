#! /bin/bash

#
# Install dependencies
#

sudo apt install -y python3-pip
pip3 install pandas torch sklearn couchdb redis docker

#
# Create ckpt, figures, logs and azure trace folders if not exist
#

folders="./ckpt ./figures ./logs ./azurefunctions-dataset2019"

for folder in $folders
do
    if [ ! -d "$folder" ]
    then
        mkdir $folder
    # else
    #     rm $folder/*
    fi
done

#
# Download Azure Functions traces
#

cd ./azurefunctions-dataset2019
wget "https://azurecloudpublicdataset2.blob.core.windows.net/azurepublicdatasetv2/azurefunctions_dataset2019/azurefunctions-dataset2019.tar.xz"
tar -xvf azurefunctions-dataset2019.tar.xz
cd ..
