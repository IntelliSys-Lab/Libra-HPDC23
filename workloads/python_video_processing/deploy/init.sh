#!/bin/bash

DIR="./build"
wget -q https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz -P ${DIR}
pushd ${DIR} > /dev/null
tar -xf ffmpeg-release-amd64-static.tar.xz
rm *.tar.xz
mv ffmpeg-* ffmpeg
popd > /dev/null
