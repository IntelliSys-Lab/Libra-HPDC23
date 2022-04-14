#! /bin/bash

echo ""
echo "Compiling functions..."
echo ""

#
# dynamic-html (dh)
#

cd python_dynamic_html/deploy

# Destroy and prepare build folder.
rm -rf build
mkdir build

# Install virtualenv
sudo docker run --rm -v "$PWD:/tmp" openwhisk/python3action bash \
  -c "cd tmp && virtualenv virtualenv && source virtualenv/bin/activate && pip3 install -r requirements.txt"

# Copy files to build folder.
cp -R ../src/* ./build
cp -R ../platforms/ibm/* ./build
cp -R virtualenv ./build
cd ./build
zip -X -r ./index.zip *

cd ../../../

#
# email-generation (eg)
#

cd python_email_generation/deploy

# Destroy and prepare build folder.
rm -rf build
mkdir build

# Install virtualenv
sudo docker run --rm -v "$PWD:/tmp" openwhisk/python3action bash \
  -c "cd tmp && virtualenv virtualenv && source virtualenv/bin/activate && pip3 install -r requirements.txt"

# Copy files to build folder.
cp -R ../src/* ./build
cp -R ../platforms/ibm/* ./build
cp -R virtualenv ./build
cd ./build
zip -X -r ./index.zip *

cd ../../../

#
# image-processing (ip)
#

cd python_image_processing/deploy

# Destroy and prepare build folder.
rm -rf build
mkdir build

# Install virtualenv
sudo docker run --rm -v "$PWD:/tmp" openwhisk/python3action bash \
  -c "cd tmp && virtualenv virtualenv && source virtualenv/bin/activate && pip3 install -r requirements.txt"

# Copy files to build folder.
cp -R ../src/* ./build
cp -R ../platforms/ibm/* ./build
cp -R virtualenv ./build
cd ./build
zip -X -r ./index.zip *

cd ../../../

#
# video-processing (vp)
#

cd python_video_processing/deploy

# Destroy and prepare build folder.
rm -rf build
mkdir build

# Install virtualenv
sudo docker run --rm -v "$PWD:/tmp" openwhisk/python3action bash \
  -c "cd tmp && virtualenv virtualenv && source virtualenv/bin/activate && pip3 install -r requirements.txt"

# Copy files to build folder.
cp -R ../src/* ./build
cp -R ../platforms/ibm/* ./build
cp -R virtualenv ./build

# Install non-pip dependency
wget -q https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz -P "./build"
pushd "./build" > /dev/null
tar -xf ffmpeg-release-amd64-static.tar.xz
rm *.tar.xz
mv ffmpeg-* ffmpeg
popd > /dev/null

cd ./build
zip -X -r ./index.zip *

cd ../../../

#
# image-recognition (ir)
#

cd python_image_recognition/deploy

# Destroy and prepare build folder.
rm -rf build
mkdir build

# Copy files to build folder.
cp -R ../src/* ./build
cp -R ../platforms/ibm/* ./build

cd ./build
zip -X -r ./index.zip *

cd ../../../

#
# k nearest neighbors (knn)
#

cd python_k_nearest_neighbors/deploy

# Destroy and prepare build folder.
rm -rf build
mkdir build

# Install virtualenv
sudo docker run --rm -v "$PWD:/tmp" openwhisk/python3action bash \
  -c "cd tmp && virtualenv virtualenv && source virtualenv/bin/activate && pip3 install -r requirements.txt"

# Copy files to build folder.
cp -R ../src/* ./build
cp -R ../platforms/ibm/* ./build
cp -R virtualenv ./build
cd ./build
zip -X -r ./index.zip *

cd ../../../

#
# arithmetic-logic-unit (alu)
#

cd python_arithmetic_logic_unit/deploy

# Destroy and prepare build folder.
rm -rf build
mkdir build

# Install virtualenv
sudo docker run --rm -v "$PWD:/tmp" openwhisk/python3action bash \
  -c "cd tmp && virtualenv virtualenv && source virtualenv/bin/activate && pip3 install -r requirements.txt"

# Copy files to build folder.
cp -R ../src/* ./build
cp -R ../platforms/ibm/* ./build
cp -R virtualenv ./build
cd ./build
zip -X -r ./index.zip *

cd ../../../

#
# merge-sorting (ms)
#

cd python_merge_sorting/deploy

# Destroy and prepare build folder.
rm -rf build
mkdir build

# Install virtualenv
sudo docker run --rm -v "$PWD:/tmp" openwhisk/python3action bash \
  -c "cd tmp && virtualenv virtualenv && source virtualenv/bin/activate && pip3 install -r requirements.txt"

# Copy files to build folder.
cp -R ../src/* ./build
cp -R ../platforms/ibm/* ./build
cp -R virtualenv ./build
cd ./build
zip -X -r ./index.zip *

cd ../../../

#
# gradient-descent (gd)
#

cd python_gradient_descent/deploy

# Destroy and prepare build folder.
rm -rf build
mkdir build

# Install virtualenv
sudo docker run --rm -v "$PWD:/tmp" openwhisk/python3action bash \
  -c "cd tmp && virtualenv virtualenv && source virtualenv/bin/activate && pip3 install -r requirements.txt"

# Copy files to build folder.
cp -R ../src/* ./build
cp -R ../platforms/ibm/* ./build
cp -R virtualenv ./build
cd ./build
zip -X -r ./index.zip *

cd ../../../

#
# dna-visualisation (dv)
#

cd python_dna_visualization/deploy

# Destroy and prepare build folder.
rm -rf build
mkdir build

# Install virtualenv
sudo docker run --rm -v "$PWD:/tmp" openwhisk/python3action bash \
  -c "cd tmp && virtualenv virtualenv && source virtualenv/bin/activate && pip3 install -r requirements.txt"

# Copy files to build folder.
cp -R ../src/* ./build
cp -R ../platforms/ibm/* ./build
cp -R virtualenv ./build
cd ./build
zip -X -r ./index.zip *

cd ../../../

echo ""
echo "Finish Compilation!"
echo ""
