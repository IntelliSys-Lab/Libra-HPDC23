#! /bin/bash


echo ""
echo "Deploying functions..."
echo ""

# dynamic-html (dh)
cd python_dynamic_html/deploy/build
wsk -i action update dh --kind python:3 --main main --memory 64 index.zip
cd ../../../

# email-generation (eg)
cd python_email_generation/deploy/build
wsk -i action update eg --kind python:3 --main main --memory 64 index.zip
cd ../../../

# image-processing (ip)
cd python_image_processing/deploy/build
wsk -i action update ip --kind python:3 --main main --memory 64 index.zip
cd ../../../

# video-processing (vp)
cd python_video_processing/deploy/build
wsk -i action update vp --kind python:3 --main main --memory 64 index.zip
cd ../../../

# image-recognition (ir)
cd python_image_recognition/deploy/build
wsk -i action update ir --docker yhf0218/actionloop-python-v3.6-ai --main main --memory 64 index.zip
cd ../../../

# k nearest neighbors (knn)
cd python_k_nearest_neighbors/deploy/build
wsk -i action update knn --kind python:3 --main main --memory 64 index.zip
cd ../../../

# arithmetic-logic-unit (alu)
cd python_arithmetic_logic_unit/deploy/build
wsk -i action update alu --kind python:3 --main main --memory 64 index.zip
cd ../../../

# merge-sorting (ms)
cd python_merge_sorting/deploy/build
wsk -i action update ms --kind python:3 --main main --memory 64 index.zip
cd ../../../

# gradient-descent (gd)
cd python_gradient_descent/deploy/build
wsk -i action update gd --kind python:3 --main main --memory 64 index.zip
cd ../../../

# dna-visualisation (dv)
cd python_dna_visualization/deploy/build
wsk -i action update dv --kind python:3 --main main --memory 64 index.zip
cd ../../../

# Initialize workload input in CouchDB
cd ../agent
python3 init_workload_input.py
cd ../workloads

echo ""
echo "Finish deployment!"
echo ""
