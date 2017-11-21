#!/bin/bash

docker run \
-it \
--rm \
-d \
-p 8003:80 \
-v "$PWD/db:/data/db" \
-v "$PWD/json:/data/json" \
--name recognizer-server \
recognizer-server \
/data/recognizer-server -d /data/db -p 80