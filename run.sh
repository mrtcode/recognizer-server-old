#!/bin/bash

docker run \
-it \
--rm \
-p 8003:80 \
-v "$PWD/db:/data/db" \
--name recognizer-server \
recognizer-server \
/data/recognizer-server -d /data/db -p 80