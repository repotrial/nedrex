#!/bin/bash

docker rm -f neo4j

[ ! -d "$PWD/neo4j/data" ] && mkdir "$PWD/neo4j/data"
[ ! -d "$PWD/neo4j/logs" ] && mkdir "$PWD/neo4j/logs"
[ ! -d "$PWD/neo4j/certificates" ] && mkdir "$PWD/neo4j/certificates"

docker run \
    -p 0.0.0.0:7474:7474 \
    -p 0.0.0.0:7473:7473 \
    -p 0.0.0.0:7687:7687 \
    --name neo4j \
    -v $PWD/neo4j/data:/data \
    -v $PWD/neo4j/logs:/logs \
    -v $PWD/neo4j/conf:/var/lib/neo4j/conf/ \
    -v $PWD/neo4j/certificates:/ssl \
    --env NEO4J_AUTH=none \
    --env NEO4J_dbms_memory_pagecache_size=16G \
    --user="$(id -u):$(id -g)" \
    -d \
    neo4j:3.5.12

#    --env NEO4J_AUTH=neo4j/repodb \

