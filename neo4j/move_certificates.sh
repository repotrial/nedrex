#!/bin/bash

mkdir -p certificates/revoked
mkdir -p certificates/trusted


export MY_DOMAIN=repotrial.net

# Configure cert neo4j will use
sudo cp /etc/letsencrypt/live/$MY_DOMAIN/fullchain.pem certificates/neo4j.cert
# Configure private key neo4j will use
sudo cp /etc/letsencrypt/live/$MY_DOMAIN/privkey.pem certificates/neo4j.key
sudo chown james:james certificates/neo4j*

# Indicate that this cert is trusted for neo4j
# sudo cp /etc/letsencrypt/live/$MY_DOMAIN/fullchain.pem certificates/https/trusted/neo4j.cert
