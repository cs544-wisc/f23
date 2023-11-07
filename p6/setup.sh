#!/bin/bash
wget https://raw.githubusercontent.com/cs544-wisc/f23/main/p6/docker-compose.yml -O docker-compose.yml
wget https://raw.githubusercontent.com/cs544-wisc/f23/main/p6/cassandra.sh -O cassandra.sh
wget https://raw.githubusercontent.com/cs544-wisc/f23/main/p6/Dockerfile -O Dockerfile

mkdir -p nb
wget https://raw.githubusercontent.com/cs544-wisc/f23/main/p6/nb/ghcnd-stations.txt -O nb/ghcnd-stations.txt
wget https://raw.githubusercontent.com/cs544-wisc/f23/main/p6/nb/records.zip -O nb/records.zip
wget https://raw.githubusercontent.com/cs544-wisc/f23/main/p6/nb/station.proto -O nb/station.proto

wget https://raw.githubusercontent.com/cs544-wisc/f23/main/p6/autograde.py -O autograde.py
wget https://raw.githubusercontent.com/cs544-wisc/f23/main/p6/pausable_nb_run.py -O pausable_nb_run.py

wget https://raw.githubusercontent.com/cs544-wisc/f23/main/tester.py -O tester.py