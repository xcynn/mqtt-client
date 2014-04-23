#!/bin/bash

cd /usr/local/mqtt-client/mqtt-client/
java -cp ./target/mqtt-client-1.10-SNAPSHOT-uber.jar org.fusesource.mqtt.cli.PublisherStaticNodeDemo -h tcp://10.217.138.185:1883 -q 2 -t /Demo/ -m "Demo" -n 10 -s 500 -itr 10000 -d
