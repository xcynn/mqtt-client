#!/bin/bash

cd /usr/local/mqtt-client/mqtt-client/
java -cp ./target/mqtt-client-1.10-SNAPSHOT-uber.jar org.fusesource.mqtt.cli.AdaxSubscriber
