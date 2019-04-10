#!/bin/bash

KEY_FILE_NAME = "amazon-grape-server.pem"
GRAPH_FILE_NAME = "amazon.dat"
GRAPH_PARTITION_COUNT = 4

#ssh login
ssh -i ~/.ssh/amazon-grape-server.pem HOSTNAME

#apt-get update
sudo apt-get update

#if no git
sudo apt-get install git

#config ssh between machines without password
git clone https://github.com/yecol/deploy.git
cp deploy/$KEY_FILE_NAME ~/.ssh/$KEY_FILE_NAME
chmod 400 ~/.ssh/$KEY_FILE_NAME

#if no java
sudo apt-get install openjdk-7-jdk

#set java home
#get JAVA_HOME: which java
export JAVA_HOME=/usr


#---------------COORDINATOR ONLY-------------------
#partition graph
./target/gpartition $GRAPH_FILE_NAME $GRAPH_PARTITION_COUNT
#distributed partitions
for machineName in arr
	scp -i ~/.ssh/$KEY_FILE_NAME ./target/$GRAPH_FILE_NAME* 172.31.50.121:~/grape/target/
done

COORDINATOR_HOST = 172.31.50.120

#begin work
java -Djava.security.policy=./target/security.policy -jar ./target/grape-coordinator-0.1.jar 
java -Djava.security.policy=./target/security.policy -jar ./target/grape-worker-0.1.jar 172.31.50.120
java -Djava.security.policy=./target/security.policy -jar ./target/grape-client-0.1.jar 172.31.50.120

#begin localhost
java -Djava.security.policy=./target/security.policy -jar ./target/dmine-coordinator-0.1.jar 
java -Djava.security.policy=./target/security.policy -jar ./target/dmine-worker-0.1.jar localhost
java -Djava.security.policy=./target/security.policy -jar ./target/dmine-client-0.1.jar localhost

#begin localhost
java -Djava.security.policy=./deploy/security.policy -jar ./deploy/dmine-coordinator-0.1.jar 
java -Djava.security.policy=./deploy/security.policy -jar ./deploy/dmine-worker-0.1.jar localhost
java -Djava.security.policy=./deploy/security.policy -jar ./deploy/dmine-client-0.1.jar localhost


