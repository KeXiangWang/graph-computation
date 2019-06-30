#!/bin/bash

rm  ./log/grape-trace.log
ps -ef|grep dmine-coordinator|awk '{print $2}'|xargs kill -s 9
ps -ef|grep dmine-worker|awk '{print $2}'|xargs kill -s 9

if [ $3 == "debug" ]
then
	if [ $2 == "true" ]
	then
		echo "aaaaaaaaaaa"
		gnome-terminal -- bash -c "java -Djava.security.policy=./target/security.policy -jar -ea ./target/dmine-coordinator-0.1.jar ;exec bash"
		sleep 2
		for ((i=1; i<=$1; i++))
		do
			gnome-terminal -- bash -c "java -Djava.security.policy=./target/security.policy -jar -ea ./target/dmine-worker-0.1.jar localhost;exec bash"
			sleep 2
		done
		gnome-terminal -- bash -c "java -Djava.security.policy=./target/security.policy -jar ./target/dmine-client-0.1.jar localhost;exec bash"
	else
		echo "bbbbbbbbbbb"
		gnome-terminal -- bash -c "java -Djava.security.policy=./target/security.policy -jar -ea ./target/dmine-coordinator-0.1.jar "
		sleep 2
		for i in {1..$1} 
		do
			gnome-terminal -- bash -c "java -Djava.security.policy=./target/security.policy -jar -ea ./target/dmine-worker-0.1.jar localhost"
			sleep 2
		done
		gnome-terminal -- bash -c "java -Djava.security.policy=./target/security.policy -jar ./target/dmine-client-0.1.jar localhost"
	fi
else
	if [ $2 == "true" ]
	then
		echo "ddddddddddd"
		gnome-terminal -- bash -c "java -Djava.security.policy=./target/security.policy -jar ./target/dmine-coordinator-0.1.jar ;exec bash"
		sleep 2
		for ((i=1; i<=$1; i++))
		do
			gnome-terminal -- bash -c "java -Djava.security.policy=./target/security.policy -jar ./target/dmine-worker-0.1.jar localhost;exec bash"
			sleep 2
		done
		gnome-terminal -- bash -c "java -Djava.security.policy=./target/security.policy -jar ./target/dmine-client-0.1.jar localhost;exec bash"
	else
		echo "eeeeeeeeeee"
		gnome-terminal -- bash -c "java -Djava.security.policy=./target/security.policy -jar ./target/dmine-coordinator-0.1.jar "
		sleep 2
		for i in {1..$1} 
		do
			gnome-terminal -- bash -c "java -Djava.security.policy=./target/security.policy -jar ./target/dmine-worker-0.1.jar localhost"
			sleep 2
		done
		gnome-terminal -- bash -c "java -Djava.security.policy=./target/security.policy -jar ./target/dmine-client-0.1.jar localhost"
	fi
fi
echo $1 
echo $2