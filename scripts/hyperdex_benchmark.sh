#!/bin/bash

# Arguments: server number - $0

# Clean up coordinator and daemons' data
tempDir="./../temp"
rm -rf $tempDir/HyperDex_Da*/*

# Run coordinator
hyperdex coordinator -l 127.0.0.1 -p 1982 -D $tempDir/Hyperdex_Data &
coordinatorPID=$!
echo "PID of the coordinator: $coordinatorPID"

# Run daemons
for i in $(seq 1 $1)
do
    echo "Starting a daemon #$i in $tempDir/HyperDex_Daemon/$i"
    mkdir -p $tempDir/HyperDex_Daemon/$i
    hyperdex daemon --listen=127.0.0.1 --listen-port=201$i --coordinator=127.0.0.1 --coordinator-port=1982 --data=$tempDir/HyperDex_Daemon/$i &
done

# Setup spaces
python setup_spaces.py

# Run benchmarking
pushd ..
java -classpath '.:/usr/local/share/java/org.hyperdex.client-1.8.1.jar' -Djava.library.path=/usr/local/lib Main
popd

# SIGTERM background HyperDex coordinator, daemons terminate themselfes after coordinator is down
kill -15 $coordinatorPID

test