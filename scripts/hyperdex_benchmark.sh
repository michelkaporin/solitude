#!/bin/bash

# Arguments:
#   Server number: $1
#   Chunk size: $2
#   Repetitions number: $3

# Resolve the relative path because of https://github.com/rescrv/HyperDex/issues/216
tempDir=$(greadlink -f ./../temp) # 'brew install coreutils' to make it work on Mac OS (equivalent of readlink in Linux)
# Clean up coordinator and daemons' data
rm -rf $tempDir/*

# Run coordinator
mkdir -p $tempDir/hyperdex_data
hyperdex coordinator -l 127.0.0.1 -p 1982 -D $tempDir/hyperdex_data &
coordinatorPID=$!
echo "PID of the coordinator: $coordinatorPID"

# Run daemons
for i in $(seq 1 $1)
do
    daemonDir=$tempDir/hyperdex_daemon/$i
    echo "Starting a daemon #$i in $daemonDir"
    mkdir -p $daemonDir
    hyperdex daemon --listen=127.0.0.1 --listen-port=201$i --coordinator=127.0.0.1 --coordinator-port=1982 --data=$daemonDir &
done

# Setup spaces
python setup_spaces.py setup

# Run benchmarking
pushd ..
classPath='.:/usr/local/share/java/org.hyperdex.client-1.8.1.jar'
javac *.java -classpath $classPath
java -classpath $classPath -Djava.library.path=/usr/local/lib Main $2 $3 >> temp/hyperdex_benchmark.log
popd

# SIGTERM background HyperDex coordinator, daemons terminate themselfes after coordinator is down
kill -15 $coordinatorPID

test