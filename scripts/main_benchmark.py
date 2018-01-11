import sys
from fabric.api import *

# Configuration
USER = 'ubuntu'
SSH_KEY_PATH = '~/.ssh/hyperdex'
TEMP_DIR = '/home/ubuntu/solitude/temp'

COORDINATOR_IP = '35.176.244.248'
DAEMON1_IP = '35.176.39.60'
DAEMON2_IP = '35.176.147.101'
DAEMON3_IP = '35.176.153.202'
DAEMON4_IP = '35.177.77.248'
EXPERIMENT_IP = '35.177.45.224'

DAEMON1 = '{}@{}:22'.format(USER, DAEMON1_IP)
DAEMON2 = '{}@{}:22'.format(USER, DAEMON2_IP)
DAEMON3 = '{}@{}:22'.format(USER, DAEMON3_IP)
DAEMON4 = '{}@{}:22'.format(USER, DAEMON4_IP)
COORDINATOR = '{}@{}:22'.format(USER, COORDINATOR_IP)
EXPERIMENT = '{}@{}:22'.format(USER, EXPERIMENT_IP)

DAEMONS = [DAEMON1, DAEMON2, DAEMON3, DAEMON4]
ALL_HOSTS = DAEMONS+[COORDINATOR, EXPERIMENT]
env.key_filename = SSH_KEY_PATH
env.warn_only = True

DTACH_COMMAND = 'dtach -n `mktemp -u /tmp/dtach.XXXX`'

if len(sys.argv) < 4:
    print "Please specify the following args: chunk_size, repetitions_num, aws_access_key_id, aws_secret_access_key"
    sys.exit(0)

@parallel
def wipe_state():
    command = 'rm -rf {}/*'.format(TEMP_DIR)
    run(command)

    command = 'pkill -f hyperdex'
    run(command)

    command = 'sudo service cassandra stop'
    run(command)

@parallel
def start_coordinator():
    command = '{} hyperdex coordinator -f -p 1982 -D {}/hyperdex_data'.format(DTACH_COMMAND, TEMP_DIR)
    run(command)

def start_daemon():
    command = '{} hyperdex daemon -f --listen-port=2014 --coordinator={} --coordinator-port=1982 --data={}/hyperdex_daemon'.format(DTACH_COMMAND, COORDINATOR_IP, TEMP_DIR)
    run(command)

def setup_spaces():
    command = 'python ~/solitude/scripts/setup_spaces.py {}'.format(COORDINATOR_IP)
    run(command)

@parallel
def start_cassandra():
    command = 'sudo service cassandra start'
    run (command)

def start_experiment(chunk_size, repetitions_num, aws_access_key_id, aws_secret_access_key):
    class_path = '.:/usr/local/share/java/org.hyperdex.client-1.8.1.jar:lib/aws-sdk/lib/aws-java-sdk-1.11.255.jar:lib/aws-sdk/third-party/lib/*:lib/cassandra/cassandra-driver-core-3.3.2.jar:lib/cassandra/lib/*'
    command = 'cd ~/solitude && javac -classpath "{}" $(find . -name \'*.java\')'.format(class_path)
    run(command)

    command = 'cd ~/solitude && java -classpath "{}" -Djava.library.path=/usr/local/lib ch/michel/test/MainBenchmark {} {} {} {} {} {}'.format(class_path, chunk_size, repetitions_num, COORDINATOR_IP, aws_access_key_id, aws_secret_access_key, COORDINATOR_IP)
    run(command)

# 0. Wipe all ex-state
# 1. Connect to the coordinator, run hyperdex coordinator
# 2. Connect to daemons, run hyperdex daemons
# 3. Connect to machine that will run an experiment, run java

print 'Wiping previous execution state'
execute(wipe_state, hosts=ALL_HOSTS)

print 'Starting HyperDex coordinator'
execute(start_coordinator, hosts=[COORDINATOR])

print 'Setting HyperDex up spaces'
execute(setup_spaces, hosts=[COORDINATOR])

print 'Starting HyperDex daemons'
execute(start_daemon, hosts=DAEMONS)

print 'Starting Cassandra seed nodes'
execute(start_cassandra, hosts=[COORDINATOR])

print 'Starting Cassandra non-seed nodes'
execute(start_cassandra, hosts=DAEMONS)

print 'Running experiment'
execute(start_experiment, sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], hosts=[EXPERIMENT])
