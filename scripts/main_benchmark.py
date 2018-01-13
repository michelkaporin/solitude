import sys
import time
from fabric.api import *
from fabric.colors import green

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

if len(sys.argv) < 5:
    print "Please specify args"
    sys.exit(0)

EXPERIMENT_TYPE = sys.argv[1]
CHUNK_SIZE = sys.argv[2]
REPETITIONS_NUM = sys.argv[3]
AWS_ACCESS_KEY_ID = sys.argv[4]
AWS_SECRET_ACCESS_KEY = sys.argv[5]
TEST_RANGE = sys.argv[4]
TEST_RANGE_ENTRY = sys.argv[5]

@parallel
def wipe_state(remove_logs):
    if remove_logs:
        command = 'rm -rf {}/*'.format(TEMP_DIR)
        run(command)

    command = 'pkill -f hyperdex'
    run(command)

    command = 'sudo service cassandra stop'
    run(command)

def start_coordinator():
    if EXPERIMENT_TYPE == 'Main':
        command = '{} hyperdex coordinator -f -p 1982 -D {}/hyperdex_data'.format(DTACH_COMMAND, TEMP_DIR)
    elif EXPERIMENT_TYPE == 'HyperDex':
        command = '{} hyperdex coordinator -f -l 127.0.0.1 -p 1982 -D {}/hyperdex_data'.format(DTACH_COMMAND, TEMP_DIR)
    run(command)

@parallel
def start_daemon(daemon_port):
    if EXPERIMENT_TYPE == 'Main':
        coordinator_ip = COORDINATOR_IP
    elif EXPERIMENT_TYPE == 'HyperDex':
        coordinator_ip = '127.0.0.1'
    command = '{} hyperdex daemon -f --listen-port={} --coordinator={} --coordinator-port=1982 --data={}/hyperdex_daemon{}'.format(DTACH_COMMAND, daemon_port, coordinator_ip, TEMP_DIR, daemon_port)
    run(command)

def setup_spaces():
    if EXPERIMENT_TYPE == 'Main':
        ip = COORDINATOR_IP
    elif EXPERIMENT_TYPE == 'HyperDex':
        ip = '127.0.0.1'
    command = 'python ~/solitude/scripts/setup_spaces.py {}'.format(ip)
    run(command)

@parallel
def start_cassandra():
    command = 'sudo service cassandra start'
    run (command)

def start_experiment():

    class_path = '.:/usr/local/share/java/org.hyperdex.client-1.8.1.jar:lib/aws-sdk/lib/aws-java-sdk-1.11.255.jar:lib/aws-sdk/third-party/lib/*:lib/cassandra/cassandra-driver-core-3.3.2.jar:lib/cassandra/lib/*'
    command = 'cd ~/solitude && javac -classpath "{}" $(find . -name \'*.java\')'.format(class_path)
    run(command)

    if EXPERIMENT_TYPE == 'Main':
        command = 'cd ~/solitude && java -classpath "{}" -Djava.library.path=/usr/local/lib ch/michel/test/MainBenchmark {} {} {} {} {} {}'.format(class_path, CHUNK_SIZE, REPETITIONS_NUM, COORDINATOR_IP, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, COORDINATOR_IP)
    elif EXPERIMENT_TYPE == 'HyperDex':
        command = 'cd ~/solitude && java -classpath "{}" -Djava.library.path=/usr/local/lib ch/michel/test/HyperDexBenchmark {} {} {} {} >> temp/hyperdex_benchmark.log'.format(class_path, CHUNK_SIZE, REPETITIONS_NUM, TEST_RANGE, TEST_RANGE_ENTRY)
    run(command)

# 0. Wipe all ex-state
# 1. Connect to the coordinator, run hyperdex coordinator
# 2. Connect to daemons, run hyperdex daemons
# 3. Connect to machine that will run an experiment, run java

print green('Wiping any previous execution state')
execute(wipe_state, True, hosts=ALL_HOSTS)

print green('Starting HyperDex coordinator')
execute(start_coordinator, hosts=[COORDINATOR])

print green('Setting HyperDex up spaces')
execute(setup_spaces, hosts=[COORDINATOR])

print green('Starting HyperDex daemons')
if EXPERIMENT_TYPE == 'Main':
    execute(start_daemon, 2014, hosts=DAEMONS)
elif EXPERIMENT_TYPE == 'HyperDex':
    for port in range(2014, 2018):
        execute(start_daemon, port, hosts=[COORDINATOR])

if EXPERIMENT_TYPE == 'Main':
    print green('Starting Cassandra seed nodes')
    execute(start_cassandra, hosts=[COORDINATOR])

    print green('Starting Cassandra non-seed nodes')
    execute(start_cassandra, hosts=DAEMONS)

time.sleep(60) # Give time for the datastores to spin up

print green('Running experiment')
if EXPERIMENT_TYPE == 'Main':
    execute(start_experiment, hosts=[EXPERIMENT])
elif EXPERIMENT_TYPE == 'HyperDex':
    execute(start_experiment, hosts=[COORDINATOR])

print green('Wipe the state')
execute(wipe_state, False, hosts=ALL_HOSTS)
