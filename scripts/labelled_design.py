import sys
from fabric.api import *
from fabric.colors import green

USER = 'ubuntu'
SSH_KEY_PATH = '~/.ssh/hyperdex'
TEMP_DIR = '/home/ubuntu/solitude/temp'

EXPERIMENT_IP = '35.177.45.224'
EXPERIMENT = '{}@{}:22'.format(USER, EXPERIMENT_IP)

env.key_filename = SSH_KEY_PATH
env.warn_only = True
DTACH_COMMAND = 'dtach -n `mktemp -u /tmp/dtach.XXXX`'

CHUNK_SIZE = sys.argv[1]
REPETITIONS_NUM = sys.argv[2]

def wipe_state(remove_logs):
    if remove_logs:
        command = 'rm -rf {}/*'.format(TEMP_DIR)
        run(command)

    command = 'pkill -f hyperdex'
    run(command)

def start_coordinator():
    command = '{} hyperdex coordinator -f -l 127.0.0.1 -p 1982 -D {}/hyperdex_data'.format(DTACH_COMMAND, TEMP_DIR)
    run(command)

def setup_spaces():
    command = 'python ~/solitude/scripts/setup_spaces.py 127.0.0.1'
    run(command)

def start_daemon(daemon_port):
    command = '{} hyperdex daemon -f --listen-port={} --coordinator=127.0.0.1 --coordinator-port=1982 --data={}/hyperdex_daemon{}'.format(DTACH_COMMAND, daemon_port, TEMP_DIR, daemon_port)
    run(command)

def start_experiment():
    class_path = '.:/usr/local/share/java/org.hyperdex.client-1.8.1.jar:lib/aws-sdk/lib/aws-java-sdk-1.11.255.jar:lib/aws-sdk/third-party/lib/*:lib/cassandra/cassandra-driver-core-3.3.2.jar:lib/cassandra/lib/*'
    command = 'cd ~/solitude && javac -classpath "{}" $(find . -name \'*.java\')'.format(class_path)
    run(command)

    command = 'cd ~/solitude && java -classpath "{}" -Djava.library.path=/usr/local/lib ch/michel/test/LabelledDesign {} {} >> temp/labelled_benchmark.log'.format(class_path, CHUNK_SIZE, REPETITIONS_NUM)
    run(command)

print green('Wiping any previous execution state')
execute(wipe_state, True, hosts=[EXPERIMENT])

print green('Starting HyperDex coordinator')
execute(start_coordinator, hosts=[EXPERIMENT])

print green('Setting HyperDex up spaces')
execute(setup_spaces, hosts=[EXPERIMENT])

print green('Starting HyperDex daemons')
execute(start_daemon, 2014, hosts=[EXPERIMENT])

print green('Running experiment')
execute(start_experiment, hosts=[EXPERIMENT])

print green('Wipe the state')
execute(wipe_state, False, hosts=[EXPERIMENT])
