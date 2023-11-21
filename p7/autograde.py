from tester import *
import os
import time
import subprocess
import concurrent.futures
import json
import threading
import traceback
import time  
from kafka import KafkaAdminClient, KafkaConsumer
from kafka.admin import NewTopic
from kafka.errors import UnknownTopicOrPartitionError

BROKER_URL = 'localhost:9092'

def log(s):
    print(f"|------------- {s}")
    
def restart_kafka():
    subprocess.call("docker kill p7-autograder-kafka", shell=True)
    subprocess.call("docker rm p7-autograder-kafka", shell=True)
    try:
        result = subprocess.run(["docker", "run", "--name", "p7-autograder-kafka",
                                 "-p", "9092:9092", "-d", "p7-autograder-build"], check=True)
        if result.returncode != 0: return "Failed to run Kafka container"
    except subprocess.CalledProcessError as e:
        return "Failed to run Kafka container"

def wait_for_kafka_to_be_up():
    log(f"Re-starting Kafka for new test (waits up to 45 sec)...")
    for _ in range(45):
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=[BROKER_URL])
            admin_client.list_topics()
            break
        except Exception as e:
            time.sleep(1)
            pass
    else: raise Exception("Failed to start Kafka")

def run_producer():
    try:
        result = subprocess.run(["docker", "exec", "-d", "p7-autograder-kafka",
                                 "python3", "/files/producer.py"], check=True)
        if result.returncode != 0: raise Exception("Failed to run producer script")
    except subprocess.CalledProcessError as e:
        raise Exception("Failed to run producer script:" + str(e))

def run_consumer():
    try:
        result = subprocess.run(["docker", "exec", "-d", "p7-autograder-kafka",
                                 "python3", "/files/consumer.py"], check=True)
        if result.returncode != 0: raise Exception("Failed to run consumer script")
    except subprocess.CalledProcessError as e:
        raise Exception("Failed to run consumer script:" + str(e))

@cleanup
def _cleanup(*args, **kwargs):
    log("Cleaning up: Stopping all existing containers and temp files")
    subprocess.call("docker kill p7-autograder-kafka", shell=True)
    subprocess.call("docker rm p7-autograder-kafka", shell=True)

@init
def init(*args, **kwargs):
    # TODO: Check if python-kafka is installed
    pass

# Test all required files present
@test(1)
def q1():
    files_dir = "files/"
    expected_files = ["Dockerfile"] + [files_dir + p for p in (
        "producer.py",
        "consumer.py",
        "report.proto",
        "weather.py",
        "partition-0.json",
        "partition-1.json",
        "partition-2.json",
        "partition-3.json",
        "report_pb2.py",
        "yearly_trends.png"
    )]

    for file_name in expected_files:
        if not os.path.exists(file_name):
            return "Couldn't find file " + str(file_name)
    
    return None

# Test p7 image builds
@test(1)
def q2():
    log("Running Test: build P7 image...")
    try:
        result = subprocess.run(["docker", "build", ".", "-t", "p7-autograder-build"], check=True)
        return None if result.returncode == 0 else "Failed to build Dockerfile"
    except subprocess.CalledProcessError as e:
        return "Failed to build Dockerfile"

# Check p7 container runs
@test(1)
def q3():
    log("Running Test: running P7 container...")
    restart_kafka()

# Test producer: check all topics created
@test(1)
def q4():
    log("Running Test: check producer creates all topics...")
    try:
        wait_for_kafka_to_be_up()
    except Exception as e:
        return "Kafka container did not start: " + str(e)
    
    try: 
        run_producer()
    except Exception as e:
        return str(e)    
    
    for _ in range(30):
        admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092'])
        try:
            if set(admin_client.list_topics()) == {'temperatures', 'birthday-temperatures'}:
                break
        except Exception as e:
            time.sleep(1)
    else: return f"Expected topics: 'temperatures', 'birthday-temperatures', Found: {admin_client.list_topics()}"
    
    # Fetch topic details
    topic_details = admin_client.describe_topics(['temperatures', 'birthday-temperatures'])

    # Check details for each topic
    for topic in topic_details:
        topic_name = topic['topic']
        partitions = len(topic['partitions'])

        # Expected values
        expected_partitions = 4 if topic_name == 'temperatures' else 1

        # Check and print the details
        if partitions != expected_partitions:
            return f"Topic '{topic_name}' has incorrect partition count: Expected:{expected_partitions}, Found:{partitions}"

# test producer as consumer
@test(1)
def q5():
    log("Running Test: checking 'temperatures' stream...")
    
    consumer = KafkaConsumer(bootstrap_servers=[BROKER_URL], auto_offset_reset='earliest')
    consumer.subscribe(['temperatures'])
    
    time.sleep(1) # Producer should be running, so wait for some data

    batch = consumer.poll(1000)
    
    if len(batch.items()) == 0:
        return "Was expecting messages in 'temperatures' stream but found nothing"
    
    for topic, messages in batch.items():
        if len(messages) == 0:
            return "Was expecting messages in 'temperatures' stream but found nothing"
    
        for message in messages:
            if str(message.key, 'utf-8') not in {"January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"}:
                return f"Key must be a month name (first letters capitalized), instead got: {message.key}"

# test proto generation
# @test(1)
# def q6():
#     log("Running Test: testing proto file ...")
#     try:
#         result = subprocess.run(["python3", "-m", "grpc_toold.protoc", "-I", ".", 
#                                  "--python_out", f"./{TMP_DIR_NAME}", "report.proto"], check=True)
#         if result.returncode != 0: raise Exception("Failed to compile report.proto")
#     except subprocess.CalledProcessError as e:
#         raise Exception("Failed to compile report.proto:" + str(e))    
    
#     # run_consumer()
    

if __name__ == "__main__":
    tester_main()