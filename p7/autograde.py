from tester import *
import os
from datetime import datetime
import time
import re
import subprocess
import concurrent.futures
import json
import threading
import traceback
import time
from kafka import KafkaAdminClient, KafkaConsumer
from kafka.admin import NewTopic
from kafka.errors import UnknownTopicOrPartitionError
import subprocess
import threading
import time
import json

BROKER_URL = "localhost:9092"
TMP_DIR = "autograder_files"

MONTHS = {
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
}


def get_environment():
    environment = os.environ.copy()
    environment["DOCKER_CLI_HINTS"] = "false"


def log(s):
    print(f"\r|--------------- {s}", end="\n", flush=True)


def restart_kafka():
    subprocess.call("docker kill p7-autograder-kafka", shell=True)
    subprocess.call("docker rm p7-autograder-kafka", shell=True)
    try:
        result = subprocess.run(
            [
                "docker",
                "run",
                "--name",
                "p7-autograder-kafka",
                "-p",
                "9092:9092",
                "-e",
                "AUTOGRADER_DELAY_OVERRIDE_VAL=0.01",
                "-d",
                "p7-autograder-build",
            ],
            check=True,
        )
        if result.returncode != 0:
            return "Failed to run Kafka container"
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
    else:
        raise Exception("Failed to start Kafka")


def run_producer():
    try:
        result = subprocess.run(
            [
                "docker",
                "exec",
                "-d",
                "p7-autograder-kafka",
                "python3",
                "/files/producer.py",
            ],
            check=True,
        )
        if result.returncode != 0:
            raise Exception("Failed to run producer script")
    except subprocess.CalledProcessError as e:
        raise Exception("Failed to run producer script:" + str(e))


def run_consumer(partition_nums):
    args = [
                "docker",
                "exec",
                "-d",
                "p7-autograder-kafka",
                "python3",
                "/files/consumer.py",
            ]
    for p in partition_nums: args.append(str(p))
    try:
        result = subprocess.run(
            args,
            check=True,
        )
        if result.returncode != 0:
            raise Exception("Failed to run consumer script for partitions:", partition_nums)
    except subprocess.CalledProcessError as e:
        raise Exception("Failed to run consumer script:" + str(e) + "partitions:", partition_nums)


def delete_temp_dir():
    global TMP_DIR
    log(f"Cleaning up temp dir '{TMP_DIR}'")
    subprocess.check_output(f"rm -rf {TMP_DIR}", env=get_environment(), shell=True)


def create_temp_dir():
    global TMP_DIR
    log(f"Creating temp dir '{TMP_DIR}'")
    os.makedirs(TMP_DIR, exist_ok=True)


def save_cmd_output(command, output_file, duration=5):
    output_file = os.path.join(os.getcwd(), output_file)
    with open(output_file, "w") as file:
        process = subprocess.Popen(
            command, shell=True, stdout=file, stderr=subprocess.STDOUT
        )

    time.sleep(duration)
    process.terminate()


def read_file_from_docker(container_name, file_path):
    command = f"docker exec {container_name} cat {file_path}"
    result = subprocess.run(
        command,
        shell=True,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return result.stdout


def run_in_docker(container_name, command):
    command = f"docker exec {container_name} {command}"
    result = subprocess.run(
        command,
        shell=True,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return result.stdout

def is_day_count_valid(data):
    date2 = datetime.strptime(data['end'], "%Y-%m-%d")
    date1 = datetime(date2.year, date2.month, 1)
    delta = (date2 - date1).days + 1
    return data['count'] == delta

@cleanup
def _cleanup(*args, **kwargs):
    log("Cleaning up: Stopping all existing containers and temp files")
    subprocess.call("docker kill p7-autograder-kafka", shell=True)
    subprocess.call("docker rm p7-autograder-kafka", shell=True)
    # delete_temp_dir()


@init
def init(*args, **kwargs):
    create_temp_dir()
    pass


# Test all required files present
@test(5)
def test_all_files_present():
    files_dir = "files/"
    expected_files = ["Dockerfile"] + [
        files_dir + p
        for p in (
            "producer.py",
            "consumer.py",
            "debug.py",
            "plot.py",
            "report.proto",
            "weather.py",
            "report_pb2.py",
            "month.svg",
        )
    ]

    for file_name in expected_files:
        if not os.path.exists(file_name):
            return "Couldn't find file " + str(file_name)

    return None


# Test p7 image builds
@test(5)
def test_p7_image_builds():
    log("Running Test: build P7 image...")
    try:
        result = subprocess.run(
            ["docker", "build", ".", "-t", "p7-autograder-build"], check=True
        )
        return None if result.returncode == 0 else "Failed to build Dockerfile"
    except subprocess.CalledProcessError as e:
        return "Failed to build Dockerfile"


# Check p7 container runs
@test(5)
def test_p7_image_runs():
    log("Running Test: running P7 container...")
    restart_kafka()

# Check KafkaProducer(..., acks='all', retries=10)
@test(5)
def test_producer_configs():
    with open("files/producer.py", "r") as f:
        producer_content = f.read()
        
        p1 = r'KafkaProducer.*\(.*acks\s*\=\s*all.*retries\s*\=\s*10\)'
        p2 = r'KafkaProducer.*\(.*retries\s*\=\s*10.*acks\s*\=\s*all\)'
        
        match_p1 = re.search(p1, producer_content)
        match_p2 = re.search(p2, producer_content)

        if not match_p1 and match_p2:
            return "Have you set the producers 'acks' and 'retries'? Couldn't find: KafkaProducer(..., acks='all', retries=10) in producer.py"

# Test producer: check all topics created
@test(10)
def test_topics_created():
    log("Running Test: check producer creates all topics...")
    try:
        wait_for_kafka_to_be_up()
    except Exception as e:
        return "Kafka container did not start: " + str(e)

    try:
        run_producer()
    except Exception as e:
        return "Failed to run producer.py:" + str(e)

    for _ in range(60):
        admin_client = KafkaAdminClient(bootstrap_servers=["localhost:9092"])
        try:
            if "temperatures" in set(admin_client.list_topics()):
                break
        except Exception as e:
            pass
        time.sleep(1)
    else:
        return f"Expected topics: 'temperatures', Found: {admin_client.list_topics()}"

    # Fetch topic details
    topic_details = admin_client.describe_topics(["temperatures"])

    # Check details for each topic
    for topic in topic_details:
        topic_name = topic["topic"]
        partitions = len(topic["partitions"])

        # Expected values
        expected_partitions = 4

        # Check and print the details
        if partitions != expected_partitions:
            return f"Topic '{topic_name}' has incorrect partition count: Expected:{expected_partitions}, Found:{partitions}"


# test producer as consumer
@test(10)
def test_producer_messages():
    log("Running Test: checking 'temperatures' stream...")
    
    global MONTHS
    consumer = KafkaConsumer(
        bootstrap_servers=[BROKER_URL], auto_offset_reset="earliest"
    )
    consumer.subscribe(["temperatures"])

    time.sleep(10)  # Producer should be running, so wait for some data

    batch = consumer.poll(1000)

    if len(batch.items()) == 0:
        return "Was expecting messages in 'temperatures' stream but found nothing"

    for topic, messages in batch.items():
        if len(messages) == 0:
            return "Was expecting messages in 'temperatures' stream but found nothing"

        for message in messages:
            if str(message.key, "utf-8") not in MONTHS:
                return f"Key must be a month name (first letters capitalized), instead got: {message.key}"


# test proto generation
@test(5)
def test_proto_build():
    log("Running Test: testing proto file ...")

    global TMP_DIR
    tmp_dir_path = os.path.join(os.getcwd(), TMP_DIR)
    try:
        result = subprocess.run(
            [
                "python3",
                "-m",
                "grpc_tools.protoc",
                "-I",
                "./files",
                "--python_out",
                f"{tmp_dir_path}",
                "report.proto",
            ],
            check=True,
        )
        if result.returncode != 0:
            raise Exception("Failed to compile report.proto")
    except subprocess.CalledProcessError as e:
        raise Exception("Failed to compile report.proto:" + str(e))


@test(10)
def test_debug_consumer_output():
    log("Running Test: testing debug.py ...")
    
    time.sleep(10)

    out_file = "q7.out"
    save_cmd_output(
        "docker exec -it p7-autograder-kafka python3 /files/debug.py", out_file, 10
    )

    with open(out_file, "r") as file:
        for line in file:
            try:
                data = json.loads(
                    line.replace("'", '"')
                )  # Convert single quotes to double quotes for valid JSON
                if all(key in data for key in ["partition", "key", "date", "degrees"]):
                    return
                else: return "Invalid keys in the output of debug.py. Keys must be: 'partition','key','date','degrees'"
            except Exception as e:
                return "Invalid line in debug.py output: " + str(line)
        return "Couldn't find the expected ouput when running debug.py"


@test(10)
def test_consumer_runs():
    log("Running Test: running consumer ...")

    # Delete parition files inside the container
    for _ in range(10):
        try:
            for i in range(4):
                run_in_docker("p7-autograder-kafka", f"rm -rf /files/partition-{i}.json")
            run_in_docker("p7-autograder-kafka", "rm -rf /files/month.svg")
            break
        except Exception as e:
            pass
    else:
        return "Failed to setup consumer. Make sure your partition files are in a 'files' dir"

    try:
        run_consumer([0,1,2,3])
    except Exception as e:
        return "Failed to run consumer.py: " + str(e)


@test(10)
def test_partition_json_creation():
    log("Running Test: testing partition files ...\n")
    global MONTHS

    months_seen = set()
    part_nums_seen = set()
    partition_offsets = dict()
    
    time.sleep(60)

    for _ in range(15):
        error_msg = ""
        time.sleep(1)
        try:
            for i in range(4):
                try:
                    file_data = read_file_from_docker(
                        "p7-autograder-kafka", f"/files/partition-{i}.json"
                    )
                except Exception as e:
                    error_msg = str(e)
                    break
                partition_dict = json.loads(file_data)
                for key in partition_dict:
                    if key not in ("partition", "offset"):
                        months_seen.add(key)
                part_nums_seen.add(partition_dict["partition"])
                partition_offsets[partition_dict["partition"]] = partition_dict[
                    "offset"
                ]
            else:
                break
        except Exception as e:
            error_msg = str(e)
            pass
    else: return f"Failed to generate and read /files/partition-{i}.json from within the container: {error_msg}"

    for month in MONTHS:
        if month not in months_seen:
            return f"No partition JSON has weather summary for {month}"
    for i in range(4):
        if i not in part_nums_seen:
            return f"No partition JSON has 'partition' key = {i}"
    for k in partition_offsets:
        if partition_offsets[k] == 0:
            return f"Partition offset of partition number {k} doesn't increase"


# Validate contents of partition files generated
@test(15)
def test_partition_json_contents():
    log("Running Test: validating partition files ...\n")
    
    for _ in range(10):
        time.sleep(1)
        for i in range(4):
            try:
                file_data = read_file_from_docker(
                    "p7-autograder-kafka", f"/files/partition-{i}.json"
                )
                partition_dict = json.loads(file_data)
            except Exception as e:
                break
            for month in partition_dict:
                if month in ("partition", "offset"):
                    continue
                if len(partition_dict[month].keys()) == 0:
                    return f"No weather summary data generated for {month}: Make sure the partition JSON resembles the sample structure"
                for year in partition_dict[month]:
                    for key in {"count", "sum", "avg", "end"}:
                        if key not in partition_dict[month][year]:
                            return f"{month}-{year} doesn't contain the key:{key}"
                    if partition_dict[month][year]["count"] > 31:
                        return f"{month}-{year} has more than 31 days. Make sure you don't overcount messages"
                    if partition_dict[month][year]["avg"] > 1000:
                        return f"{month}-{year} avg. temperature is {partition_dict[month][year]['avg']}, yikes! Make sure duplicate messages are ignored"
                    if not is_day_count_valid(partition_dict[month][year]):
                        return f"{month}-{year} has an invalid 'count' when compared to 'start' and 'end' dates"
                        
        else: return
    return f"Failed to read /files/partition-{i}.json inside the container."

@test(10)
def test_plot_generation():
    for _ in range(10):
        try:
            time.sleep(1)
            run_in_docker("p7-autograder-kafka", "python3 /files/plot.py")
            break
        except Exception as e:
            pass
        
    patterns = [r'January-\d{4}', r'February-\d{4}', r'March-\d{4}']
    
    plot_data = read_file_from_docker("p7-autograder-kafka", "/files/month.svg")
    old_years = dict()
    
    for pattern in patterns:
        matches = re.findall(pattern, plot_data)
        if len(matches) == 0: return f"Couldn't find pattern {pattern} in month.svg"
        old_years[matches[0].split("-")[0]] = int(matches[0].split("-")[1]) 
    
    time.sleep(5) # Wait for atleast one year
    
    for _ in range(10):
        try:
            run_in_docker("p7-autograder-kafka", "python3 /files/plot.py")
            break
        except Exception as e:
            pass
        
    plot_data = read_file_from_docker("p7-autograder-kafka", "/files/month.svg")
    for pattern in patterns:
        matches = re.findall(pattern, plot_data)
        if len(matches) == 0: return f"Couldn't find pattern {pattern} in month.svg on re-running plot.py"
        if int(matches[0].split("-")[1]) <= old_years[matches[0].split("-")[0]]:
            return "Plot doesn't seem to be updating on re-running"

if __name__ == "__main__":
    tester_main()

