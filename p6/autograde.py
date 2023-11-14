from tester import *
import os
import time
import subprocess
import concurrent.futures
import json
import threading
import traceback


def verify_files_present():
    notebook_file = "nb/p6.ipynb"
    expected_files = ["cassandra.sh", "docker-compose.yml", "Dockerfile", notebook_file, "pausable_nb_run.py",
                      "nb/station.proto", "nb/ghcnd-stations.txt", "nb/records.zip"]

    for file_name in expected_files:
        if not os.path.exists(file_name):
            raise Exception("Couldn't find file " + str(file_name))


def get_environment():
    environment = os.environ.copy()
    environment["DOCKER_CLI_HINTS"] = "false"


@cleanup
def _cleanup(*args, **kwargs):
    # Shut down existing
    print("Stopping all existing containers")
    environment = get_environment()
    subprocess.call(["docker", "compose", "down"], env=environment)


def wait_for_all_three_up():
    print("Waiting for the cassandra cluster to start up - This is going to take a while!!")

    all_three_up = False
    command_to_run = "docker exec -it p6-db-1 nodetool status"

    while not all_three_up:
        time.sleep(10)  # Wait a little bit

        # Read the result of nodetool status
        result = subprocess.run(
            command_to_run, capture_output=True, text=True, shell=True, env=os.environ.copy())

        all_three_up = result.stdout.count("UN") >= 3

    time.sleep(10)
    print("Cassandra cluster has started up")


def wait_for_one_dead():
    print("Waiting for a cassandra node to be down")
    one_node_dead = False
    command_to_run = "docker exec -it p6-db-1 nodetool status"

    while not one_node_dead:
        time.sleep(5)  # Wait a little bit

        # Read the result of nodetool status
        result = subprocess.run(
            command_to_run, capture_output=True, text=True, shell=True, env=os.environ.copy())

        one_node_dead = result.stdout.count("DN") >= 1

    time.sleep(10)
    print("Detected a down cassandra node")


def cell_pause_runner(output_path):
    # Block till the docker_autograde requests the server to be run
    q4_cell_path = os.path.join(output_path, "q4.cell")
    while not os.path.exists(q4_cell_path):
        time.sleep(5)
    print("Blocking notebook execution to startup server")

    # Start up the server
    environment = get_environment()
    expected_server_path = os.path.join("nb", "server.py")
    if os.path.exists(expected_server_path):

        # Startup the server
        server_start_cmd = f"docker exec -d -w /nb p6-db-1 sh -c \"python3 -u server.py >> {output_dir_name}/server.out 2>&1 \" "
        subprocess.run(server_start_cmd, shell=True, env=environment)
        time.sleep(10)
        print("Started up the server")
    else:
        print(
            f"WARNING: Couldn't start server because couldn't find server file at {expected_server_path}")

    # Inform the container that the server has been started up
    q4_remove_command = f"rm -rf {q4_cell_path}"
    subprocess.run(q4_remove_command, shell=True, env=environment)
    print("Resuming notebook execution")

    # Block till server requests to kill one of the nodes
    q7_cell_path = os.path.join(output_path, "q7.cell")
    while not os.path.exists(q7_cell_path):
        time.sleep(5)

    # Kill one of the nodes
    print("Blocking notebook execution to kill p6-db-2")
    subprocess.run("docker kill p6-db-2", shell=True, env=environment)

    # Waiting for node to be killed
    wait_for_one_dead()

    # Inform the container that the node has been killed
    q7_remove_command = f"rm -rf {q7_cell_path}"
    subprocess.run(q7_remove_command, shell=True, env=environment)
    print("Resuming notebook execution")


output_dir_name = "autograder_result"


def init_runner(test_dir):
    global output_dir_name

    # Determine where the autograder will write the results to
    environment = get_environment()
    output_path = os.path.join("nb", output_dir_name)
    subprocess.check_output(
        f"rm -rf {output_path}", shell=True, env=environment)
    os.makedirs(output_path, exist_ok=True)

    print("Checking if all valid files are present in", test_dir)
    verify_files_present()

    # Build the p6 base image
    _cleanup()

    print("Building the p6 base image")
    subprocess.check_output("docker build . -t p6-base",
                            shell=True, env=environment)

    # Start up the docker container
    print("Running docker compose up")
    subprocess.check_output("docker compose up -d",
                            shell=True, env=environment)

    # Wait for the cluster to be initialized
    wait_for_all_three_up()

    # Build the proto file before starting up the notebook
    proto_build_cmd = f"docker exec -d -w /nb p6-db-1 sh -c \"python3 -m grpc_tools.protoc -I=. --python_out=. --grpc_python_out=. station.proto \" "
    subprocess.run(proto_build_cmd, shell=True, env=environment)

    # Make all the notebooks writeable
    subprocess.check_output(
        "docker exec p6-db-1 sh -c 'chmod o+w nb/*.ipynb'", shell=True, env=environment)

    # Start the autograder
    print("Running the notebook inside of the p6-db-1 container - This is going to take a while!!")
    pausable_file_name = "pausable_nb_run.py"
    pausable_save_path = os.path.join("nb", pausable_file_name)
    subprocess.run(
        f"cp -f {pausable_file_name} {pausable_save_path}", shell=True, env=environment)

    command_to_run = f"docker exec -d -w /nb p6-db-1 sh -c \"python3 -u pausable_nb_run.py p6.ipynb --pauses=4,7 >> {output_dir_name}/nb_runner.out 2>&1\" "
    result = subprocess.run(command_to_run, shell=True, env=environment)

    # Start the listener as a daemon thread
    listener_thread = threading.Thread(
        target=cell_pause_runner, args=(output_path, ))
    listener_thread.daemon = True
    listener_thread.start()

    # Wait for the result file
    results_file = os.path.join(output_path, "result.ipynb")
    while not os.path.exists(results_file):
        time.sleep(5)

    # Copying the results back
    save_dir = os.path.join(test_dir, output_dir_name)
    os.makedirs(save_dir, exist_ok=True)

    time.sleep(5)
    os.system(f"cp -rf {output_path}/. {save_dir}")
    os.system(f"rm -rf {save_dir}/*.cell")
    os.system(f"rm -rf nb/pausable_nb_run.py")

    _cleanup()


notebook_content = None


@init
def init(*args, **kwargs):
    global output_dir_name, notebook_content

    expected_path = kwargs["existing_file"]
    if expected_path is None:
        test_dir = os.path.dirname(os.path.abspath(__file__))
        default_timeout = 900  # Allow for 15 minutes to run the entire notebook

        # Run the init function
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(init_runner, test_dir)
            future.result(timeout=default_timeout)

        # Verify that we got some output
        output_dir = os.path.join(test_dir, output_dir_name)
        expected_path = os.path.join(test_dir, output_dir_name, "result.ipynb")
        if not os.path.exists(expected_path):
            raise Exception(
                f"Couldn't find results file at {expected_path}. Check the out files in {output_dir} for details")

    print(f"Reading notebook from path {expected_path}")

    # Read the json notebook
    try:
        with open(expected_path, mode="r", encoding="utf-8") as reader:
            notebook = json.load(reader)

        if "cells" not in notebook:
            raise Exception(
                f"Notebook at {expected_path} doesn't have any cells")

        notebook_content = notebook["cells"]
    except Exception as e:
        print("Failed to parse output notebook due to error",
              traceback.format_exc())


def get_cell_containing_txt(target_txt):
    global notebook_content
    if notebook_content is None:
        return "Couldn't extract cells from notebook"

    for cell in notebook_content:
        if "source" in cell:
            cell_contents = "".join(cell["source"])
            if target_txt in cell_contents:
                return cell

    return f"Couldn't find cell with txt {target_txt}"


def extract_txt_from_cell(cell, read_stdout=True):
    cell_output = ""
    if "outputs" in cell:
        for output in cell["outputs"]:
            if (output["output_type"] == "stream" and output["name"] == "stdout" and "text" in output and read_stdout):
                cell_output += "".join(output["text"])
            elif output["output_type"] == "execute_result" and "data" in output and "text/plain" in output["data"]:
                cell_output += "".join(output["data"]["text/plain"])

    return cell_output.strip().lower()


def get_output_line(search_lines, search_txt):
    for curr_line in search_lines:
        if search_txt in curr_line:
            return curr_line.strip()

    return None


@test(10)
def q1():
    # Extract the output
    cell = get_cell_containing_txt("#q1")
    if isinstance(cell, str):
        return cell
    output = extract_txt_from_cell(cell)
    output_lines = output.split("\n")

    # Key is used to search for the line in the output -> Value is the expected txt in the line
    expected_terms = {
        "create table": ["weather.stations"],
        "id": ["text"],
        "date": ["date"],
        "name": ["text", "static"],
        "record": ["station_record"],
        "primary key": ["id"],
        "clustering order by": ["date"]
    }

    for search_txt, all_excepted_words in expected_terms.items():
        # Verify that we have a line
        line = get_output_line(output_lines, search_txt)
        if line is None:
            return f"Couldn't find line with txt {search_txt} in output {output}"

        # Ensure the expected txt is present
        for expected_txt in all_excepted_words:
            if expected_txt not in line:
                return f"Couldn't find txt {expected_txt} in line {line}"

    return None


@test(10)
def q2():
    # Extract the output
    cell = get_cell_containing_txt("#q2")
    if isinstance(cell, str):
        return cell
    output = extract_txt_from_cell(cell)

    expected_txt = "madison dane co rgnl ap"
    if expected_txt not in output:
        return f"couldn't find txt {expected_txt} in output {output}"
    return None


@test(10)
def q3():
    # Extract the output
    cell = get_cell_containing_txt("#q3")
    if isinstance(cell, str):
        return cell
    output = extract_txt_from_cell(cell)

    expected_txt = "-9014250178872933741"
    if expected_txt not in output:
        return f"couldn't find txt {expected_txt} in output {output}"
    return None


@test(10)
def q4():
    # Extract the output
    cell = get_cell_containing_txt("#q4")
    if isinstance(cell, str):
        return cell
    output = extract_txt_from_cell(cell)

    # Get the number
    row_number = int("-9014250178872933741")
    vnode_number = None
    try:
        vnode_number = int(output)
    except:
        return f"Couldn't parse txt {output} as integer"

    if vnode_number < row_number:
        return f"vnode number {vnode_number} should be greater than {row_number}"
    return None


@test(10)
def q5():
    # Extract the output
    cell = get_cell_containing_txt("#q5")
    if isinstance(cell, str):
        return cell
    output = extract_txt_from_cell(cell)

    expected_txt = "356"
    if expected_txt not in output:
        return f"couldn't find txt {expected_txt} in output {output}"
    return None


@test(10)
def q5():
    # Extract the output
    cell = get_cell_containing_txt("#q5")
    if isinstance(cell, str):
        return cell
    output = extract_txt_from_cell(cell)

    expected_txt = "356"
    if expected_txt not in output:
        return f"couldn't find txt {expected_txt} in output {output}"
    return None


@test(10)
def q6():
    # Extract the output
    cell = get_cell_containing_txt("#q6")
    if isinstance(cell, str):
        return cell
    output = extract_txt_from_cell(cell)

    for expected_txt in ["stations", "temporary"]:
        if expected_txt not in output:
            return f"couldn't find txt {expected_txt} in output {output}"

    return None


@test(10)
def q7():
    # Extract the output
    cell = get_cell_containing_txt("#q7")
    if isinstance(cell, str):
        return cell
    output = extract_txt_from_cell(cell)

    # Extract the dict
    output_line = output.replace("\n", "")
    if "{" not in output_line or "}" not in output_line:
        return f"No dictionary in output {output_line}"

    # Parse the dictionary
    dict_words = output_line[output_line.index(
        "{"): output_line.index("}") + 1]
    dict_words = dict_words.replace("'", '"')
    avg_diffs = None
    try:
        avg_diffs = json.loads(dict_words)
    except Exception as e:
        return f"failed to parse {dict_words} due to error {e}"

    # Verify the size
    if len(avg_diffs) != 4:
        return f"Only expected 4 keys but found {len(avg_diffs)}"

    # Compare the dicts
    expected_diffs = {
        "usr0000wddg": 102.07,
        "usw00014839": 89.70,
        "usw00014898": 102.94,
        "usw00014837": 105.64
    }

    for expected_key, expected_num in expected_diffs.items():
        # Verify the key is present
        if expected_key not in avg_diffs:
            return f"Couldn't find key {expected_key} in dict {avg_diffs}"

        # Compare the difference
        diff_val = avg_diffs[expected_key]
        curr_diff = None
        try:
            curr_diff = float(diff_val)
        except Exception as e:
            return f"Couldn't parse txt {diff_val} as float"

        if abs(curr_diff - expected_num) > 0.02:
            return f"Differnce between expected {expected_num} and actual {curr_diff} for key {expected_key} is > 0.02"

    return None


@test(10)
def q8():
    # Extract the output
    cell = get_cell_containing_txt("#q8")
    if isinstance(cell, str):
        return cell
    output = extract_txt_from_cell(cell)

    expected_txt = "dn"
    if expected_txt not in output:
        return f"couldn't find txt {expected_txt} in output {output}"
    return None


@test(10)
def q9():
    # Extract the output
    cell = get_cell_containing_txt("#q9")
    if isinstance(cell, str):
        return cell
    output = extract_txt_from_cell(cell)

    expected_txt = "need 3 replicas, but only have 2"
    if expected_txt not in output:
        return f"couldn't find txt {expected_txt} in output {output}"
    return None


@test(10)
def q10():
    # Extract the output
    cell = get_cell_containing_txt("#q10")
    if isinstance(cell, str):
        return cell

    output = extract_txt_from_cell(cell, read_stdout=False)
    cleaned_output = "".join([s for s in output if s.isalnum()]).strip()
    if len(cleaned_output) > 0:
        return f"Expected empty err msg but got output of length {len(output)}"
    return None


if __name__ == "__main__":
    tester_main()
