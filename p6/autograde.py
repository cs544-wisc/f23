from tester import *
import os
import time
import subprocess
import concurrent.futures
import json


def verify_files_present():
    notebook_file = "nb/p6.ipynb"
    expected_files = ["cassandra.sh", "docker-compose.yml", "Dockerfile", notebook_file, "nb/server.py",
                      "nb/station.proto", "nb/ghcnd-stations.txt", "nb/records.zip"]

    for file_name in expected_files:
        if not os.path.exists(file_name):
            raise Exception("Couldn't find file " + str(file_name))

    # Verify that the p6 notebook file contains all required cells
    required_comments = set(["#q" + str(i) for i in range(1, 11)])
    with open(notebook_file) as reader:
        starting_notebook_content = reader.read()

    # Iterate through the cells
    found_comments = set()
    for comment in required_comments:
        if comment in starting_notebook_content:
            found_comments.add(comment)

    # Determine if there are any missing comments
    missing_comments = required_comments - found_comments
    if len(missing_comments) > 0:
        raise Exception(
            f"Couldn't find cells with comment(s) {missing_comments} in {notebook_file}"
        )


def get_environment():
    environment = os.environ.copy()
    environment["DOCKER_CLI_HINTS"] = "false"


@cleanup
def _cleanup(*args, **kwargs):
    # Shut down existing
    print("Stopping all existing containers")
    environment = get_environment()
    subprocess.call(["docker", "compose", "down"], env=environment)


output_dir_name = "autograder_result"


def init_runner(test_dir):
    global output_dir_name

    # Determine where the autograder will write the results to
    environment = get_environment()
    output_path = os.path.join("nb", output_dir_name)
    subprocess.check_output(["rm", "-rf", output_path], env=environment)
    os.makedirs(output_path, exist_ok=True)

    print("Checking if all valid files are present in", test_dir)
    verify_files_present()

    # Build the p6 base image
    _cleanup()
    print("Building the p6 base image")
    subprocess.check_output(
        ["docker", "build", ".", "-t", "p6-base"], env=environment)

    # Start up the docker container
    print("Running docker compose up")
    subprocess.check_output(["docker", "compose", "up", "-d"], env=environment)

    # Start the autograder
    print("Running docker_autograde.py inside of the p6-db-1 container")
    subprocess.call(["cp", "docker_autograde.py",
                    "nb/docker_autograde.py"], env=environment)

    command_to_run = f"docker exec -d -w /nb p6-db-1 sh -c \"python3 -u docker_autograde.py >> {output_dir_name}/autograder.out 2>&1\" "
    result = subprocess.run(
        command_to_run, capture_output=True, text=True, shell=True, env=environment)

    # Block till the docker_autograde requests the server to be run
    print("Running autograder from #q1 to #q4 - This might take a while")
    start_server_path = os.path.join(output_path, "start_server.record")
    while not os.path.exists(start_server_path):
        time.sleep(5)

    # Start up the server
    environment = get_environment()
    print("Starting up the server")
    command_to_run = f"docker exec -d -w /nb p6-db-1 sh -c \"python3 -u server.py >> {output_dir_name}/server.out 2>&1 \" "
    result = subprocess.run(
        command_to_run, capture_output=True, text=True, shell=True, env=environment)
    time.sleep(10)

    # Inform the container that the server has been started up
    started_server_path = os.path.join(output_path, "server_started.record")
    with open(started_server_path, "w+") as writer:
        pass
    print("Running the autograder from #q5 to #q7 - This might take a while")

    # Block till server requests to kill one of the nodes
    kill_node_file = os.path.join(output_path, "kill_node.record")
    while not os.path.exists(kill_node_file):
        time.sleep(5)

    # Kill one of the nodes
    print("Killing p6-db-2 node")
    result = subprocess.run(
        "docker kill p6-db-2", capture_output=True, text=True, shell=True, env=environment)
    time.sleep(10)

    # Inform the container that the node has been killed
    node_killed_file = os.path.join(output_path, "node_killed.record")
    with open(node_killed_file, "w+") as writer:
        pass
    print("Running the autograder from #q7 to #q10 - This might take a while")

    # Wait for the result file
    results_file = os.path.join(output_path, "result.ipynb")
    while not os.path.exists(results_file):
        time.sleep(5)

    # Copying the results back
    save_dir = os.path.join(test_dir, output_dir_name)
    os.makedirs(save_dir, exist_ok=True)

    print("Copying the autograder run results into", save_dir)
    time.sleep(5)
    os.system(f"cp -rf {output_path}/. {save_dir}")
    os.system(f"rm -rf {save_dir}/*.record")
    os.system(f"rm -rf nb/docker_autograde.py")

    _cleanup()


notebook_content = None


@init
def init(*args, **kwargs):
    global output_dir_name, notebook_content

    test_dir = kwargs["test_dir"]
    default_timeout = 900  # Allow for 15 minutes to run the entire notebook
    # Run the init function
    with concurrent.futures.ProcessPoolExecutor(max_workers=1) as executor:
        future = executor.submit(init_runner, test_dir)
        future.result(timeout=default_timeout)

    # Verify that we got some output
    output_dir = os.path.join(test_dir, output_dir_name)
    expected_path = os.path.join(test_dir, output_dir_name, "result.ipynb")
    if not os.path.exists(expected_path):
        raise Exception(
            f"Couldn't find results file at {expected_path}. Check the out files in {output_dir} for details")

    # Read the json notebook
    with open(expected_path, mode="r", encoding="utf-8") as reader:
        notebook = json.load(reader)

    if "cells" not in notebook:
        raise Exception(f"Notebook at {expected_path} doesn't have any cells")

    notebook_content = notebook["cells"]


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


def extract_txt_from_cell(cell):
    cell_output = ""
    if "outputs" in cell:
        for output in cell["outputs"]:
            if (output["output_type"] == "stream" and output["name"] == "stdout" and "text" in output):
                cell_output += "".join(output["text"])
            elif output["output_type"] == "execute_result" and "data" in output and "text/plain" in output["data"]:
                cell_output += "".join(output["data"]["text/plain"])

    return cell_output.lower()


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
    output = extract_txt_from_cell(cell)

    expected_txt = "0"
    if expected_txt not in output:
        return f"couldn't find txt {expected_txt} in output {output}"
    return None


if __name__ == "__main__":
    tester_main()
