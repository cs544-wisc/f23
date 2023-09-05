import subprocess
import time
import pandas as pd
from io import StringIO
import os
import random
import shutil
from subprocess import Popen, PIPE
import threading

def run_command(command, timeout_val = None, throw_on_err = True, debug = False):
    command_to_run = command.split(" ")

    std_out, std_err = None, None
    try:
        result = subprocess.run(command_to_run, timeout = timeout_val, capture_output = True, text = True)
        if debug:
            print("Command", command, "exited with code", result.returncode)

        if result.returncode != 0:
            std_err = "Command " + command + " exited with code " + str(result.returncode)
            std_err += " due to error " + result.stderr + " and standard out " + result.stdout
        else:
            std_out = result.stdout.strip()
    except Exception as e:
        std_err = "Failed to run command " + str(command) + " due to error: " + str(e)

    if throw_on_err and std_err is not None and len(std_err) > 0:
        raise Exception(std_err)

    return std_out, std_err

def perform_startup(startup_timeout = 180, command_timeout = 20, bootup_buffer = 60, debug = False):
    # Start them using docker-compose up
    if debug:
        print("Starting all the containers")
    std_out, _ = run_command("docker-compose up -d", timeout_val = startup_timeout, debug = debug)
    time.sleep(5)

    # Get the main container
    std_out, _ = run_command("docker ps", timeout_val = command_timeout)
    specs_df = pd.read_csv(StringIO(std_out), sep='\s{2,}', engine='python', header=0)
    specs_df = specs_df[specs_df["PORTS"].str.contains("5000->5000")]
    container_name = specs_df.iloc[0]["NAMES"]
    if debug:
        print("Got main container of", container_name)

    # Give time for cassandra nodes to startup
    if debug:
        print("Waiting for cassandra nodes to bootup ...")

    check_command = "docker exec -it " + container_name + " nodetool ring"
    are_alive = False
    while not are_alive:
        std_out, std_err = run_command(check_command, throw_on_err = False)
        if (std_err is None or len(std_err.strip()) == 0) and (std_out is not None):
            are_alive = True
        else:
            time.sleep(10)

    time.sleep(bootup_buffer)
    if debug:
        print("Can connect to the cassandra nodes")

    return container_name

def run_autograder(main_container, notebook_runner_path, notebook_runner_timeout, dest_dir_name = "autograder_results", 
                   command_timeout = 20, server_wait_time = 30, debug = False):
    
    # Ensure that dest dir is reset
    dest_dir = os.path.join(os.getcwd(), dest_dir_name)
    if os.path.exists(dest_dir):
        shutil.rmtree(dest_dir)
    os.makedirs(dest_dir)

    # Copy the notebook runner file
    src_path = notebook_runner_path
    dest_path = main_container + ":/notebook_runner.py"
    copy_command = "docker cp " + src_path + " " + dest_path
    run_command(copy_command, timeout_val = command_timeout, debug = debug)

    # Startup the server
    if debug:
        print("Starting up server")
    server_start_command = "docker exec -it -d " + main_container + " python3 /notebooks/server.py"
    run_command(server_start_command, timeout_val = command_timeout, debug = debug)
    time.sleep(server_wait_time)
    
    # Run the autograder with 3 nodes
    if debug:
        print("Running notebook with all three nodes up")
        
    autograder_command = "docker exec -it " + main_container + " python3 /notebook_runner.py --parts part_1 part_2 part_3 part_4"
    run_command(autograder_command, timeout_val = notebook_runner_timeout, debug = debug)

    # Copy the result back
    src_path = main_container + ":/part_1,part_2,part_3,part_4,result.ipynb"
    notebook_all3_path = os.path.join(dest_dir, "3_node_result.ipynb")
    copy_command = "docker cp " + src_path + " " + notebook_all3_path
    run_command(copy_command, timeout_val = command_timeout, debug = debug)

    # Kill of one of the non main nodes
    std_out, _ = run_command("docker ps", timeout_val = command_timeout)
    specs_df = pd.read_csv(StringIO(std_out), sep='\s{2,}', engine='python', header=0)
    specs_df = specs_df[~specs_df["PORTS"].str.contains("5000->5000")]
    container_kill_loc = random.randrange(0, len(specs_df.index))
    kill_container_name = specs_df.iloc[container_kill_loc]["NAMES"]

    if debug:
        print("Killing container", kill_container_name)

    kill_command = "docker kill " + kill_container_name
    run_command(kill_command, timeout_val = command_timeout, debug = debug)

    # Run the autograder with 2 nodes
    if debug:
        print("Running notebook with only two nodes up")

    autograder_command = "docker exec -it " + main_container + " python3 notebook_runner.py --parts part_1 part_2 part_3 part_4"
    run_command(autograder_command, timeout_val = notebook_runner_timeout, debug = debug)

    # Copy the result back
    src_path = main_container + ":/part_1,part_2,part_3,part_4,result.ipynb"
    notebook_all2_path = os.path.join(dest_dir, "2_node_result.ipynb")
    copy_command = "docker cp " + src_path + " " + notebook_all2_path
    run_command(copy_command, timeout_val = command_timeout, debug = debug)

    return (notebook_all3_path, notebook_all2_path)

def cleanup():
    run_command("docker-compose down --rmi all")

def ensure_files_exist(files):
    for file in files:
        if not os.path.exists(file):
            raise Exception("Autograder couldn't find file " + str(file))

def run_notebooks(notebook_runner_path, notebook_runner_timeout = 750, debug = False):
    cleanup()
    submission_notebook = "nb/p6.ipynb"
    ensure_files_exist(["docker-compose.yml", "image/Dockerfile", "image/cassandra.sh", 
                    submission_notebook, "nb/server.py", "nb/station.proto", 
                    "nb/station_pb2_grpc.py", "nb/station_pb2.py"])

    result_paths = None
    try:
        main_container = perform_startup(debug = debug)
        result_paths = run_autograder(main_container, notebook_runner_path, notebook_runner_timeout, debug = debug)
    except Exception as e:
        print("Failed to run p6 notebook due to error", e)
    finally:
        cleanup()
    
    return result_paths