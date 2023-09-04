import subprocess
import time
import pandas as pd
from io import StringIO
import os
import random
import shutil

CLEANUP_AT_END = False

def run_command(command, timeout_val = None, throw_on_err = True):
    command_to_run = command.split(" ")

    std_out, std_err = None, None
    try:
        print("Running command", command_to_run)
        result = subprocess.run(command_to_run, timeout = timeout_val, capture_output = True, text = True)
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

def perform_startup():
    # Start them using docker-compose up
    print("Starting all the containers")
    std_out, _ = run_command("docker-compose up -d", timeout_val = 150)

    # Give time for cassandra nodes to startup
    time.sleep(20)
    print("Finished starting up all of the containers")

    # Get the main container
    std_out, _ = run_command("docker ps", timeout_val = 10)
    specs_df = pd.read_csv(StringIO(std_out), sep='\s{2,}', engine='python', header=0)
    specs_df = specs_df[specs_df["PORTS"].str.contains("5000->5000")]
    container_name = specs_df.iloc[0]["NAMES"]
    
    return container_name

def run_autograder(main_container, dest_dir = "autograder_results"):
    # Ensure that dest dir is reset
    if os.path.exists(dest_dir):
        shutil.rmtree(dest_dir)
    os.makedirs(dest_dir)

    # Startup the server
    command_to_run = "docker exec -it -d " + main_container + " python3 /notebooks/server.py"
    run_command(command_to_run)
    time.sleep(10)

    # Copy the notebook runner file
    src_path = "autograder_scripts/notebook_runner.py"
    dest_path = main_container + ":/notebook_runner.py"
    copy_command = "docker cp " + src_path + " " + dest_path
    std_out, std_err = run_command(copy_command, timeout_val = 20)
    
    # Run the autograder with 3 nodes
    autograder_command = "docker exec -it " + main_container + " python3 /notebook_runner.py --parts part_1 part_2 part_3"
    std_out, _ = run_command(autograder_command, timeout_val = 600)

    # Copy the result back
    src_path = main_container + ":/part_1,part_2,part_3,result.ipynb"
    dest_path = os.path.join(dest_dir, "3_node_result.ipynb")
    copy_command = "docker cp " + src_path + " " + dest_path
    run_command(copy_command, timeout_val = 20)

    # Kill of one of the non main nodes
    std_out, _ = run_command("docker ps", timeout_val = 10)
    specs_df = pd.read_csv(StringIO(std_out), sep='\s{2,}', engine='python', header=0)
    specs_df = specs_df[~specs_df["PORTS"].str.contains("5000->5000")]
    container_kill_loc = random.randrange(0, len(specs_df.index))
    kill_container_name = specs_df.iloc[container_kill_loc]["NAMES"]
    print("Killing container", kill_container_name)

    # Run the autograder with 2 nodes
    autograder_command = "docker exec -it " + main_container + " python3 notebook_runner.py --parts part_3 part_4"
    std_out, _ = run_command(autograder_command, timeout_val = 600)

    # Copy the result back
    src_path = main_container + ":/part_3,part_4,result.ipynb"
    dest_path = os.path.join(dest_dir, "2_node_result.ipynb")
    copy_command = "docker cp " + src_path + " " + dest_path
    run_command(copy_command, timeout_val = 20)

def cleanup():
    run_command("docker-compose down --rmi all")

def ensure_files_exist(files):
    for file in files:
        if not os.path.exists(file):
            raise Exception("Autograder couldn't find file " + str(file))

def runner():
    submission_notebook = "nb/p6.ipynb"
    ensure_files_exist(["docker-compose.yml", "image/Dockerfile", "image/cassandra.sh", 
                    submission_notebook, "nb/server.py", "nb/station.proto", 
                    "nb/station_pb2_grpc.py", "nb/station_pb2.py"])

    try:
        main_container = perform_startup()
        print("Got main container of", main_container)
        run_autograder(main_container)
    finally:
        cleanup()

if __name__ == "__main__":
    runner()