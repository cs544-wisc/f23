import subprocess
import time
import pandas as pd
from io import StringIO
import os

def run_command(command, timeout_val = None, throw_on_err = True):
    command_to_run = command.split(" ")

    std_out, std_err = None, None
    try:
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

def run_autograder(main_container):
    # Copy the notebook runner file
    src_path = "autograder_scripts/notebook_runner.py"
    dest_path = main_container + ":/notebook_runner.py"
    copy_command = "docker cp " + src_path + " " + dest_path
    std_out, std_err = run_command(copy_command, timeout_val = 20)
    
    # Run the autograder
    autograder_command = "docker exec -it " + main_container + " python3 notebook_runner.py"
    std_out, _ = run_command(autograder_command, timeout_val = 600)

    # Copy the result back
    src_path = main_container + ":/result.txt"
    dest_path = "result.txt"
    copy_command = "docker cp " + src_path + " " + dest_path
    std_out, std_err = run_command(copy_command, timeout_val = 20)
    print("Result written to", dest_path)

def perform_startup():
    # Check if the containers are already running
    std_out, _ = run_command("docker ps", timeout_val = 10)
    specs_df = pd.read_csv(StringIO(std_out), sep='\s{2,}', engine='python', header=0)
    if len(specs_df.index) > 0:
        return specs_df[specs_df["PORTS"].str.contains("5000->5000")].iloc[0]["NAMES"]

    # Since they don't exist start them using docker-compose up
    print("Starting all the containers")
    std_out, _ = run_command("docker-compose up -d", timeout_val = 150)

    # Give time for cassandra nodes to startup
    time.sleep(10)
    print("Finished starting up all of the containers")

    # Get the main container
    std_out, _ = run_command("docker ps", timeout_val = 10)
    specs_df = pd.read_csv(StringIO(std_out), sep='\s{2,}', engine='python', header=0)
    specs_df = specs_df[specs_df["PORTS"].str.contains("5000->5000")]
    container_name = specs_df.iloc[0]["NAMES"]
    
    return container_name

def cleanup():
    run_command("docker-compose down")

def ensure_files_exist(files):
    for file in files:
        if not os.path.exists(file):
            raise Exception("Autograder couldn't find file " + str(file))

def runner():
    submission_notebook = "nb/p6.ipynb"
    ensure_files_exist(["docker-compose.yml", "image/Dockerfile", "image/cassandra.sh", 
                    submission_notebook, "nb/server.py", "nb/station.proto", 
                    "nb/station_pb2_grpc.py", "nb/station_pb2.py"])

    # Perform startup
    try:
        main_container = perform_startup()
        print("Got main container of", main_container)
        run_autograder(main_container)
    finally:
        cleanup()

if __name__ == "__main__":
    runner()