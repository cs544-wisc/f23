import os, datetime, json, time, re
import subprocess
from subprocess import check_output
from subprocess import Popen, PIPE
from pathlib import Path
from io import StringIO

import docker
import pandas as pd

from tester import init, test, tester_main

# key=num, val=answer (as string)
ANSWERS = {}

global part2_json_content
global main
global worker1
global worker2
global lines
global ls_output 

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

def perform_startup(startup_timeout = 400, command_timeout = 20, bootup_buffer = 60, debug = False):
    cleanup()

    check_output("docker build . -f hdfs.Dockerfile -t p4-hdfs", shell=True)
    check_output("docker build . -f namenode.Dockerfile -t p4-nn", shell=True)
    check_output("docker build . -f datanode.Dockerfile -t p4-dn", shell=True)
    check_output("docker build . -f notebook.Dockerfile -t p4-nb", shell=True)

    # Start them using docker-compose up
    if debug:
        print("Starting all the containers")
    std_out, _ = run_command("docker compose up -d", timeout_val = startup_timeout, debug = debug)
    # sleep so it has time to get setup
    time.sleep(10)

    # Get the notebook container
    std_out, _ = run_command("docker ps", timeout_val = command_timeout)
  
    specs_df = pd.read_csv(StringIO(std_out), sep='\s{2,}', engine='python', header=0)
    specs_df = specs_df[specs_df["PORTS"].str.contains("5000->5000")]
    container_name = specs_df.iloc[0]["NAMES"]
    if debug:
        print("Got notebook container of", container_name)

    return container_name
   

# TODO: are we overriding the cleanup() function we import?
def cleanup():
    try:
        # stop all running docker containers
        result = subprocess.run(["docker", "container", "ls", ], capture_output = True, check=True, shell=False)
        if result.stdout.decode('utf-8').count("\n") > 1:
            subprocess.run(["docker stop $(docker ps -q)" ], check=True, shell=True)
        
        # remove image to build it fresh
        result = subprocess.run(["docker", "compose", "down"], capture_output=True, check=True, shell=False)
        # TODO: update this
        result = subprocess.run(["docker", "rmi", "-f", "p4-image"], check=True, shell=False)
    except Exception as ex:
        pass


def run_student_code():
    container_name = perform_startup(debug=True)
    file_dir = os.path.abspath(__file__)
    tester_dir = os.path.dirname(file_dir)
    path,_ = run_command("pwd")
    
    # for a later test
    global lines
    global main
    global worker1
    global worker2
    global ls_output
    ls_output = subprocess.run(["docker", "container", "ls", ], capture_output = True, check=True, shell=False).stdout.decode('utf-8')
    lines = len(ls_output.split("\n")) <= 3
    main = "main" in ls_output
    worker2 = "worker2" in ls_output
    worker1 = "worker1" in ls_output

    print("Running Part 1 notebook... this will take a while")
    cmd = (f"docker exec {container_name} sh -c '" +
                "export CLASSPATH=`$HADOOP_HOME/bin/hdfs classpath --glob` && " +
                "python3 -m nbconvert --execute --to notebook " +
                "nb/p4a.ipynb --output tester-p4a.ipynb'")
    print(cmd)
    check_output(cmd, shell=True)

    print("Killing worker 1")
    client = docker.from_env()
    containers = client.containers.list()
    for container in containers:
        if "worker1" in container.name:
            container.stop()

    print("Sleeping for 90 seconds to give HDFS time to detect the stopped node as dead")
    time.sleep(90)

    print("Running Part 2 notebook... this will take a while")
    cmd = (f"docker exec {container_name} sh -c '" +
                "export CLASSPATH=`$HADOOP_HOME/bin/hdfs classpath --glob` && " +
                "python3 -m nbconvert --execute --to notebook " +
                "nb/p4b.ipynb --output tester-p4b.ipynb'")
    print(cmd)
    check_output(cmd, shell=True)

    # make all notebooks writable (if only root can, it's a pain to delete/overwrite later)
    cmd = (f"docker exec {container_name} sh -c 'chmod o+w nb/*.ipynb'")
    print(cmd)
    check_output(cmd, shell=True)

def extract_notebook_answers(path):
    print(path)
    answers = {}
    with open(path) as f:
        nb = json.load(f)
        cells = nb["cells"]
        expected_exec_count = 1

        for cell in cells:
            if cell["cell_type"] != "code":
                continue
            if not cell["source"]:
                continue
            m = re.match(r"#[qQ](\d+)(.*)", cell["source"][0].strip())
            if not m:
                continue

            # found a answer cell, add its output to list
            qnum = int(m.group(1))
            notes = m.group(2).strip()
            if qnum in answers:
                print(f"Warning: answer {qnum} repeated!")
            expected = 1 + (max(answers.keys()) if answers else 0)
            if qnum != expected:
                print(f"Warning: Expected question {expected} next but found {qnum}!")

            for output in cell["outputs"]:
                print("DEBUG", output)
                if output.get("output_type") == "execute_result":
                    answers[qnum] = "\n".join(output["data"]["text/plain"])
                if output.get("output_type") == "stream":
                    if not qnum in answers:
                        answers[qnum] = "\n".join(output["text"])

    return answers


def extract_student_answers():
    path = Path("nb") / "tester-p4a.ipynb"
    print(path)
    if os.path.exists(path):
        ANSWERS.update(extract_notebook_answers(path))

    path = Path("nb") / "tester-p4b.ipynb"
    if os.path.exists(path):
        ANSWERS.update(extract_notebook_answers(path))


@init
def init(verbose = False):
    run_student_code()
    extract_student_answers()


@test(points=10)
def q1():
    if not 1 in ANSWERS:
        raise Exception("Answer to question 1 not found")
    if not "Live datanodes (2):" in ANSWERS[1]:
        return "Q1 output does not indicate 2 live datanodes"

@test(points=10)
def q2():
    if not 2 in ANSWERS:
        raise Exception("Answer to question 2 not found")
    
# @test(points = 10, timeout = 100)
# def basic_setup_test():
#     pwd = False
#     docker = False
#     wget = False
#     for cell in parta_json_content[0]['cells']:
#         if cell["cell_type"]  == "code":
#             if "1.1" in cell['source']:
#                 if "/\r\n" in cell['outputs'][0]['text']:
#                     pwd = True
#             if "1.2" in cell['source']:  
#                 if "/usr/bin/sh: 1: docker: not found" in cell['outputs'][0]['text']:
#                     docker = True
#             if "1.3" in cell['source']:
#                 for line in cell['outputs'][0]['text'].split("\n"):
#                     if "hdma-wi-2021.csv" in line:
#                         wget = True
            
#     if pwd and wget and docker:
#         return None
#     else: 
#         return f"Failed (0/10): Failed tests marked false -- 1. pwd test: {pwd}, 1.2 docker test: {docker}, 1.3 wget test: {wget}"

# @test(points = 15, timeout = 100)
# def hdfs_setup_test():
#     rep_settings = False
#     block_size = False
#     for cell in parta_json_content[0]['cells']:
#         if cell["cell_type"]  == "code":
           
#             if "1.4" in cell['source']:

#                 double = False
#                 single = False
#                 for line in cell['outputs'][0]['text'].split("\n"):
#                     if 'single.csv' in line:
#                         if "166.8" in line and line.count("166.8") == 2:
#                             single = True
#                     if 'double.csv' in line:
#                         if '333.7' in line:
#                             double = True
#                 rep_settings = single and double
            
        
#             if "1.5" in cell['source']:
#                 if '1048576' in cell['outputs'][1]['text'] and '1048576' in cell['outputs'][3]['text']:
#                     block_size = True
#     if rep_settings and block_size:
#         return None
#     else:
#         return f"Failed (0/15): Failed tests marked false -- 1.4 replication settings check: {rep_settings}, 1.5 block size check: {block_size}"

# @test(points = 15, timeout = 100)
# def block_location_test():
#     for cell in parta_json_content[0]['cells']:
#         if cell["cell_type"]  == "code":
#             if "2.1" in cell['source']:

                
#                 output = cell['outputs'][0]['data']['text/plain']
#                 output = output.strip("\,\{\} ").split("\n")
#                 count_1 = int(output[0].split(":")[3].strip(" ,"))
#                 count_2 = int(output[1].split(":")[3])
#                 if count_1 + count_2 == 167:
#                     return None
#     return f"Failed (0/15): Count 1 and Count2 did not add up to 167 -- count 1: {count_1}, count 2: {count_2}"

# @test(points = 15, timeout = 100)
# def hdfs_count_test():
#     s_family = False
#     s_single = False

#     d_family = False
#     d_single = False
#     for cell in parta_json_content[0]['cells']:
#         if cell["cell_type"]  == "code":
#             if "3.1" in cell['source']:
#                 s_single = int(cell['outputs'][0]['data']['text/plain']) == 444874
#             if "3.2" in cell['source']:
#                 s_family = int(cell['outputs'][0]['data']['text/plain']) == 2493
#             if "3.4" in cell['source']:
#                 d_single = int(cell['outputs'][0]['data']['text/plain']) == 444874
#             if "3.5" in cell['source']:
#                 d_family = int(cell['outputs'][0]['data']['text/plain']) == 2493
    

#     if s_family and s_single and d_family and d_single:
#         return None
#     return f"Failed (0/15): Incorrect counts marked false -- singe.csv \"single\" line count {s_single}, singe.csv \"family\" line count {s_family} \
#             double.csv \"single\" line count {d_single}, double.csv \"family\" line count {d_family}"

# @test(points = 15, timeout = 100)
# def different_buffer_size_tests():
    
#     for cell in parta_json_content[0]['cells']:
#         if cell["cell_type"]  == "code":
#             if "3.3" in cell['source']:
#                 t1 = float(cell['outputs'][0]['data']['text/plain'])
#             if "3.6" in cell['source']:
#                 t2 = float(cell['outputs'][0]['data']['text/plain'])
#             if "3.7" in cell['source']:
#                 temp =  cell['outputs'][0]['data']['text/plain'].strip(" () ").split(",")
#                 bs1 = int(temp[0].strip(" "))
#                 bs2 = int(temp[1].strip(" "))
#     if (t1 != t2 and bs1 != bs2):
#                 return None
    
#     return f"Failed (0/10): Either your times or your buffer sizes matched -- time1: {t1}, time2: {t2}, buffer size 1: {bs1}, buffer size 2: {bs2}"

# @test(points=10)
# def dfs_admin_test():
#     for cell in part2_json_content[0]['cells']:
#         if cell["cell_type"]  == "code":
#             if "4.1" in cell['source']:

#                 for outputs in cell['outputs']:
#                     if "Dead datanodes (1):" in outputs['text']:
#                         return None
#     return "FAILED (0/10): hdfs dfsadmin -fs hdfs://main:9000/ -report did not show a dead datanode"

# @test(points=10)
# def newline_test():
#     for cell in part2_json_content[0]['cells']:
#         if cell["cell_type"]  == "code":
#             if "4.2" in cell['source']:
#                 if int(cell['outputs'][0]['data']['text/plain']) > 0:
#                     return None
#     return "FAILED (0/10): no newline replacments detected"

# @test(points=10)
# def post_diaster_count_test():
#     s_family = False
#     s_single = False

#     d_family = False
#     d_single = False

#     for cell in part2_json_content[0]['cells']:
#         if cell["cell_type"]  == "code":
#             if "4.3" in cell['source']:
#                 if 10000 < int(cell['outputs'][0]['data']['text/plain']) < 444874:
#                     s_single = True 
#             if "4.4" in cell['source']:
#                 if 500 < int(cell['outputs'][0]['data']['text/plain']) < 2493:
#                     s_family = True
#             if "4.5" in cell['source']:
#                 d_single = int(cell['outputs'][0]['data']['text/plain']) == 444874
#             if "4.6" in cell['source']:
#                 d_family = int(cell['outputs'][0]['data']['text/plain']) == 2493
#     if s_family and s_single and d_family and d_single:
#         return None
#     return f"FAILED (0/10): Incorrect counts marked false -- singe.csv \"single\" line count {s_single}, singe.csv \"family\" line count {s_family} \
#             double.csv \"single\" line count {d_single}, double.csv \"family\" line count {d_family}"


if __name__ == "__main__":
    tester_main()
    cleanup()
