import os, datetime, json, time, re
import subprocess
from subprocess import check_output
from subprocess import Popen, PIPE
from pathlib import Path
from io import StringIO
import argparse
import docker
import pandas as pd
import sys
from tester import init, test, tester_main, debug
import traceback
# key=num, val=answer (as string)
ANSWERS = {}




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
    docker_reset()

    check_output("docker build . -f hdfs.Dockerfile -t p4-hdfs", shell=True)
    check_output("docker build . -f namenode.Dockerfile -t p4-nn", shell=True)
    check_output("docker build . -f datanode.Dockerfile -t p4-dn", shell=True)
    check_output("docker build . -f notebook.Dockerfile -t p4-nb", shell=True)

    # Start them using docker-compose up
    if debug:
        print("Starting all the containers")
    std_out, _ = run_command("docker compose up -d", timeout_val = startup_timeout, debug = debug)

    # Get the notebook container
    std_out, _ = run_command("docker ps", timeout_val = command_timeout)
  
    specs_df = pd.read_csv(StringIO(std_out), sep='\s{2,}', engine='python', header=0)
    specs_df = specs_df[specs_df["PORTS"].str.contains("5000->5000")]
    container_name = specs_df.iloc[0]["NAMES"]
    if debug:
        print("Got notebook container of", container_name)

    return container_name
   

def docker_reset():
    try:
        subprocess.run(["docker compose kill; docker compose rm -f"], shell=True)
        subprocess.run(["docker", "rmi", "-f", "p4-nb"], check=True, shell=False)
        subprocess.run(["docker", "rmi", "-f", "p4-nn"], check=True, shell=False)
        subprocess.run(["docker", "rmi", "-f", "p4-dn"], check=True, shell=False)
        subprocess.run(["docker", "rmi", "-f", "p4-hdfs"], check=True, shell=False)

        result = subprocess.run(["docker", "container", "ls", ], capture_output = True, check=True, shell=False)
        if result.stdout.decode('utf-8').count("\n") > 1:
            subprocess.run(["docker stop $(docker ps -q)" ], check=True, shell=True)
    except Exception as ex:
        pass


def run_student_code():
    container_name = perform_startup(debug=True)
    file_dir = os.path.abspath(__file__)
    tester_dir = os.path.dirname(file_dir)
    path,_ = run_command("pwd")
    
    
    # for a later test

    print("\n" + "="*70)
    print("Waiting for HDFS cluster to stabilize... this may take a while")
    print("="*70)
    cmd = f"docker exec {container_name} hdfs dfsadmin -fs hdfs://boss:9000 -report"
    print(cmd)    
    for i in range(300):
        try:
            output = check_output(cmd, shell=True)
            m = re.search(r"Live datanodes \((\d+)\)", str(output, "utf-8"))
            if not m:
                print("report didn't describe live datanodes")
            else:
                count = int(m.group(1))
                print(f"found {count} live DataNodes")
                if count >= 2:
                    print("cluster is ready")
                    break
        except subprocess.CalledProcessError as e:
            print("couldn't get report from NameNode")
        time.sleep(1)

    print("\n" + "="*70)
    print("Running p4a.ipynb notebook... this will take a while")
    print("="*70)
    cmd = (f"docker exec {container_name} sh -c '" +
                "export CLASSPATH=`$HADOOP_HOME/bin/hdfs classpath --glob` && " +
                "python3 -m nbconvert --execute --to notebook " +
                "nb/p4a.ipynb --output tester-p4a.ipynb'")
    print(cmd)
    check_output(cmd, shell=True)

    print("\n" + "="*70)
    print("Killing worker 1")
    print("="*70)
    client = docker.from_env()
    containers = client.containers.list()
    for container in containers:
        if "dn-1" in container.name:
            print(f"stop {container.name}")
            container.stop()
            break
    else:
         raise Exception("could not find worker to kill")   

    print("\n" + "="*70)
    print("Waiting for NameNode to detect DataNode is dead... this may take a while")
    print("="*70)
    cmd = f"docker exec {container_name} hdfs dfsadmin -fs hdfs://boss:9000 -report"
    print(cmd)    
    for i in range(60):
        try:
            output = check_output(cmd, shell=True)
            m = re.search(r"Live datanodes \((\d+)\)", str(output, "utf-8"))
            if not m:
                print("report didn't describe live datanodes")
            else:
                count = int(m.group(1))
                print(f"NameNode thinks there are {count} live DataNodes")
                if count < 2:
                    print("DataNode death detected by NameNode")
                    break
        except subprocess.CalledProcessError as e:
            print("couldn't get report from NameNode")
        time.sleep(5)

    print("\n" + "="*70)
    print("Running p4b.ipynb notebook... this will take a while")
    print("="*70)
    try:
        cmd = (f"docker exec {container_name} sh -c '" +
                    "export CLASSPATH=`$HADOOP_HOME/bin/hdfs classpath --glob` && " +
                    "python3 -m nbconvert --execute --to notebook " +
                    "nb/p4b.ipynb --output tester-p4b.ipynb'")
        print(cmd)
        check_output(cmd, shell=True)
    except Exception as e:
        print("An exception occurred while executing p4b.ipynb:", e)
        traceback.print_exc()


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

            for output in cell["outputs"]:
                if output.get("output_type") == "execute_result":
                    answers[qnum] = "\n".join(output["data"]["text/plain"])
                if output.get("output_type") == "stream":
                    if not qnum in answers:
                        answers[qnum] = "\n".join(output["text"])

    return answers

def extract_student_answers():
    path = Path("nb") / "tester-p4a.ipynb"
    if os.path.exists(path):
        ANSWERS.update(extract_notebook_answers(path))

    path = Path("nb") / "tester-p4b.ipynb"
    if os.path.exists(path):
        ANSWERS.update(extract_notebook_answers(path))

def diagnostic_checks():

    out, _ = run_command("cat /etc/os-release")
    if "VERSION=\"22.04.3 LTS (Jammy Jellyfish)\"" not in out and "Ubuntu 22.04.3 LTS" not in out:
        print("WARNING - you should be using UBUNTU 22.04.3 LTS (Jammy Jellyfish)")

    out, _ = run_command("lscpu")
    if "x86_64" not in out:
        print("WARNING - are you using an x86 Architecture")
    try:
        out, _ = run_command("wget -q -O - --header Metadata-Flavor:Google metadata/computeMetadata/v1/instance/machine-type")
        if "e2-medium" not in out:
            print("WARNING - did you switch to an E-2 Medium")
    except:
        pass
    
    
    

@debug
def create_debug_dir():
    file_dir = os.path.abspath(__file__)
    tester_dir = os.path.dirname(file_dir)
    print("tester_dir: ", file_dir)
    
    target = f"{tester_dir}/notebooks_from_test/"
    print("target: ", target)
    check_output(f"mkdir {target} && cp nb/tester-p4a.ipynb {target} && cp nb/tester-p4b.ipynb {target}", shell=True)

@init
def init(verbose = False):
    run_student_code()
    extract_student_answers()
    

def check_has_answer(num):
    if not num in ANSWERS:
        raise Exception(f"Answer to question {num} not found")
    
@test(points=10)
def q1():
    check_has_answer(1)
    if not "Live datanodes (2):" in ANSWERS[1]:
        return "Output does not indicate 2 live datanodes"

@test(points=10)
def q2():
    check_has_answer(2)
    single = False
    double = False
    for line in ANSWERS[2].split("\n"):
        if "166" in line and "333" not in line and "single" in line:
            single = True
        if "166" in line and "333" in line and "double" in line:
            double = True
    if not single:
        return "Expected a line like '166.8 M  166.8 M  hdfs://boss:9000/single.csv'"
    if not double:
        return "Expected a line like '166.8 M  333.7 M  hdfs://boss:9000/double.csv'"

@test(points=10)
def q3():
    check_has_answer(3)
    d = json.loads(ANSWERS[3])
    assert "FileStatus" in d
    assert "single.csv" in ANSWERS[3]
    assert "double.csv" in ANSWERS[3]

@test(points=10)
def q3():
    check_has_answer(3)
    # single quote => double quote turns Python dict into JSON
    d = json.loads(ANSWERS[3].replace("'", '"'))
    assert "FileStatus" in d
    assert d["FileStatus"]["blockSize"] == 1048576
    assert d["FileStatus"]["length"] == 174944099

@test(points=10)
def q4():
    check_has_answer(4)
    assert ":9864/webhdfs/v1/single.csv?op=OPEN&namenoderpcaddress=boss:9000&offset=0" in ANSWERS[4]

@test(points=10)
def q5():
    check_has_answer(5)
    # single quote => double quote turns Python dict into JSON
    d = json.loads(ANSWERS[5].replace("'", '"'))
    assert len(d) == 2
    assert min(d.values()) > 1
    assert sum(d.values()) == 167

@test(points=10)
def q6():
    check_has_answer(6)
    assert ANSWERS[6] == "b'activity_y'"

@test(points=10)
def q7():
    check_has_answer(7)
    assert int(ANSWERS[7]) == 444874

@test(points=10)
def q8():
    check_has_answer(8)
    if not "Live datanodes (1):" in ANSWERS[8]:
        return "Output does not indicate 2 live datanodes"

@test(points=10)
def q9():
    check_has_answer(9)
    # single quote => double quote turns Python dict into JSON
    d = json.loads(ANSWERS[9].replace("'", '"'))
    assert len(d) == 2
    assert min(d.values()) > 1
    assert sum(d.values()) == 167
    assert "lost" in d

@test(points=10)
def q10():
    check_has_answer(10)
    count = int(ANSWERS[10])
    total = 444874
    if count < 0 or count > total:
        raise Exception(f"count={count} is outside range of 0 to {444874}")
    if count == 0:
        raise Exception(f"count was 0, suggesting every data block was lost (not likely)")
    if count == total:
        raise Exception(f"count was {total}, suggesting no data blocks were lost (not likely)")

if __name__ == "__main__":
    diagnostic_checks()
    tester_main()
    docker_reset()
