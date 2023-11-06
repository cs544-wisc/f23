import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
from nbconvert.exporters import NotebookExporter
import os
import time
import traceback
import subprocess 

output_dir_name = "autograder_result"

def wait_for_all_three_up():
    all_three_up = False
    command_to_run = "nodetool status"
    
    while not all_three_up:
        time.sleep(5) # Wait a little bit
        
        environment = os.environ.copy()
        result = subprocess.run(command_to_run, capture_output = True, text = True, shell = True, env = environment) 
        print("Result of nodetool status is", result.stdout)       
        all_three_up = result.stdout.count("UN") >= 3
    
    time.sleep(10) 

def startup_server():
    global output_dir_name
    
    # Inform the autograder to startup the server
    start_server_file = os.path.join(output_dir_name, "start_server.record")
    print("Wrote file", start_server_file)
    with open(start_server_file, "w+") as writer:
        pass
    
    # Wait for the server to start up
    server_start_file = os.path.join(output_dir_name, "server_started.record")
    print("Checking for file", start_server_file)
    while not os.path.exists(server_start_file):
        time.sleep(5)

def kill_one_node():
    global output_dir_name
    
    # Inform the autograder to kill one of the nodes
    kill_node_file = os.path.join(output_dir_name, "kill_node.record")
    with open(kill_node_file, "w+") as writer:
        pass
    print("Wrote file", kill_node_file)
    
    node_killed_file = os.path.join(output_dir_name, "node_killed.record")
    print("Checking for file", node_killed_file)
    while not os.path.exists(node_killed_file):
        time.sleep(5)
    
    one_node_dead = False
    command_to_run = "nodetool status"
    while not one_node_dead:
        time.sleep(5) # Wait a little bit
        
        environment = os.environ.copy()
        result = subprocess.run(command_to_run, capture_output = True, text = True, shell = True, env = environment)
        print("Result of nodetool status is", result.stdout)        
        one_node_dead = result.stdout.count("DN") >= 1
    
    time.sleep(10)

class PartExecutor(ExecutePreprocessor):
    
    def preprocess_cell(self, cell, resources, cell_index):
        print("Running cell", cell_index, "with source", cell["source"])
        
        # Run the cell
        return_value = cell, resources
        try:        
            return_value = super().preprocess_cell(cell, resources, cell_index)
        except Exception as e:
            print("Failed to run cell due to error", traceback.format_exc())
                
        if "source" in cell:
            cell_contents = cell["source"]
            
            # Start up the server for Part 2
            if "#q4" in cell_contents:
                startup_server()
            
            # Kill one of the nodes for Part 4
            if "#q7" in cell_contents:
                kill_one_node()
                
        return return_value

def verify_notebook_content(notebook):
    expected_comments = ["#q" + str(i) for i in range(1, 11)]
    

def main(cell_timeout = 120):
    global output_dir_name
    
    # Read the notebook
    os.makedirs(output_dir_name, exist_ok = True)
    notebook_path = "p6.ipynb"
    notebook = nbformat.read(notebook_path, as_version=4)
    
    # Wait for nodetool ring to be up
    wait_for_all_three_up()
    
    # Create a preprocessor to execute those cells
    notebook_executor = PartExecutor(timeout = cell_timeout, kernel_name='python3', allow_errors = True)
    
    try:
        notebook_executor.preprocess(notebook)
    except Exception as e:
        print("Failed to run notebook due to error", traceback.format_exc())
    
    save_path = os.path.join(output_dir_name, "result.ipynb")
    print("Writing result to file", save_path)
    nbformat.write(notebook, save_path)

if __name__ == "__main__":
    main()