from tester import *
import os
import time
import p6_scripts.p6_init_script as p6_init
import nbformat

all3_notebook, all2_notebook = None, None

@init
def init(verbose = False):
    global all3_notebook, all2_notebook

    # Switch working directory
    file_dir = os.path.abspath(__file__)
    tester_dir = os.path.dirname(file_dir)
    notebook_runner = os.path.join(tester_dir, "p6_scripts", "notebook_runner.py")

    # Run the notebook
    result_paths = p6_init.run_notebooks(notebook_runner, debug = verbose)
    if result_paths is None:
        raise Exception("Couldn't sucessfully rerun the notebooks")
    
    # Read the notebooks
    notebook_all3_path, notebook_all2_path = result_paths
    print("All 3 path", notebook_all3_path)
    print("All 2 path", notebook_all2_path)

@test(points = 15, timeout = 10)
def all3_exists():
    global all3_notebook

    if all3_notebook is not None:
        return None
    return "Didn't find output for all 3 nodes up"

@test(points = 15, timeout = 10)
def all2_exists():
    global all2_notebook

    if all2_notebook is not None:
        return None
    return "Didn't find output for all 2 nodes up"

@cleanup
def cleanup():
    global all3_notebook, all2_notebook

    all3_notebook = None
    all2_notebook = None

if __name__ == "__main__":
    tester_main()