from tester import *
import os
import time
import p6_scripts.p6_init_script as p6_init
import nbformat

all3_notebook_content = None
all2_notebook_content = None
def get_cell_idx(notebook, expected_cell_txt):
    for cell_idx in range(0, len(notebook.cells)):
        cell_values = notebook.cells[cell_idx]
        if expected_cell_txt in cell_values["source"]:
            return cell_idx
    
    return None

def get_cell_output(notebook, expected_cell_txt):
    # Get the cell idx
    cell_idx = get_cell_idx(notebook, expected_cell_txt)
    if cell_idx is None:
        raise Exception("Failed to get cell with content", expected_cell_txt)

    # Read the output
    cell_output = notebook.cells[cell_idx]["outputs"]
    if len(cell_output) == 0:
        return None, None

    # Make sure there are not errors
    std_out, std_err = "", ""
    for output in cell_output:
        if output["name"] == "stderr":
            std_err += output["text"]
        else:
            std_out += output["text"]
    
    return std_out.strip(), std_err.strip()

@init
def init(verbose = False):
    global all3_notebook_content, all2_notebook_content

    # Switch working directory
    file_dir = os.path.abspath(__file__)
    tester_dir = os.path.dirname(file_dir)
    notebook_runner = os.path.join(tester_dir, "p6_scripts", "notebook_runner.py")

    # Run the notebook
    result_paths = p6_init.run_notebooks(notebook_runner, debug = verbose)
    if result_paths is None:
        raise Exception("Couldn't sucessfully rerun the notebooks")
    
    # Read the notebooks
    all3_notebook_path, all2_notebook_path = result_paths
    if verbose:
        print("All 3 notebook saved to", all3_notebook_path)
        print("Only 2 notebook saved to", all2_notebook_path)
        
    all3_notebook_content = nbformat.read(all3_notebook_path, as_version = 4)
    all2_notebook_content = nbformat.read(all2_notebook_path, as_version = 4)

def cell_has_text(notebook, cell_search_txt, expected_txt, std_err_allowed = False):
    cell_search_txt = cell_search_txt.strip()
    expected_txt = expected_txt.strip()

    stdout, stderr = get_cell_output(notebook, cell_search_txt)
    if stdout is None or stderr is None:
        return "Have no output for cell with text " + cell_search_txt

    if not std_err_allowed and len(stderr) > 0:
        return "Cell with text " + cell_search_txt + " has stderr of " + stderr
    
    if expected_txt not in stdout:
        return "Couldn't find " + expected_txt + " in stdout of " + stdout
    
    return None

@test(points = 1, timeout = 10)
def q1_keyspace_creation():
    return cell_has_text(all3_notebook_content, "# Q1 Ans", "CREATE KEYSPACE weather")

@test(points = 1, timeout = 10)
def q1_replication_factor():
    return cell_has_text(all3_notebook_content, "# Q1 Ans", "'replication_factor': '3'")

@test(points = 1, timeout = 10)
def q1_station_record_type():
    return cell_has_text(all3_notebook_content, "# Q1 Ans", "CREATE TYPE weather.station_record")

@test(points = 0.5, timeout = 10)
def q1_station_record_min_field():
    return cell_has_text(all3_notebook_content, "# Q1 Ans", "tmin int")

@test(points = 0.5, timeout = 10)
def q1_station_record_max_field():
    return cell_has_text(all3_notebook_content, "# Q1 Ans", "tmax int")

@test(points = 1, timeout = 10)
def q1_stations_table():
    return cell_has_text(all3_notebook_content, "# Q1 Ans", "CREATE TABLE weather.stations")

@test(points = 1, timeout = 10)
def q1_stations_id_field():
    return cell_has_text(all3_notebook_content, "# Q1 Ans", "id text")

@test(points = 1, timeout = 10)
def q1_stations_date_field():
    return cell_has_text(all3_notebook_content, "# Q1 Ans", "date date")

@test(points = 1, timeout = 10)
def q1_stations_record_field():
    return cell_has_text(all3_notebook_content, "# Q1 Ans", "record frozen<station_record>")

@test(points = 1, timeout = 10)
def q1_stations_primary_key():
    return cell_has_text(all3_notebook_content, "# Q1 Ans", "PRIMARY KEY (id, date")

@test(points = 1, timeout = 10)
def q2_row_token():
    return cell_has_text(all3_notebook_content, "# Q2 Ans", "Row token: -9014250178872933741")

@test(points = 1, timeout = 10)
def q2_vnode_token():
    return cell_has_text(all3_notebook_content, "# Q2 Ans", "Vnode token: -8")

@test(points = 1, timeout = 10)
def simulate_USW00014837():
    return cell_has_text(all3_notebook_content, "# gRPC client runner", "max temp for USW00014837 is 356")

@test(points = 1, timeout = 10)
def simulate_USR0000WDDG():
    return cell_has_text(all3_notebook_content, "# gRPC client runner", "max temp for USR0000WDDG is 344")

@test(points = 1, timeout = 10)
def simulate_USW00014898():
    return cell_has_text(all3_notebook_content, "# gRPC client runner", "max temp for USW00014898 is 356")

@test(points = 1, timeout = 10)
def simulate_USW00014839():
    return cell_has_text(all3_notebook_content, "# gRPC client runner", "max temp for USW00014839 is 378")

@test(points = 1, timeout = 10)
def q4_coor():
    return cell_has_text(all3_notebook_content, "# TODO: Q4", "Correlation is 0.98", std_err_allowed = True)

@test(points = 1, timeout = 10)
def q5_coor():
    return cell_has_text(all2_notebook_content, "# TODO: Q5", "StationMax call for USW00014837 returned error", std_err_allowed = True)

@test(points = 1, timeout = 10)
def q6_coor():
    return cell_has_text(all2_notebook_content, "# TODO: Q6", "Failed to write data for 0 date(s) of USC00477115", std_err_allowed = True)

@test(points = 1, timeout = 10)
def q7_before():
    return cell_has_text(all2_notebook_content, "# TODO: Q7", "Before refresh: 1460", std_err_allowed = True)

@test(points = 1, timeout = 10)
def q7_after():
    return cell_has_text(all2_notebook_content, "# TODO: Q7", "After refresh: 1825", std_err_allowed = True)

if __name__ == "__main__":
    tester_main()