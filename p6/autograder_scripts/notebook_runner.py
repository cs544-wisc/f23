import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
from nbconvert.exporters import NotebookExporter
from traitlets.config import Config
from argparse import ArgumentParser
import json

def read_args():
    parser = ArgumentParser()
    parser.add_argument('-p', '--parts', nargs='+', default=[])
    return parser.parse_args()

class PartExecutor(ExecutePreprocessor):

    def set_valid_range(self, ranges):
        self.valid_ranges = ranges
        self.valid_ranges.sort(key = lambda x : x[0])
    
    def preprocess_cell(self, cell, resources, cell_index):
        should_execute_cell = False
        for range in self.valid_ranges:
            if cell_index >= range[0] and cell_index <= range[1]:
                should_execute_cell = True
                break

        if not should_execute_cell:
            # Don't execute this cell in output
            return cell, resources

        return super().preprocess_cell(cell, resources, cell_index)

def get_cell_idx_containing_txt(notebook, texts):
    num_cells = len(notebook.cells)
    for idx in range(num_cells):
        curr_src = notebook.cells[idx]["source"]

        # Determine if the cell contains the text
        contains_all = True
        for text in texts:
            if text not in curr_src:
                contains_all = False
                break
        
        if contains_all:
            return idx
    
    return num_cells - 1

def get_cell_range(notebook, part):
    part_num = int(part.split("_")[1].strip())
    start_txt = "## Part " + str(part_num)
    end_txt = "## Part " + str(part_num + 1)

    return [get_cell_idx_containing_txt(notebook, [start_txt]) + 1, get_cell_idx_containing_txt(notebook, [end_txt]) - 1]

def get_cells_to_execute(notebook, args):
    setup_end =  get_cell_idx_containing_txt(notebook, "## Part 1")
    last_cell_idx = len(notebook.cells) - 1
    default_cells = [[0, setup_end], [last_cell_idx, last_cell_idx]]

    for cell_name in args.parts:
        default_cells.append(get_cell_range(notebook, cell_name))
    
    return default_cells

# Tuple format: Part idx, points, expected text
expected_checks = [(0, 1.0, "CREATE KEYSPACE weather"),
                    (0, 1.0, "'replication_factor': '3'"),
                    (1, 1.0, "CREATE TYPE weather.station_record "),
                    (1, 1.0, "tmin int"),
                    (1, 1.0, "tmax int"),
                    (2, 1.0, "CREATE TABLE weather.stations"),
                    (2, 1.0, "id text"),
                    (2, 1.0, "date date"),
                    (2, 1.0, "name text static"),
                    (2, 1.0, "record frozen<station_record>"),
                    (2, 1.0, "PRIMARY KEY (id, date)")
                    ]
def check_describe_commits(notebook, cell_ranges):
    describe_cell_idx = cell_ranges[0][1]
    cell_outputs = notebook.cells[describe_cell_idx]["outputs"]
    cell_txt = cell_outputs[0]["text"]
    
    # Split into parts
    parts = cell_txt.split(";")
    parts = [parts[i].strip() for i in range(len(parts))]
    output_lines = []

    # See what all is missing
    for part_idx, pts, expected_txt in expected_checks:
        part_to_check = parts[part_idx]
        if expected_txt not in part_to_check:
            line = "-" + str(pts) + ": Missing text " + expected_txt + " in cell output"
            output_lines.append(line) 

    output_msg = "\n".join(output_lines)
    if len(output_lines) > 0:
        output_msg += "\nScores based on cell output of:\n" + cell_txt
    else:
        output_msg = "All checks passed!"

    return output_msg

def check_cells_for_error(notebook, cell_ranges):
    for curr_range in cell_ranges: # Iterate through the range
        start_val, end_val = curr_range[0], curr_range[1]
        for index in range(start_val, end_val + 1): # Iterate through the idx in the range
            # Make sure that the cell has outputs
            curr_cell = notebook.cells[index]
            if "outputs" not in curr_cell:
                continue
            
            for output in curr_cell["outputs"]:
                if output["output_type"] == "error":
                    # Write the error message
                    error_val = output["evalue"]
                    traceback_lines = output["traceback"]
                    traceback = "\n".join(traceback_lines)

                    err_msg = "Encountered error running cell " + str(index) + ":\n"
                    err_msg += error_val + "\n" + traceback
                    return err_msg
    
    return None

def runner(timeout = 1000):
    # Read the notebook
    notebook_path = "/notebooks/p6.ipynb"
    notebook = nbformat.read(notebook_path, as_version=4)

    # Get the cells to run
    args = read_args()
    cells_to_run = get_cells_to_execute(notebook, args)
    print("Running cellls", cells_to_run, "of", notebook_path)

    # Create a preprocessor to execute those cells
    notebook_executor = PartExecutor(timeout = timeout, kernel_name='python3')
    notebook_executor.set_valid_range(cells_to_run)

    # Write code to run those cells and export the result
    try:
        metadata = {'metadata': {'path': '/notebooks/'}}
        notebook_executor.preprocess(notebook, resources = metadata)
    except:
        pass
    
    # Write the output
    file_name = ",".join(args.parts) + ",result.ipynb"
    print("Writing result to file", file_name)
    nbformat.write(notebook, file_name)

if __name__ == "__main__":
    runner()