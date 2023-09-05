import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
from nbconvert.exporters import NotebookExporter
from traitlets.config import Config
from argparse import ArgumentParser
import json

import sys

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

def runner(timeout = 1000):
    # Read the notebook
    notebook_path = "/notebooks/p6.ipynb"
    notebook = nbformat.read(notebook_path, as_version=4)

    # Get the cells to run
    args = read_args()
    cells_to_run = get_cells_to_execute(notebook, args)
    print("Running cellls", cells_to_run, "of", notebook_path, file = sys.stderr)

    # Create a preprocessor to execute those cells
    notebook_executor = PartExecutor(timeout = timeout, kernel_name='python3')
    notebook_executor.set_valid_range(cells_to_run)

    # Write code to run those cells and export the result
    try:
        metadata = {'metadata': {'path': '/notebooks/'}}
        notebook_executor.preprocess(notebook, resources = metadata)
    except Exception as e:
        print("Failed to run notebook due to error:", file = sys.stderr)
        print(e, file = sys.stderr)
    
    # Write the output
    file_name = ",".join(args.parts) + ",result.ipynb"
    print("Writing result to file", file_name, file = sys.stderr)
    nbformat.write(notebook, file_name)

if __name__ == "__main__":
    runner()