import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
import os
import time
import traceback
import argparse

output_dir_name = "autograder_result"


class PartExecutor(ExecutePreprocessor):

    def record_pause_points(self, pause_comments):
        pause_comments = pause_comments.strip()
        if len(pause_comments) == 0:
            return

        # Determine the cells to pause on
        pause_points = pause_comments.split(",")
        self.pause_text = []
        for pause_point in pause_points:
            pause_point = pause_point.strip()
            if len(pause_point) > 0:
                self.pause_text.append("#q" + str(pause_point))

        print("Will pause after execution of cells containing comments",
              self.pause_text)

    def preprocess_cell(self, cell, resources, cell_index):
        global output_dir_name

        print("Running cell", cell_index)

        # Run the cell
        return_value = cell, resources
        try:
            return_value = super().preprocess_cell(cell, resources, cell_index)
        except Exception as e:
            print("Failed to run cell due to error", traceback.format_exc())

        # Determine if a pause should occur
        if "source" in cell and hasattr(self, "pause_text"):
            cell_contents = cell["source"]

            # Determine if we should pause or not
            should_pause, pause_file_name = False, None
            for expected_txt in self.pause_text:
                if expected_txt in cell_contents:
                    should_pause = True
                    pause_file_name = expected_txt.replace("#", "") + ".cell"
                    break

            if should_pause:
                # Write the file
                pause_file_path = os.path.join(
                    output_dir_name, pause_file_name)

                print("Wrote file to", pause_file_path)
                with open(pause_file_path, "w+") as writer:
                    pass

                print("Waiting for file", pause_file_path, "to be removed")
                while os.path.exists(pause_file_path):
                    time.sleep(1)

                print("File", pause_file_path, "has been removed")

        return return_value


def read_args():
    parser = argparse.ArgumentParser(
        description='Pausable Jupyter Notebook Runner')
    parser.add_argument('notebook_file', type=str, default="",
                        help='Path to the Jupyter Notebook file')

    parser.add_argument(
        '--pauses', type=str, help='Comma-separated list of pause intervals', default="")

    args = parser.parse_args()
    if not os.path.exists(args.notebook_file):
        parser.error(f"Couldn't find file {args.notebook_file}")

    return args


def main(cell_timeout=120):
    global output_dir_name

    # Read the notebook
    args = read_args()
    os.makedirs(output_dir_name, exist_ok=True)
    save_path = os.path.join(output_dir_name, "result.ipynb")

    try:
        # Create a preprocessor to execute those cells
        notebook = nbformat.read(args.notebook_file, as_version=4)
        notebook_executor = PartExecutor(
            timeout=cell_timeout, kernel_name='python3', allow_errors=True)
        notebook_executor.record_pause_points(args.pauses)
        notebook_executor.preprocess(notebook)
        nbformat.write(notebook, save_path)
    except Exception as e:
        print(f"Failed to run notebook due to error {traceback.format_exc()}")

        # Write an empty file so that autograder stops
        with open(save_path, 'w+') as writer:
            pass


if __name__ == "__main__":
    main()
