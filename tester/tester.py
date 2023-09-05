import json, argparse
import os, traceback, shutil
import tempfile
import multiprocessing

VERBOSE = False
TEST_DIR = None

# full list of tests
INIT = None
TESTS = {}
CLEANUP = None

# dataclass for storing test object info
class _unit_test():
    def __init__(self, func, points, timeout, desc):
        self.func = func
        self.points = points
        self.timeout = timeout
        self.desc = desc

    def run(self, ret):
        points = 0

        try:
            result = self.func()
            if not result:
                points = self.points
                result = f"PASS ({self.points}/{self.points})"
        except Exception as e:
            result = traceback.format_exception(e)
            print(f"Exception in {self.func.__name__}:\n")
            print("\n".join(result) + "\n")

        ret.send((points, result))

# init decorator
def init(init_func):
    global INIT
    INIT = init_func

# test decorator
def test(points, timeout=None, desc=""):
    def wrapper(test_func):
        TESTS[test_func.__name__] = _unit_test(test_func, points, timeout, desc)
    return wrapper

# cleanup decorator
def cleanup(cleanup_func):
    global CLEANUP
    CLEANUP = cleanup_func

# lists all tests
def list_tests():
    for (test_name, test) in TESTS.items():
        print(f"{test_name}({test.points}): {test.desc}")

# run all tests
def run_tests():
    results = {
        "score": 0,
        "full_score": 0,
    }

    for test_name, test in TESTS.items():
        results["full_score"] += test.points

        ret_send, ret_recv = multiprocessing.Pipe()
        proc = multiprocessing.Process(target=test.run, args=(ret_send, ))
        proc.start()
        proc.join(test.timeout)
        if proc.is_alive():
            proc.terminate()
            points = 0
            result = "Timeout"
        else:
            (points, result) = ret_recv.recv()
            
        results["score"] += points
        results[test_name] = result

    assert(results["score"] <= results["full_score"])
    if VERBOSE:
        print("Saving JSON of", results)

    return results

# save the result as json
def save_results(results):
    output_file = f"{TEST_DIR}/test_result.json"
    with open(output_file, "w") as f:
        json.dump(results, f, indent = 4)
    print("Test results saved to path", output_file)

def tester_main():
    global VERBOSE, TEST_DIR

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--dir", type=str, default = ".", help="path to your repository")
    parser.add_argument("-t", "--tmp_dir", type=str, default = "", help="path to the temp dir")
    parser.add_argument("-l", "--list", action="store_true", help="list all tests")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    if args.list:
        list_tests()
        return

    VERBOSE = args.verbose

    test_dir = args.dir
    if not os.path.isdir(test_dir):
        print("invalid path")
        return
    TEST_DIR = os.path.abspath(test_dir)

    # make a copy of the code ensuring that we have the same dir name
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_parent_dir = temp_dir
        if len(args.tmp_dir) > 0:
            temp_parent_dir = args.tmp_dir
        temp_path = os.path.join(temp_parent_dir, os.path.basename(test_dir))
        
        # Ensure we copy into an empty dir
        if os.path.exists(temp_path):
            shutil.rmtree(temp_path)
        os.makedirs(temp_path, exist_ok = True)
        
        # Copy the student code to the temp dir
        shutil.copytree(src=TEST_DIR, dst = temp_path, dirs_exist_ok=True)
        os.chdir(temp_path)

        # run init
        if INIT:
            INIT(verbose = VERBOSE)
        
        # run tests
        results = run_tests()
        save_results(results)

        # run cleanup
        if CLEANUP:
            CLEANUP()
        
        if len(args.tmp_dir) == 0:
            shutil.rmtree(temp_path)