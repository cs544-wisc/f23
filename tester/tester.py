import json, argparse
import os, traceback, shutil

import multiprocessing

VERBOSE = False

TMP_DIR = "/tmp/_cs544_tester_directory"
TEST_DIR = None

# full list of tests
TESTS = {}

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
            print("\n".join(self.result) + "\n")

        ret.send((points, result))

# test annotator
def test(points, timeout=None, desc=""):
    def wrapper(test_func):
        TESTS[test_func.__name__] = _unit_test(test_func, points, timeout, desc)
    return wrapper

# lists all tests
def list_tests():
    for (test_name, test) in TESTS.items():
        print(f"{test_name}({test.points}): {test.desc}")

# run all tests
def run_tests():

    # make a copy of the code
    shutil.copytree(src=TEST_DIR, dst=TMP_DIR, dirs_exist_ok=True)
    os.chdir(TMP_DIR)

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
        print(results)
    return results

# save the result as json
def save_results(results):
    output_file = f"{TEST_DIR}/test.json"
    with open(output_file, "w") as f:
        json.dump(results, f)


def tester_main():
    global VERBOSE, TEST_DIR

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--dir", type=str, default = ".", help="path to your repository")
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

    results = run_tests()
    save_results(results)
