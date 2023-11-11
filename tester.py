from collections import OrderedDict
import json
import argparse
import os
import traceback
import shutil

import multiprocessing

multiprocessing.set_start_method("fork")

VERBOSE = False

TMP_DIR = "/tmp/_cs544_tester_directory"
TEST_DIR = None

# full list of tests
INIT = None
TESTS = OrderedDict()
CLEANUP = None
DEBUG = None
GO_FOR_DEBUG = None

# dataclass for storing test object info


class _unit_test:
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
    return init_func


# test decorator
def test(points, timeout=None, desc=""):
    def wrapper(test_func):
        TESTS[test_func.__name__] = _unit_test(
            test_func, points, timeout, desc)

    return wrapper

# debug dir decorator


def debug(debug_func):
    global DEBUG
    DEBUG = debug_func
    return debug_func

# cleanup decorator


def cleanup(cleanup_func):
    global CLEANUP
    CLEANUP = cleanup_func
    return cleanup_func


# lists all tests
def list_tests():
    for test_name, test in TESTS.items():
        print(f"{test_name}({test.points}): {test.desc}")


# run all tests
def run_tests():
    results = {
        "score": 0,
        "full_score": 0,
        "tests": {},
    }

    for test_name, test in TESTS.items():
        if VERBOSE:
            print(f"===== Running Test {test_name} =====")

        results["full_score"] += test.points

        ret_send, ret_recv = multiprocessing.Pipe()
        proc = multiprocessing.Process(target=test.run, args=(ret_send,))
        proc.start()
        proc.join(test.timeout)
        if proc.is_alive():
            proc.terminate()
            points = 0
            result = "Timeout"
        else:
            (points, result) = ret_recv.recv()

        if VERBOSE:
            print(result)
        results["score"] += points
        results["tests"][test_name] = result

    assert results["score"] <= results["full_score"]
    if VERBOSE:
        print("===== Final Score =====")
        print(json.dumps(results, indent=4))
        print("=======================")
    # and results['score'] != results["full_score"]
    if DEBUG and GO_FOR_DEBUG:
        DEBUG()
    # cleanup code after all tests run
    shutil.rmtree(TMP_DIR, ignore_errors=True)
    return results


# save the result as json
def save_results(results):
    output_file = f"{TEST_DIR}/test.json"
    print(f"Output written to: {output_file}")
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)


def tester_main():
    global VERBOSE, TEST_DIR, GO_FOR_DEBUG

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d", "--dir", type=str, default=".", help="path to your repository"
    )
    parser.add_argument("-l", "--list", action="store_true",
                        help="list all tests")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-g", "--debug", action="store_true",
                        help="create a debug directory with the files used while testing")
    parser.add_argument("-e", "--existing", default=None,
                        help="run the autograder on an existing notebook")
    args = parser.parse_args()

    if args.list:
        list_tests()
        return

    VERBOSE = args.verbose
    GO_FOR_DEBUG = args.debug
    test_dir = args.dir
    if not os.path.isdir(test_dir):
        print("invalid path")
        return
    TEST_DIR = os.path.abspath(test_dir)

    # make a copy of the code
    def ignore(_dir_name, _dir_content): return [
        ".git", ".github", "__pycache__", ".gitignore", "*.pyc"]
    shutil.copytree(src=TEST_DIR, dst=TMP_DIR,
                    dirs_exist_ok=True, ignore=ignore)

    if args.existing is None and CLEANUP:
        CLEANUP()

    os.chdir(TMP_DIR)

    # run init
    if INIT:
        INIT(existing_file=args.existing)

    # run tests
    results = run_tests()
    save_results(results)

    # run cleanup
    if args.existing is None and CLEANUP:
        CLEANUP()
