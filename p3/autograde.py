import json
import re
from functools import wraps
from os import environ
from pathlib import Path
from subprocess import DEVNULL, call, check_output
from sys import stderr
from time import sleep

from tester import cleanup, init, test, tester_main


@cleanup
def _cleanup(*args, **kwargs):
    call(["docker", "stop", "p3_autograde"], stdout=DEVNULL, stderr=DEVNULL)
    call(["docker", "rm", "p3_autograde"], stdout=DEVNULL, stderr=DEVNULL)


@init
def _init(*args, **kwargs):
    _cleanup()


@test(10)
def docker_build_run():
    environment = environ.copy()
    environment["DOCKER_CLI_HINTS"] = "false"
    check_output(["docker", "build", ".", "-t", "p3_autograde"], env=environment)
    check_output(
        [
            "docker",
            "run",
            "--rm",
            "-d",
            "--name",
            "p3_autograde",
            "-w",
            "/autograde",
            "-v",
            ".:/autograde",
            "p3_autograde",
        ]
    )
    sleep(5)  # wait for server to start


@test(0)
def run_docker_autograde():
    check_output(
        [
            "docker",
            "exec",
            "p3_autograde",
            "python3",
            "docker_autograde.py",
        ]
    )
    test_json_output = (Path.cwd() / "test.json").resolve(strict=True)
    test_json_output.rename(test_json_output.with_stem("docker_test"))


def docker_test(test_func):
    @wraps(test_func)
    def wrapper():
        docker_test_json = Path.cwd() / "docker_test.json"
        if not docker_test_json.exists():
            return "FAIL: docker_test.json not found"
        with open(docker_test_json, "r", encoding="utf-8") as json_file:
            json_contents = json.load(json_file)
            if (
                "tests" not in json_contents
                or test_func.__name__ not in json_contents["tests"]
            ):
                fail_str = f"FAIL: {test_func.__name__} not found in docker_test.json"
                print(fail_str, file=stderr)
                return fail_str
            score_value = json_contents["tests"][test_func.__name__]
            if type(score_value) is list:
                for line in score_value:
                    print(line, file=stderr)

                return "".join(score_value)

            if type(score_value) is not str:
                return score_value
            score_match = re.match(r"PASS \((\d+)/(\d+)\)", score_value)
            if score_match is None:
                return score_value

    return wrapper


@test(10)
@docker_test
def protobuf_interface():
    # see docker_autograde.py:protobuf_interface
    pass


@test(10)
@docker_test
def set_coefs():
    # see docker_autograde.py:set_coefs
    pass


@test(10)
@docker_test
def predict():
    # see docker_autograde.py:predict
    pass


@test(10)
@docker_test
def predict_single_call_cache():
    # see docker_autograde.py:predict_single_call_cache
    pass


@test(10)
@docker_test
def predict_full_cache_eviction():
    # see docker_autograde.py:predict_full_cache_eviction
    pass


@test(10)
@docker_test
def set_coefs_cache_invalidation():
    # see docker_autograde.py:set_coefs_cache_invalidation
    pass


@test(10)
@docker_test
def client_workload_1():
    # see docker_autograde.py:client_workload_1
    pass


@test(10)
@docker_test
def client_workload_2():
    # see docker_autograde.py:client_workload_2
    pass


@test(10)
@docker_test
def client_workload_3():
    # see docker_autograde.py:client_workload_3
    pass


if __name__ == "__main__":
    tester_main()
