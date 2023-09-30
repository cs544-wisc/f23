from functools import wraps
import re
from subprocess import check_output

from google.protobuf.descriptor import FieldDescriptor
import grpc
import numpy as np
from modelserver_pb2 import (
    PredictRequest,  # type: ignore
    PredictResponse,  # type: ignore
    SetCoefsRequest,  # type: ignore
    SetCoefsResponse,  # type: ignore
)
from modelserver_pb2_grpc import ModelServerStub

from tester import test, tester_main

PORT = 5440


def with_client():
    def decorator(test_func):
        @wraps(test_func)
        def wrapper():
            addr = f"127.0.0.1:{PORT}"
            channel = grpc.insecure_channel(addr)
            stub = ModelServerStub(channel)
            return test_func(stub)

        return wrapper

    return decorator


def client_workload(coefs, *csv_files):
    def decorator(test_func):
        @wraps(test_func)
        def wrapper():
            coefs_str = ",".join(coefs.astype(str))

            output = (
                check_output(["python3", "client.py", str(PORT), coefs_str, *csv_files])
                .decode("utf-8")
                .splitlines()
            )

            assert len(output) >= 1, f"Expected at least 1 line of output"

            last_line = output[-1]

            try:
                hit_rate = float(last_line)
            except ValueError:
                assert False, f"Expected last line to be a float, but got {last_line}"

            return test_func(hit_rate)

        return wrapper

    return decorator


@test(10)
def protobuf_interface():
    descriptors = {
        SetCoefsRequest.DESCRIPTOR: {
            "coefs": [FieldDescriptor.LABEL_REPEATED, FieldDescriptor.CPPTYPE_FLOAT]
        },
        SetCoefsResponse.DESCRIPTOR: {
            "error": [
                FieldDescriptor.LABEL_OPTIONAL,
                FieldDescriptor.CPPTYPE_STRING,
            ],
        },
        PredictRequest.DESCRIPTOR: {
            "X": [
                FieldDescriptor.LABEL_REPEATED,
                FieldDescriptor.CPPTYPE_FLOAT,
            ],
        },
        PredictResponse.DESCRIPTOR: {
            "y": [
                FieldDescriptor.LABEL_OPTIONAL,
                FieldDescriptor.CPPTYPE_FLOAT,
            ],
            "hit": [
                FieldDescriptor.LABEL_OPTIONAL,
                FieldDescriptor.CPPTYPE_BOOL,
            ],
            "error": [
                FieldDescriptor.LABEL_OPTIONAL,
                FieldDescriptor.CPPTYPE_STRING,
            ],
        },
    }

    for descriptor, fields in descriptors.items():
        assert len(descriptor.fields_by_name) == len(
            fields
        ), f"{descriptor.name} should have {len(fields)} fields, but it has {len(descriptor.fields_by_name)} fields"

        for field_name, field_info in fields.items():
            field = descriptor.fields_by_name.get(field_name)
            assert (
                field is not None
            ), f"{descriptor.name} should have a field named {field_name}"
            assert (
                field.label == field_info[0]
            ), f"{field_name} field should be {field_info[0]}"
            assert (
                field.cpp_type == field_info[1]
            ), f"{field_name} field should be {field_info[1]}"


@test(10)
@with_client()
def set_coefs(stub: ModelServerStub):
    response = stub.SetCoefs(SetCoefsRequest(coefs=[1, 2, 3]))
    assert str(response) == "", response
    response = stub.SetCoefs(SetCoefsRequest(coefs=[2, 3, 4]))
    assert str(response) == "", response


@test(10)
@with_client()
def predict(stub: ModelServerStub):
    stub.SetCoefs(SetCoefsRequest(coefs=[1, 2, 3]))
    response = stub.Predict(PredictRequest(X=[1, 2, 3]))
    assert str(response) == "y: 14\n", response
    stub.SetCoefs(SetCoefsRequest(coefs=[0.70176, 0.27204, 0.11234]))
    response = stub.Predict(PredictRequest(X=[0.92017, 0.92842, 0.6054]))
    assert re.match(r"^y: 0.9663\d+\n$", str(response)), response


@test(10)
@with_client()
def predict_single_call_cache(stub: ModelServerStub):
    stub.SetCoefs(SetCoefsRequest(coefs=[1, 2, 3]))

    # Check one subsequent call is cached
    response = stub.Predict(PredictRequest(X=[1, 2, 3]))
    assert str(response) == "y: 14\n", response
    response = stub.Predict(PredictRequest(X=[1, 2, 3]))
    assert str(response) == "y: 14\nhit: true\n", response


@test(10)
@with_client()
def predict_full_cache_eviction(stub: ModelServerStub):
    stub.SetCoefs(SetCoefsRequest(coefs=[1, 2, 3]))
    stub.Predict(PredictRequest(X=[1, 2, 3]))

    # Fill the cache
    for i in range(10):
        response = stub.Predict(PredictRequest(X=[3, 2, i]))
        output = 1 * 3 + 2 * 2 + 3 * i
        assert str(response) == f"y: {output}\n", response

    # Check first call is no longer cached
    response = stub.Predict(PredictRequest(X=[1, 2, 3]))
    assert str(response) == "y: 14\n", response


@test(10)
@with_client()
def set_coefs_cache_invalidation(stub: ModelServerStub):
    stub.SetCoefs(SetCoefsRequest(coefs=[1, 2, 3]))

    response = stub.Predict(PredictRequest(X=[1, 2, 3]))
    assert str(response) == "y: 14\n", response

    # Invalidate cache
    stub.SetCoefs(SetCoefsRequest(coefs=[1, 2, 3]))

    response = stub.Predict(PredictRequest(X=[1, 2, 3]))
    assert str(response) == "y: 14\n", response


@test(10)
@client_workload(np.array([1, 2, 3]), "workload/workload1.csv")
def client_workload_1(hit_rate):
    expected_hit_rate = 0
    assert expected_hit_rate == hit_rate, (
        f"Expected cache hit rate to be {expected_hit_rate}, " f"but got {hit_rate}"
    )


@test(10)
@client_workload(np.array([1, 2, 3]), "workload/workload2.csv")
def client_workload_2(hit_rate):
    expected_hit_rate = 2 / 12
    assert expected_hit_rate == hit_rate, (
        f"Expected cache hit rate to be {expected_hit_rate}, " f"but got {hit_rate}"
    )


@test(10)
@client_workload(
    np.array([1, 2, 3]), "workload/workload1.csv", "workload/workload2.csv"
)
def client_workload_3(_hit_rate):
    # Due to multithreading, this test is not deterministic
    assert 0 <= _hit_rate <= 1, f"Expected hit rate to be between 0 and 1"


if __name__ == "__main__":
    tester_main()
