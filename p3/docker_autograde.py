from functools import wraps

from google.protobuf.descriptor import FieldDescriptor
import grpc
from modelserver_pb2 import (
    PredictRequest,  # type: ignore
    PredictResponse,  # type: ignore
    SetCoefsRequest,  # type: ignore
    SetCoefsResponse,  # type: ignore
)
from modelserver_pb2_grpc import ModelServerStub

from tester import test, tester_main


def with_client():
    def decorator(test_func):
        @wraps(test_func)
        def wrapper():
            port = "5440"
            addr = f"127.0.0.1:{port}"
            channel = grpc.insecure_channel(addr)
            stub = ModelServerStub(channel)
            return test_func(stub)

        return wrapper

    return decorator


@test(10)
def correct_protobuf_interface():
    descriptors = {
        SetCoefsRequest.DESCRIPTOR: {
            "coefs": [FieldDescriptor.LABEL_REPEATED, FieldDescriptor.CPPTYPE_FLOAT]
        },
        SetCoefsResponse.DESCRIPTOR: {
            "success": [
                FieldDescriptor.LABEL_OPTIONAL,
                FieldDescriptor.CPPTYPE_BOOL,
            ],
            "error": [
                FieldDescriptor.LABEL_OPTIONAL,
                FieldDescriptor.CPPTYPE_STRING,
            ],
        },
        PredictRequest.DESCRIPTOR: {
            "inputs": [
                FieldDescriptor.LABEL_REPEATED,
                FieldDescriptor.CPPTYPE_FLOAT,
            ],
        },
        PredictResponse.DESCRIPTOR: {
            "output": [
                FieldDescriptor.LABEL_OPTIONAL,
                FieldDescriptor.CPPTYPE_FLOAT,
            ],
            "cache_hit": [
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
def correct_set_coefs(stub: ModelServerStub):
    response = stub.SetCoefs(SetCoefsRequest(coefs=[1, 2, 3]))
    assert str(response) == "success: true\n", response
    response = stub.SetCoefs(SetCoefsRequest(coefs=[2, 3, 4]))
    assert str(response) == "success: true\n", response


@test(10)
@with_client()
def correct_predict(stub: ModelServerStub):
    stub.SetCoefs(SetCoefsRequest(coefs=[1, 2, 3]))
    response = stub.Predict(PredictRequest(inputs=[1, 2, 3]))
    assert str(response) == "output: 14\n", response


@test(10)
@with_client()
def correct_predict_single_call_cache(stub: ModelServerStub):
    stub.SetCoefs(SetCoefsRequest(coefs=[1, 2, 3]))

    # Check one subsequent call is cached
    response = stub.Predict(PredictRequest(inputs=[1, 2, 3]))
    assert str(response) == "output: 14\n", response
    response = stub.Predict(PredictRequest(inputs=[1, 2, 3]))
    assert str(response) == "output: 14\ncache_hit: true\n", response


@test(10)
@with_client()
def correct_predict_full_cache_eviction(stub: ModelServerStub):
    stub.SetCoefs(SetCoefsRequest(coefs=[1, 2, 3]))
    stub.Predict(PredictRequest(inputs=[1, 2, 3]))

    # Fill the cache
    for i in range(10):
        response = stub.Predict(PredictRequest(inputs=[3, 2, i]))
        output = 1 * 3 + 2 * 2 + 3 * i
        assert str(response) == f"output: {output}\n", response

    # Check first call is no longer cached
    response = stub.Predict(PredictRequest(inputs=[1, 2, 3]))
    assert str(response) == "output: 14\n", response


@test(10)
@with_client()
def correct_set_coefs_cache_invalidation(stub: ModelServerStub):
    stub.SetCoefs(SetCoefsRequest(coefs=[1, 2, 3]))

    response = stub.Predict(PredictRequest(inputs=[1, 2, 3]))
    assert str(response) == "output: 14\n", response

    # Invalidate cache
    stub.SetCoefs(SetCoefsRequest(coefs=[1, 2, 3]))

    response = stub.Predict(PredictRequest(inputs=[1, 2, 3]))
    assert str(response) == "output: 14\n", response


if __name__ == "__main__":
    tester_main()
