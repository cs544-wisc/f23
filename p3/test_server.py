import grpc
import modelserver_pb2
import modelserver_pb2_grpc

port = "5440"
addr = f"127.0.0.1:{port}"
channel = grpc.insecure_channel(addr)
stub = modelserver_pb2_grpc.ModelServerStub(channel)

stub.SetCoefs(modelserver_pb2.SetCoefsRequest(coefs=[1, 2, 3]))
resp = stub.Predict(modelserver_pb2.PredictRequest(inputs=[1, 2, 3]))
print(f"value={resp.value}, cache_hit={resp.cache_hit}")  # should be 14, false
resp = stub.Predict(modelserver_pb2.PredictRequest(inputs=[1, 2, 3]))
print(f"value={resp.value}, cache_hit={resp.cache_hit}")  # should be 14, true
resp = stub.Predict(modelserver_pb2.PredictRequest(inputs=[1.00004, 2.00001, 2.99996]))
print(f"value={resp.value}, cache_hit={resp.cache_hit}")  # should be 14, true
resp = stub.Predict(modelserver_pb2.PredictRequest(inputs=[2, 3, 4]))
print(f"value={resp.value}, cache_hit={resp.cache_hit}")  # should be 20, false
