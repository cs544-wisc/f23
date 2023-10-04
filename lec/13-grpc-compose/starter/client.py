import sys
import grpc
import count_pb2, count_pb2_grpc

channel = grpc.insecure_channel("127.0.0.1:" + sys.argv[1])
stub = count_pb2_grpc.CounterStub(channel)

print(stub.Count(count_pb2.Req()))
