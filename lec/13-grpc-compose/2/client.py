import math_pb2_grpc, math_pb2
import grpc

channel = grpc.insecure_channel("localhost:5440")
stub = math_pb2_grpc.CalcStub(channel)

resp = stub.Mult(math_pb2.MultReq(x=3, y=4))
print(resp)

resp = stub.MultMany(math_pb2.MultManyReq(nums=[2,3,4]))
print(resp)
