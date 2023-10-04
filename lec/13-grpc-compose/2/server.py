import math_pb2_grpc, math_pb2
import grpc
from concurrent import futures

class MyCalc(math_pb2_grpc.CalcServicer):
    def Mult(self, request, context):
        print("hi")
        print(request)
        return math_pb2.MultResp(result = request.x * request.y)

    def MultMany(self, request, context):
        total = 1
        for num in request.nums:
            total *= num
        return math_pb2.MultResp(result = total)
    
server = grpc.server(futures.ThreadPoolExecutor(max_workers=10), options=[("grpc.so_reuseport", 0)])
math_pb2_grpc.add_CalcServicer_to_server(MyCalc(), server)
server.add_insecure_port('0.0.0.0:5440')
server.start()
print("started")
server.wait_for_termination()

    
