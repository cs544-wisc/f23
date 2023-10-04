import grpc
import count_pb2, count_pb2_grpc
from concurrent import futures

total = 0

class MyCounter(count_pb2_grpc.CounterServicer):
    def Count(self, request, context):
        global total
        total += 1
        return count_pb2.Resp(total=total)

server = grpc.server(futures.ThreadPoolExecutor(max_workers=1), options=[("grpc.so_reuseport", 0)])
count_pb2_grpc.add_CounterServicer_to_server(MyCounter(), server)
server.add_insecure_port("0.0.0.0:5440")
server.start()
print("started")
server.wait_for_termination()
