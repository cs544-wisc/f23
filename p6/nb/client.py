import station_pb2_grpc
import station_pb2
import grpc

PORT_TO_USE = 5440
SERVER_ADDRESS = f"127.0.0.1:{PORT_TO_USE}"

def runner():
    channel = grpc.insecure_channel(SERVER_ADDRESS)
    stub = station_pb2_grpc.StationStub(channel)

    test_station = "USC00470273"
    date = "2023-08-30"
    tmin, tmax = -1, 14
    response = stub.RecordTemps(station_pb2.RecordTempsRequest(station = test_station, date = date, tmin = tmin, tmax = tmax))
    print("Insert Error", response.error)

    response = stub.StationMax(station_pb2.StationMaxRequest(station = test_station))
    print("Max Error", response.error)
    print("Max", response.tmax)

if __name__ == "__main__":
    runner()