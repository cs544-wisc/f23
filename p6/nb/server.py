from concurrent import futures
import grpc
import station_pb2_grpc
import station_pb2
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel

class StationRecord: 

    def __init__(self, tmin, tmax):
        self.tmin = tmin
        self.tmax = tmax

# Initialize the session
try:
    cluster = Cluster(['p6_db_2', 'p6_db_1', 'p6_db_3'])
    session = cluster.connect()
except Exception as e:
    print(e)

def setup_cassandra_table():
    session.execute("DROP KEYSPACE IF EXISTS weather;") # Ensure to drop the keyspace

    # Create keyspace
    session.execute("""CREATE keyspace weather
    WITH REPLICATION = { 
    'class' : 'SimpleStrategy', 
    'replication_factor' : 3 
    };""")

    # Create type
    session.execute(""" CREATE type weather.station_record (
        tmin int,
        tmax int                
    );""")

    # Create table
    session.execute(""" CREATE TABLE weather.stations (
        id text,
        name text STATIC,
        date date,
        record FROZEN<weather.station_record>,
        PRIMARY KEY (id, date)              
    ) WITH CLUSTERING ORDER BY (date ASC); """)

while True:
    try:
        cluster.register_user_type('weather', 'station_record', StationRecord)
        insert_prepared_statement = "INSERT INTO weather.stations(id, date, record) VALUES (?, ?, ?)"
        insert_statement = session.prepare(insert_prepared_statement)
        insert_statement.consistency_level = ConsistencyLevel.ONE

        max_prepared_statement = "SELECT * FROM weather.stations WHERE id = ?"
        max_statement = session.prepare(max_prepared_statement)
        max_statement.consistency_level = ConsistencyLevel.THREE
        break
    except:
        setup_cassandra_table()


class ServerImplementation(station_pb2_grpc.StationServicer):

    def RecordTemps(self, request, context):
        global cluster, session, insert_statement, max_statement

        response = ""
        try:
            station = request.station
            date = request.date
            record = StationRecord(request.tmin, request.tmax)

            session.execute(insert_statement, (station, date, record) )
        except Exception as e:
            print(e)
            response = "Failed to execute insert request due to error " + str(e)
        
        return station_pb2.RecordTempsReply(error = response)

    def StationMax(self, request, context):
        global cluster, session, insert_statement, max_statement

        max_val = None
        try:
            result = session.execute(max_statement, (request.station,) )
            for row in result:
                curr_val = row.record.tmax
                if max_val is None or curr_val > max_val:
                    max_val = curr_val
        except Exception as e:
            max_val = None
        
        response = ""
        if max_val is None:
            max_val = -1
            response = "Couldn't find max value for station " + str(request.station)
        
        return station_pb2.StationMaxReply(tmax = max_val, error = response)

PORT_TO_USE = 5440
NUM_WORKERS = 4
def server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers = NUM_WORKERS))
    station_pb2_grpc.add_StationServicer_to_server(
        ServerImplementation(), server
    )
    server.add_insecure_port("[::]:" + str(PORT_TO_USE))

    print("Runing server on port", PORT_TO_USE)
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    server()    