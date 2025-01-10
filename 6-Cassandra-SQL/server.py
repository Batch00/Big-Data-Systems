import grpc
from concurrent import futures
import station_pb2
import station_pb2_grpc
import cassandra
from cassandra.cluster import Cluster
from cassandra.query import PreparedStatement, ConsistencyLevel

class StationServicer(station_pb2_grpc.StationServicer):
    def __init__(self):
        # Cassandra connection
        self.cluster = Cluster(['p6-db-1', 'p6-db-2', 'p6-db-3'])
        self.cass = self.cluster.connect('weather')
        
        # Prepare statements
        self.insert_statement = self.cass.prepare("INSERT INTO stations (id, date, record) VALUES (?, ?, {tmin:?,tmax:?})")
        self.insert_statement.consistency_level = ConsistencyLevel.ONE
        
        self.max_statement = self.cass.prepare("SELECT MAX(record.tmax) FROM stations WHERE id = ?")
        self.max_statement.consistency_level = ConsistencyLevel.THREE

    def RecordTemps(self, request, context):
        try:
            station_id = request.station
            date = request.date
            tmin = request.tmin
            tmax = request.tmax
            
            self.cass.execute(self.insert_statement, (station_id, date, tmin, tmax))
            
            # send to the client
            return station_pb2.RecordTempsReply(error="")
            
        except cassandra.cluster.NoHostAvailable as e:
            for error_message in e.errors.values():
                if isinstance(error_message, cassandra.Unavailable):
                    return station_pb2.RecordTempsReply(error=f"need {error_message.required_replicas} replicas, but only have {error_message.alive_replicas}")
            return station_pb2.RecordTempsReply(error="Unknown error")
            
        except cassandra.Unavailable as e:
            return station_pb2.RecordTempsReply(error=f"need {e.required_replicas} replicas, but only have {e.alive_replicas}")
            
        except Exception as e:
            return station_pb2.RecordTempsReply(error=f"A different error occurred {e}")

    def StationMax(self, request, context):
        try:
            station_id = request.station
            
            # get the maximum tmax
            result = self.cass.execute(self.max_statement, (station_id,))
            max_tmax = result.one()[0]
            
            # send to the client
            return station_pb2.StationMaxReply(tmax=max_tmax)
        
        except cassandra.cluster.NoHostAvailable as e:
            for error_message in e.errors.values():
                if isinstance(error_message, cassandra.Unavailable):
                    return station_pb2.StationMaxReply(error=f"need {error_message.required_replicas} replicas, but only have {error_message.alive_replicas}")
            return station_pb2.StationMaxReply(error="Unknown error")
            
        except cassandra.Unavailable as e:
            return station_pb2.StationMaxReply(error=f"need {e.required_replicas} replicas, but only have {e.alive_replicas}")
            
        except Exception as e:
            return station_pb2.StationMaxReply(error=f"A different error occurred {e}")
        

server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
station_pb2_grpc.add_StationServicer_to_server(StationServicer(), server)
server.add_insecure_port('[::]:5440')
server.start()
server.wait_for_termination()
