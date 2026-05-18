from concurrent import futures
import grpc

from generated import database_pb2
from generated import database_pb2_grpc

database = {}

logical_clock = 0

REPLICA_ADDRESS = 'localhost:50052'

class DatabaseService(database_pb2_grpc.DatabaseServiceServicer):

    def update_clock(self, received_timestamp):
        global logical_clock

        logical_clock = max(logical_clock, received_timestamp) + 1

        return logical_clock

    def replicate_data(self, key, value, timestamp):

        try:
            channel = grpc.insecure_channel(REPLICA_ADDRESS)

            stub = database_pb2_grpc.DatabaseServiceStub(channel)

            stub.Replicate(
                database_pb2.ReplicationRequest(
                    key=key,
                    value=value,
                    timestamp=timestamp
                )
            )

            print(f"[REPLICATION] Data replicated to replica node")

        except Exception as e:
            print(f"[ERROR] Replication failed: {e}")

    def Put(self, request, context):

        current_clock = self.update_clock(request.timestamp)

        database[request.key] = request.value

        print(
            f"[LEADER][Clock {current_clock}] "
            f"PUT {request.key} = {request.value}"
        )

        self.replicate_data(
            request.key,
            request.value,
            current_clock
        )

        return database_pb2.PutResponse(
            message="Data stored successfully",
            timestamp=current_clock
        )

    def Get(self, request, context):

        current_clock = self.update_clock(request.timestamp)

        value = database.get(request.key, "")

        print(
            f"[LEADER][Clock {current_clock}] "
            f"GET {request.key}"
        )

        return database_pb2.GetResponse(
            value=value,
            timestamp=current_clock
        )

    def Replicate(self, request, context):

        current_clock = self.update_clock(request.timestamp)

        database[request.key] = request.value

        print(
            f"[REPLICA][Clock {current_clock}] "
            f"REPLICATED {request.key} = {request.value}"
        )

        return database_pb2.ReplicationResponse(
            message="Replication successful"
        )

    def Update(self, request, context):

        current_clock = self.update_clock(request.timestamp)

        if request.key in database:

            database[request.key] = request.value

            print(
                f"[UPDATE][Clock {current_clock}] "
                f"{request.key} = {request.value}"
            )

            self.replicate_data(
                request.key,
                request.value,
                current_clock
            )

            return database_pb2.UpdateResponse(
                message="Updated successfully",
                timestamp=current_clock
            )

        return database_pb2.UpdateResponse(
            message="Key not found",
            timestamp=current_clock
        )

    def Delete(self, request, context):

        current_clock = self.update_clock(request.timestamp)

        if request.key in database:

            del database[request.key]

            print(
                f"[DELETE][Clock {current_clock}] "
                f"{request.key}"
            )

            return database_pb2.DeleteResponse(
                message="Deleted successfully",
                timestamp=current_clock
            )

        return database_pb2.DeleteResponse(
            message="Key not found",
            timestamp=current_clock
        )

    def ListKeys(self, request, context):

        current_clock = self.update_clock(request.timestamp)

        keys = list(database.keys())

        print(
            f"[LIST_KEYS][Clock {current_clock}]"
        )

        return database_pb2.ListKeysResponse(
            keys=keys,
            timestamp=current_clock
        )

    def Exists(self, request, context):

        current_clock = self.update_clock(request.timestamp)

        exists = request.key in database

        print(
            f"[EXISTS][Clock {current_clock}] "
            f"{request.key} -> {exists}"
        )

        return database_pb2.ExistsResponse(
            exists=exists,
            timestamp=current_clock
        )

    def Size(self, request, context):

        current_clock = self.update_clock(request.timestamp)

        size = len(database)

        print(
            f"[SIZE][Clock {current_clock}] "
            f"{size}"
        )

        return database_pb2.SizeResponse(
            size=size,
            timestamp=current_clock
        )

    def Clear(self, request, context):

        current_clock = self.update_clock(request.timestamp)

        database.clear()

        print(
            f"[CLEAR][Clock {current_clock}] "
            f"Database cleared"
        )

        return database_pb2.ClearResponse(
            message="Database cleared",
            timestamp=current_clock
        )

    def GetAll(self, request, context):

        current_clock = self.update_clock(request.timestamp)

        items = []

        for key, value in database.items():

            items.append(
                database_pb2.KeyValue(
                    key=key,
                    value=value
                )
            )

        print(
            f"[GET_ALL][Clock {current_clock}]"
        )

        return database_pb2.GetAllResponse(
            items=items,
            timestamp=current_clock
        )


def serve():

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10)
    )

    database_pb2_grpc.add_DatabaseServiceServicer_to_server(
        DatabaseService(),
        server
    )

    server.add_insecure_port('[::]:50051')

    print("Leader Node running on port 50051")

    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    serve()