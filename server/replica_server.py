from concurrent import futures
import grpc

from generated import database_pb2
from generated import database_pb2_grpc

database = {}

logical_clock = 0


class DatabaseService(database_pb2_grpc.DatabaseServiceServicer):

    def update_clock(self, received_timestamp):
        global logical_clock

        logical_clock = max(logical_clock, received_timestamp) + 1

        return logical_clock

    # =========================================
    # PUT
    # =========================================

    def Put(self, request, context):

        current_clock = self.update_clock(request.timestamp)

        database[request.key] = request.value

        print(
            f"[REPLICA][Clock {current_clock}] "
            f"PUT {request.key} = {request.value}"
        )

        return database_pb2.PutResponse(
            message="Data stored successfully",
            timestamp=current_clock
        )

    # =========================================
    # GET
    # =========================================

    def Get(self, request, context):

        current_clock = self.update_clock(request.timestamp)

        value = database.get(request.key, "")

        print(
            f"[REPLICA][Clock {current_clock}] "
            f"GET {request.key}"
        )

        return database_pb2.GetResponse(
            value=value,
            timestamp=current_clock
        )

    # =========================================
    # REPLICATION
    # =========================================

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

    # =========================================
    # UPDATE
    # =========================================

    def Update(self, request, context):

        current_clock = self.update_clock(request.timestamp)

        if request.key in database:

            database[request.key] = request.value

            print(
                f"[REPLICA][Clock {current_clock}] "
                f"UPDATE {request.key} = {request.value}"
            )

            return database_pb2.UpdateResponse(
                message="Updated successfully",
                timestamp=current_clock
            )

        return database_pb2.UpdateResponse(
            message="Key not found",
            timestamp=current_clock
        )

    # =========================================
    # DELETE
    # =========================================

    def Delete(self, request, context):

        current_clock = self.update_clock(request.timestamp)

        if request.key in database:

            del database[request.key]

            print(
                f"[REPLICA][Clock {current_clock}] "
                f"DELETE {request.key}"
            )

            return database_pb2.DeleteResponse(
                message="Deleted successfully",
                timestamp=current_clock
            )

        return database_pb2.DeleteResponse(
            message="Key not found",
            timestamp=current_clock
        )

    # =========================================
    # LIST KEYS
    # =========================================

    def ListKeys(self, request, context):

        keys = list(database.keys())

        print("[REPLICA] LIST_KEYS")

        return database_pb2.ListKeysResponse(
            keys=keys
        )

    # =========================================
    # EXISTS
    # =========================================

    def Exists(self, request, context):

        exists = request.key in database

        print(
            f"[REPLICA] EXISTS {request.key} -> {exists}"
        )

        return database_pb2.ExistsResponse(
            exists=exists
        )

    # =========================================
    # SIZE
    # =========================================

    def Size(self, request, context):

        size = len(database)

        print(f"[REPLICA] SIZE -> {size}")

        return database_pb2.SizeResponse(
            size=size
        )

    # =========================================
    # CLEAR
    # =========================================

    def Clear(self, request, context):

        database.clear()

        print("[REPLICA] DATABASE CLEARED")

        return database_pb2.ClearResponse(
            message="Database cleared"
        )

    # =========================================
    # GET ALL
    # =========================================

    def GetAll(self, request, context):

        items = []

        for key, value in database.items():

            items.append(
                database_pb2.KeyValue(
                    key=key,
                    value=value
                )
            )

        print("[REPLICA] GET_ALL")

        return database_pb2.GetAllResponse(
            items=items
        )


# =========================================
# SERVER
# =========================================

def serve():

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10)
    )

    database_pb2_grpc.add_DatabaseServiceServicer_to_server(
        DatabaseService(),
        server
    )

    server.add_insecure_port('[::]:50052')

    print("Replica Node running on port 50052")

    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()