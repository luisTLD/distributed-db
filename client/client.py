import grpc

from generated import database_pb2
from generated import database_pb2_grpc

logical_clock = 0

channel = grpc.insecure_channel('localhost:50051')

stub = database_pb2_grpc.DatabaseServiceStub(channel)

# =========================
# PUT
# =========================

logical_clock += 1

response = stub.Put(
    database_pb2.PutRequest(
        key="usuario",
        value="Luis",
        timestamp=logical_clock
    )
)

logical_clock = response.timestamp

print("\n[PUT]")
print(response.message)
print(f"Client Clock: {logical_clock}")

# =========================
# GET
# =========================

logical_clock += 1

response = stub.Get(
    database_pb2.GetRequest(
        key="usuario",
        timestamp=logical_clock
    )
)

logical_clock = response.timestamp

print("\n[GET]")
print("Value:", response.value)
print(f"Client Clock: {logical_clock}")

# =========================
# UPDATE
# =========================

logical_clock += 1

response = stub.Update(
    database_pb2.UpdateRequest(
        key="usuario",
        value="Carlos",
        timestamp=logical_clock
    )
)

logical_clock = response.timestamp

print("\n[UPDATE]")
print(response.message)
print(f"Client Clock: {logical_clock}")

# =========================
# EXISTS
# =========================

response = stub.Exists(
    database_pb2.ExistsRequest(
        key="usuario"
    )
)

print("\n[EXISTS]")
print(response.exists)

# =========================
# LIST_KEYS
# =========================

response = stub.ListKeys(
    database_pb2.EmptyRequest()
)

print("\n[LIST_KEYS]")
print(list(response.keys))

# =========================
# SIZE
# =========================

response = stub.Size(
    database_pb2.EmptyRequest()
)

print("\n[SIZE]")
print(response.size)

# =========================
# GET_ALL
# =========================

response = stub.GetAll(
    database_pb2.EmptyRequest()
)

print("\n[GET_ALL]")

for item in response.items:
    print(f"{item.key} = {item.value}")

# =========================
# DELETE
# =========================

logical_clock += 1

response = stub.Delete(
    database_pb2.DeleteRequest(
        key="usuario",
        timestamp=logical_clock
    )
)

logical_clock = response.timestamp

print("\n[DELETE]")
print(response.message)
print(f"Client Clock: {logical_clock}")

# =========================
# EXISTS AFTER DELETE
# =========================

response = stub.Exists(
    database_pb2.ExistsRequest(
        key="usuario"
    )
)

print("\n[EXISTS AFTER DELETE]")
print(response.exists)

# =========================
# CLEAR
# =========================

response = stub.Clear(
    database_pb2.EmptyRequest()
)

print("\n[CLEAR]")
print(response.message)

# =========================
# FINAL SIZE
# =========================

response = stub.Size(
    database_pb2.EmptyRequest()
)

print("\n[FINAL SIZE]")
print(response.size)