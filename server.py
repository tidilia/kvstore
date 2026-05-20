import time
from collections import OrderedDict
from concurrent import futures

import grpc
import kvstore_pb2
import kvstore_pb2_grpc

import threading

MAX_KEYS = 10

store = {}        
expires = {}    
lru = OrderedDict()

lock = threading.Lock()


def is_expired(key: str) -> bool:
    if key not in expires:
        return False

    exp = expires[key]
    if exp is None:
        return False

    if time.time() > exp:
        store.pop(key, None)
        expires.pop(key, None)
        lru.pop(key, None)
        return True

    return False


def touch_lru(key: str):
    if key in lru:
        lru.move_to_end(key)
    else:
        lru[key] = True
        
def evict_if_needed():
    while len(store) > MAX_KEYS:
        old_key, _ = lru.popitem(last=False)

        store.pop(old_key, None)
        expires.pop(old_key, None)

        print(f"Evicted LRU key: {old_key}")


class KeyValueStoreServicer(kvstore_pb2_grpc.KeyValueStoreServicer):

    def Put(self, request, context):
        with lock:
            key = request.key
            value = request.value

            store[key] = value

            # TTL
            if request.ttl_seconds > 0:
                expires[key] = time.time() + request.ttl_seconds
            else:
                expires[key] = None

            # LRU update
            touch_lru(key)
            evict_if_needed()

            return kvstore_pb2.PutResponse()

    def Get(self, request, context):
        with lock:
            key = request.key

            if key not in store or is_expired(key):
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Key not found")
                return kvstore_pb2.GetResponse()

            touch_lru(key)

            return kvstore_pb2.GetResponse(value=store[key])

    def Delete(self, request, context):
        with lock:
            key = request.key

            store.pop(key, None)
            expires.pop(key, None)
            lru.pop(key, None)

            return kvstore_pb2.DeleteResponse()

    def List(self, request, context):
        with lock:
            prefix = request.prefix

            items = []

            # iterate over copy to avoid runtime modification issues
            for key in list(store.keys()):
                if is_expired(key):
                    continue

                if key.startswith(prefix):
                    items.append(
                        kvstore_pb2.KeyValue(
                            key=key,
                            value=store[key]
                        )
                    )

            return kvstore_pb2.ListResponse(items=items)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    kvstore_pb2_grpc.add_KeyValueStoreServicer_to_server(
        KeyValueStoreServicer(),
        server
    )

    server.add_insecure_port("[::]:8000")
    server.start()

    print("Server started on port 8000")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()