"""The Python implementation of the GRPC KVStore client demo for proj 1."""

from __future__ import print_function
import logging

import grpc

import keyvaluestore_pb2
import keyvaluestore_pb2_grpc

def run():
    # NOTE(gRPC Python Team): .close() is possible on a channel and should be
    # used in circumstances in which the with statement does not fit the needs
    # of the code.
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = keyvaluestore_pb2_grpc.KeyValueStoreStub(channel)
        response = stub.PutValue(keyvaluestore_pb2.PutRequest(key='a', newvalue='a value'))
        print("Greeter client received: " + response.value)
        response = stub.GetValue(keyvaluestore_pb2.GetRequest(key='a'))
        print("Greeter client received: " + response.value)


if __name__ == '__main__':
    logging.basicConfig()
    run()
