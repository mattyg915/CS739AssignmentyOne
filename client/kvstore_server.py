"""The Python implementation of the GRPC KVStore server for proj 1."""

from concurrent import futures
import logging

import grpc

import keyvaluestore_pb2
import keyvaluestore_pb2_grpc


class KeyValueStore(keyvaluestore_pb2_grpc.KeyValueStoreServicer):

    def GetValue(self, request, context):
        return keyvaluestore_pb2.Response(value='Get received: %s-%s!' % (request.key, 'value'))
    def PutValue(self, request, context):
        return keyvaluestore_pb2.Response(value='Set received: %s-%s!' % (request.key, request.newvalue))


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    keyvaluestore_pb2_grpc.add_KeyValueStoreServicer_to_server(KeyValueStore(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    serve()
