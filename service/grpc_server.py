"""The Python implementation of the GRPC KVStore client demo for proj 1."""
from concurrent import futures
import logging

import grpc

import keyvaluestore_pb2
import keyvaluestore_pb2_grpc
###############grpc end#################
#from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from urllib.parse import urlparse
#from socketserver import ThreadingMixIn

#import json
import sys
import os

import sqlite3

path = os.path.dirname(os.path.abspath(__file__))
dbPath = os.path.join(path, 'kv.db')


class KeyValueStore(keyvaluestore_pb2_grpc.KeyValueStoreServicer):

	def GetValue(self, request, context):
		connection = sqlite3.connect(dbPath)
		cursor = connection.cursor()
		route = urlparse(path)
		print(route.path)
		# handle read request
		if True: #route.path == "/kv739/":
		
			query = (request.key,)
			cursor.execute('''SELECT value FROM 'records' WHERE key=?''', request.key)
			result = cursor.fetchone()

			if result is not None:
				ret_val = result[0]
			else:
				ret_val = "None"
		return keyvaluestore_pb2.Response(value=ret_val)


	def PutValue(self, request, context):
		connection = sqlite3.connect(dbPath)
		cursor = connection.cursor()
		route = urlparse(path)
		print(route.path)
		# direct write request
		if True: #route.path == "/kv739/":

			key, value = request.key, request.newvalue
			query = (key,)
			cursor.execute('''SELECT value FROM 'records' WHERE key=?''', query)
			result = cursor.fetchone()
			if result is not None and result != value:
				# don't waste time updating value with same value
				cursor.execute('''UPDATE 'records' SET value = ? WHERE key = ?''', (value, key))
				connection.commit()
			else:
				cursor.execute('''INSERT INTO 'records' VALUES (?, ?)''', (key, value))
				connection.commit()

			if result is not None:
				ret_val = result[0]
			else:
				ret_val = "None"
		return keyvaluestore_pb2.Response(value=ret_val)

def serve(ip, port):
	server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
	keyvaluestore_pb2_grpc.add_KeyValueStoreServicer_to_server(KeyValueStore(), server)
	server.add_insecure_port(ip+':'+port)
	server.start()
	print('Server initializing, reachable at http://{}:{}'.format(ip, port))
	server.wait_for_termination()

class Server():
	if __name__ == '__main__':
		ip, port = sys.argv[1], sys.argv[2]
		connection = sqlite3.connect(dbPath)
		cursor = connection.cursor()

		# make sure we only create the table once
		cursor.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='records' ''')
		if cursor.fetchone()[0] == 0:
			cursor.execute('''CREATE TABLE 'records' (key text, value text)''')

		logging.basicConfig()
		serve(ip, port)