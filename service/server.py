#!/usr/bin/python3

from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from urllib.parse import urlparse
from socketserver import ThreadingMixIn
from datetime import datetime

import json
import sys
import os

import sqlite3

path = os.path.dirname(os.path.abspath(__file__))

# caching = False
# cache_size = 0
# cache_size =
# cache = OrderedDict()

class HandleRequests(BaseHTTPRequestHandler):
    # disable logging
    def log_message(self, format, *args):
        return

    def do_GET(self):
        route = urlparse(self.path)
        if route.path == '/health/':
            response = "OK"
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response.encode())
        if route.path == '/nodes/':
            response = json.dumps({"nodes" : node_list})
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response.encode())
        if route.path == '/die_clean/':
            response = "DYING"
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response.encode())
            #TODO Flush all state and die
        if route.path == '/die/':
            #TODO Kill all threads and
            response = "DYING"

    # all requests handled via POST
    # def do_GET(self):
    #     connection = sqlite3.connect(dbPath)
    #     cursor = connection.cursor()
    #     route = urlparse(self.path)
    #     # handle read request
    #     if route.path == "/kv739/":
    #         key = route.query.split("=")[-1]
    #         query = (key,)
    #         cache_hit = False
    #
    #         # if caching enabled check the cache
    #         if caching:
    #             if key in cache.keys():
    #                 result = [cache[key]]
    #                 cache_hit = True
    #             else:
    #                 cursor.execute('''SELECT value FROM 'records' WHERE key=?''', query)
    #                 result = cursor.fetchone()
    #                 cache_hit = False
    #
    #         else:
    #             cursor.execute('''SELECT value FROM 'records' WHERE key=?''', query)
    #             result = cursor.fetchone()
    #
    #         if result is not None:
    #             package = {"exists": "yes", "value": result[0]}
    #
    #             if caching and not cache_hit:
    #                 # cache is FIFO
    #                 if len(cache.keys()) > 250:
    #                     cache.popitem(last=False)
    #                     cache[key] = result
    #                 else:
    #                     cache[key] = result
    #         else:
    #             package = {"exists": "no", "value": "None"}
    #
    #         response = json.dumps(package)
    #
    #         self.send_response(200)
    #         self.send_header('Content-type', 'application/json')
    #         self.send_header("Content-Length", str(len(response)))
    #         self.end_headers()
    #         self.wfile.write(response.encode())
    #         return
    #     else:
    #         self.send_response(200)
    #         #self.send_header('Content-type', 'application/json')
    #         self.send_header("Content-Length", str(len("helloworld")))
    #         self.end_headers()
    #         self.wfile.write("helloworld".encode())
    #         return

    def do_POST(self):
        try:
            connection = sqlite3.connect(dbPath, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        except Exception:
            package = {"error": "error opening database connection"}
            response = json.dumps(package)

            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response.encode())
            return

        cursor = connection.cursor()
        content_len = int(self.headers.get('Content-Length'))
        post_body = self.rfile.read(content_len)

        decoded_body = post_body.decode()
        body = json.loads(decoded_body)
        try:
            route = urlparse(self.path)
        except UnicodeEncodeError:
            package = {"error": "error parsing POST body"}
            response = json.dumps(package)

            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response.encode())
            return

        # direct write request
        if route.path == "/kv739/":
            value = None
            if 'method' not in body or 'key' not in body:
                package = {"error": "missing required key. 'key' and 'method' are required, 'value' also required for puts"}
                response = json.dumps(package)

                self.send_response(400)
                self.send_header('Content-type', 'application/json')
                self.send_header("Content-Length", str(len(response)))
                self.end_headers()
                self.wfile.write(response.encode())
                return

            key = body['key']
            method = body['method']

            if method != 'get' and 'value' not in body:
                package = {
                    "error": "missing required key. 'key' and 'method' are required, 'value' also required for puts"}
                response = json.dumps(package)

                self.send_response(400)
                self.send_header('Content-type', 'application/json')
                self.send_header("Content-Length", str(len(response)))
                self.end_headers()
                self.wfile.write(response.encode())
                return

            query = (key,)
            # cache_hit = False

            # validate strings
            try:
                key.encode('ASCII')
            except UnicodeEncodeError:
                package = {"error": "invalid char in body"}
                response = json.dumps(package)

                self.send_response(400)
                self.send_header('Content-type', 'application/json')
                self.send_header("Content-Length", str(len(response)))
                self.end_headers()
                self.wfile.write(response.encode())
                return
            if '[' in key or ']' in key:
                package = {"error": "invalid char in body"}
                response = json.dumps(package)

                self.send_response(400)
                self.send_header('Content-type', 'application/json')
                self.send_header("Content-Length", str(len(response)))
                self.end_headers()
                self.wfile.write(response.encode())
                return

            # if caching:
            #     if key in cache.keys():
            #         result = [cache[key]]
            #         cache_hit = True
            #     else:
            #         try:
            #             cursor.execute('''SELECT value FROM 'records' WHERE key=?''', query)
            #             result = cursor.fetchone()
            #             cache_hit = False
            #         except Exception:
            #             package = {"error": "Internal server error"}
            #             response = json.dumps(package)
            #
            #             self.send_response(500)
            #             self.send_header('Content-type', 'application/json')
            #             self.send_header("Content-Length", str(len(response)))
            #             self.end_headers()
            #             self.wfile.write(response.encode())
            #             return

            # else:
            try:
                cursor.execute('''SELECT value FROM 'records' WHERE key=?''', query)
                result = cursor.fetchone()
            except Exception as e:
                message = "Internal server error: {}".format(e)
                package = {"error": message}
                response = json.dumps(package)

                self.send_response(500)
                self.send_header('Content-type', 'application/json')
                self.send_header("Content-Length", str(len(response)))
                self.end_headers()
                self.wfile.write(response.encode())
                return

            #cursor.execute('''SELECT value FROM 'records' WHERE key=?''', query)
            #result = cursor.fetchone()
            if method == 'get':
                if result is not None:
                    package = {"exists": "yes", "former_value": result[0], "new_value": "[]"}
                    # if caching and not cache_hit:
                    #     # cache is FIFO
                    #     if len(cache.keys()) > cache_size:
                    #         cache.popitem(last=False)
                    #         cache[key] = result
                    #     else:
                    #         cache[key] = result

                else:
                    package = {"exists": "no", "former_value": "[]", "new_value": "[]"}
            else:
                value = body['value']
                # validate strings
                try:
                    value.encode('ASCII')
                except UnicodeEncodeError:
                    package = {"error": "invalid char in body"}
                    response = json.dumps(package)

                    self.send_response(400)
                    self.send_header('Content-type', 'application/json')
                    self.send_header("Content-Length", str(len(response)))
                    self.end_headers()
                    self.wfile.write(response.encode())
                    return
                if '[' in value or ']' in value:
                    package = {"error": "invalid char in body"}
                    response = json.dumps(package)

                    self.send_response(400)
                    self.send_header('Content-type', 'application/json')
                    self.send_header("Content-Length", str(len(response)))
                    self.end_headers()
                    self.wfile.write(response.encode())
                    return
                if result is not None and result[0] != value:
                    # don't waste time updating value with same value
                    try:
                        millisec = int((datetime.utcnow() - datetime(1970, 1, 1)).total_seconds() * 1000)
                        cursor.execute('''UPDATE 'records' SET value = ?, time = ? WHERE key = ?''', (value, millisec, key))
                        connection.commit()
                        # # update the cache
                        # cache[key] = value
                    except Exception as e:
                        message = "Internal server error: {}".format(e)
                        package = {"error": message}
                        response = json.dumps(package)

                        self.send_response(500)
                        self.send_header('Content-type', 'application/json')
                        self.send_header("Content-Length", str(len(response)))
                        self.end_headers()
                        self.wfile.write(response.encode())
                        return
                else:
                    try:
                        millisec = int((datetime.utcnow() - datetime(1970, 1, 1)).total_seconds() * 1000)
                        cursor.execute('''INSERT INTO 'records' VALUES (?, ?, ?)''', (key, value, millisec))
                        connection.commit()
                    except Exception as e:
                        message = "Internal server error: {}".format(e)
                        package = {"error": message}
                        response = json.dumps(package)

                        self.send_response(500)
                        self.send_header('Content-type', 'application/json')
                        self.send_header("Content-Length", str(len(response)))
                        self.end_headers()
                        self.wfile.write(response.encode())
                        return

                if result is not None:
                    package = {"exists": "yes", "former_value": result[0], "new_value": value}
                else:
                    package = {"exists": "no", "former_value": "[]", "new_value": value}

            response = json.dumps(package)
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response.encode())
            global entropy_counter
            entropy_counter += 1

def readNodes(nodes_file):
    f = open(nodes_file, "r")
    l = []
    for line in f.readlines():
        l += [line.strip(' ').strip('\n')]
    return l


class Server(ThreadingMixIn, HTTPServer):
    if __name__ == '__main__':
        global dbPath
        global node_list
        global entropy_counter
        entropy_counter = 0
        nodes_file, node_index = sys.argv[1], sys.argv[2]
        node_list = readNodes(nodes_file)

        node_address = node_list[int(node_index)].split()
        ip = node_address[0]
        port = node_address[1]

        dbName = port + 'kv.db'
        dbPath = os.path.join(path, dbName)

        connection = sqlite3.connect(dbPath, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        cursor = connection.cursor()

        # make sure we only create the table once
        cursor.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='records' ''')
        if cursor.fetchone()[0] == 0:
            cursor.execute('''CREATE TABLE 'records' (key text, value text, time integer )''')

        server = ThreadingHTTPServer((ip, int(port)), HandleRequests)
        print('Server initializing, reachable at http://{}:{}'.format(ip, port))
        server.serve_forever()
