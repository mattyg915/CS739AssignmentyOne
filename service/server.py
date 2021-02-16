#!/usr/bin/python3

from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from urllib.parse import urlparse
from socketserver import ThreadingMixIn
from collections import OrderedDict

import json
import sys
import os

import sqlite3

path = os.path.dirname(os.path.abspath(__file__))
dbPath = os.path.join(path, 'kv.db')

caching = False
cache_size = 0
cache = OrderedDict()

class HandleRequests(BaseHTTPRequestHandler):
    # disable logging
    def log_message(self, format, *args):
        return

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
            connection = sqlite3.connect(dbPath)
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
            if 'value' not in body or 'method' not in body or 'key' not in body:
                package = {"error": "missing required key. 'key' and 'method' are required, 'value' also required for puts"}
                response = json.dumps(package)

                self.send_response(400)
                self.send_header('Content-type', 'application/json')
                self.send_header("Content-Length", str(len(response)))
                self.end_headers()
                self.wfile.write(response.encode())
                return

            value = body['value']
            key = body['key']
            method = body['method']
            query = (key,)
            cache_hit = False

            # validate strings
            try:
                key.encode('ASCII')
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
            if '[' in key or '[' in value or ']' in key or ']' in value:
                package = {"error": "invalid char in body"}
                response = json.dumps(package)

                self.send_response(400)
                self.send_header('Content-type', 'application/json')
                self.send_header("Content-Length", str(len(response)))
                self.end_headers()
                self.wfile.write(response.encode())
                return

            if caching:
                if key in cache.keys():
                    result = [cache[key]]
                    cache_hit = True
                else:
                    try:
                        cursor.execute('''SELECT value FROM 'records' WHERE key=?''', query)
                        result = cursor.fetchone()
                        cache_hit = False
                    except Exception:
                        package = {"error": "Internal server error"}
                        response = json.dumps(package)

                        self.send_response(500)
                        self.send_header('Content-type', 'application/json')
                        self.send_header("Content-Length", str(len(response)))
                        self.end_headers()
                        self.wfile.write(response.encode())
                        return

            else:
                try:
                    cursor.execute('''SELECT value FROM 'records' WHERE key=?''', query)
                    result = cursor.fetchone()
                except Exception:
                    package = {"error": "Internal server error"}
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
                    if caching and not cache_hit:
                        # cache is FIFO
                        if len(cache.keys()) > cache_size:
                            cache.popitem(last=False)
                            cache[key] = result
                        else:
                            cache[key] = result

                else:
                    package = {"exists": "no", "former_value": "[]", "new_value": "[]"}
            else:
                if result is not None and result[0] != value:
                    # don't waste time updating value with same value
                    try:
                        cursor.execute('''UPDATE 'records' SET value = ? WHERE key = ?''', (value, key))
                        connection.commit()
                        # update the cache
                        cache[key] = value
                    except Exception:
                        package = {"error": "Internal server error"}
                        response = json.dumps(package)

                        self.send_response(500)
                        self.send_header('Content-type', 'application/json')
                        self.send_header("Content-Length", str(len(response)))
                        self.end_headers()
                        self.wfile.write(response.encode())
                        return
                else:
                    try:
                        cursor.execute('''INSERT INTO 'records' VALUES (?, ?)''', (key, value))
                        connection.commit()
                    except Exception:
                        package = {"error": "Internal server error"}
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


class Server(ThreadingMixIn, HTTPServer):
    if __name__ == '__main__':
        ip, port = sys.argv[1], sys.argv[2]
        connection = sqlite3.connect(dbPath)
        cursor = connection.cursor()

        if len(sys.argv) > 4:
            enable_cache, size = sys.argv[3], sys.argv[4]
            if enable_cache == "--cache":
                caching = True
                cache_size = int(size)

        # make sure we only create the table once
        cursor.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='records' ''')
        if cursor.fetchone()[0] == 0:
            cursor.execute('''CREATE TABLE 'records' (key text, value text)''')

        server = ThreadingHTTPServer((ip, int(port)), HandleRequests)
        print('Server initializing, reachable at http://{}:{}'.format(ip, port))
        server.serve_forever()
