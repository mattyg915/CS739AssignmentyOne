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
cache = OrderedDict()

class HandleRequests(BaseHTTPRequestHandler):
    def do_GET(self):
        connection = sqlite3.connect(dbPath)
        cursor = connection.cursor()
        route = urlparse(self.path)
        # handle read request
        if route.path == "/kv739/":
            key = route.query.split("=")[-1]
            query = (key,)
            cache_hit = False

            # if caching enabled check the cache
            if caching:
                if key in cache.keys():
                    result = [cache[key]]
                    cache_hit = True
                else:
                    cursor.execute('''SELECT value FROM 'records' WHERE key=?''', query)
                    result = cursor.fetchone()
                    cache_hit = False

            else:
                cursor.execute('''SELECT value FROM 'records' WHERE key=?''', query)
                result = cursor.fetchone()

            if result is not None:
                package = {"exists": "yes", "value": result[0]}

                if caching and not cache_hit:
                    # cache is FIFO
                    if len(cache.keys()) > 250:
                        del cache[cache.keys()[0]]
                        cache[key] = result
                    else:
                        cache[key] = result
            else:
                package = {"exists": "no", "value": "None"}

            response = json.dumps(package)

            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response.encode())
            return

    def do_POST(self):
        connection = sqlite3.connect(dbPath)
        cursor = connection.cursor()
        content_len = int(self.headers.get('Content-Length'))
        post_body = self.rfile.read(content_len)

        decoded_body = post_body.decode()
        body = json.loads(decoded_body)
        route = urlparse(self.path)
        # direct write request
        if route.path == "/kv739/":
            value = None
            if 'value' in body:
                value = body['value']

            key = body['key']
            query = (key,)
            cursor.execute('''SELECT value FROM 'records' WHERE key=?''', query)
            result = cursor.fetchone()
            if body['method'] == 'get':
                if result is not None:
                    package = {"exists": "yes", "former_value": result[0], "new_value": "[]"}
                else:
                    package = {"exists": "no", "former_value": "[]", "new_value": "[]"}
            else:
                if result is not None and result[0] != value:
                    # don't waste time updating value with same value
                    cursor.execute('''UPDATE 'records' SET value = ? WHERE key = ?''', (value, key))
                    connection.commit()
                else:
                    cursor.execute('''INSERT INTO 'records' VALUES (?, ?)''', (key, value))
                    connection.commit()

                if result is not None:
                    package = {"exists": "yes", "former_value": result[0], "new_value": value}

                    # update the cache
                    cache[key] = result[0]
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
        ip, port, enable_cache = sys.argv[1], sys.argv[2], sys.argv[3]
        connection = sqlite3.connect(dbPath)
        cursor = connection.cursor()

        if enable_cache == "--cache":
            caching = True

        # make sure we only create the table once
        cursor.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='records' ''')
        if cursor.fetchone()[0] == 0:
            cursor.execute('''CREATE TABLE 'records' (key text, value text)''')

        server = ThreadingHTTPServer((ip, int(port)), HandleRequests)
        print('Server initializing, reachable at http://{}:{}'.format(ip, port))
        server.serve_forever()
