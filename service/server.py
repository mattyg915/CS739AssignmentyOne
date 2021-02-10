from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from urllib.parse import urlparse
from socketserver import ThreadingMixIn

import json
import sys
import os

import sqlite3

rec = False
path = os.path.dirname(os.path.abspath(__file__))
dbPath = os.path.join(path, 'kv.db')

class HandleRequests(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        connection = sqlite3.connect(dbPath)
        cursor = connection.cursor()
        route = urlparse(self.path)
        # handle read request
        if route.path == "/kv739/":
            k = route.query.split("=")[-1]
            query = (k,)
            cursor.execute('''SELECT value FROM 'records' WHERE key=?''', query)
            result = cursor.fetchone()

            s = {"is_key_in": "yes", "value": result[0]} if result != None else {"is_key_in": "no", "value": "None"}
            response = json.dumps(s)
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header("Content-Length", len(response))
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

            key, value = body['key'], body['value']

            cursor.execute('''INSERT INTO 'records' VALUES (?, ?)''', (key, value))
            connection.commit()

class Server(ThreadingMixIn, HTTPServer):
    """Handle requests in a separate thread."""

if __name__ == '__main__':
    ip, port = sys.argv[1], sys.argv[2]
    connection = sqlite3.connect(dbPath)
    cursor = connection.cursor()

    # make sure we only create the table once
    cursor.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='records' ''')
    if cursor.fetchone()[0] == 0:
        cursor.execute('''CREATE TABLE 'records' (key text, value text)''')

    server = ThreadingHTTPServer((ip, int(port)), HandleRequests)
    print('Server initializing, reachable at http://{}:{}'.format(ip, port))
    server.serve_forever()