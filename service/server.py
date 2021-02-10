from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from urllib.parse import urlparse
import sys
import os

import sqlite3
import threading

path = os.path.dirname(os.path.abspath(__file__))
dbPath = os.path.join(path, 'kv.db')

class RequestHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    connection = sqlite3.connect(dbPath)
    cursor = connection.cursor()

    def do_GET(self):
        print(threading.currentThread().getName())
        parsed_path = urlparse(self.path)
        # handle read request
        if parsed_path.path == "/kv739/":
            print("request received")
            k = parsed_path.query.split("=")[-1]
            print(k)
            return


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle requests in a separate thread."""

if __name__ == '__main__':
    ip, port = sys.argv[1], sys.argv[2]
    # reconnect to the database
    # key->value

    connection = sqlite3.connect(dbPath)
    cursor = connection.cursor()

    # make sure we only create the table once
    cursor.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='values' ''')
    if cursor.fetchone()[0] == 1:
        cursor.execute('''CREATE TABLE values (key text, value text)''')

    server = ThreadedHTTPServer((ip, int(port)), RequestHandler)
    print('Server initalizing, reachable at http://{}:{}'.format(ip, port))
    server.serve_forever()