#!/usr/bin/python3

from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from urllib.parse import urlparse
from socketserver import ThreadingMixIn
from datetime import datetime

import http.client

import random
import json
import sys
import os

import sqlite3

path = os.path.dirname(os.path.abspath(__file__))

KEEP_RUNNING = True
ENTROPY_MAX = 1
entropy_counter = 0
conns = []

class HandleRequests(BaseHTTPRequestHandler):


    # disable logging
    def log_message(self, format, *args):
        return

    def validate_string(self, string_to_validate):
        try:
            string_to_validate.encode('ASCII')
        except UnicodeEncodeError:
            package = {"error": "invalid char in body"}
            response = json.dumps(package)

            self.send_response(400)
            self.send_header('Content-type', 'application/json')
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response.encode())
            return False

        if '[' in string_to_validate or ']' in string_to_validate:
            package = {"error": "invalid char in body"}
            response = json.dumps(package)

            self.send_response(400)
            self.send_header('Content-type', 'application/json')
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response.encode())
            return False

        return True

    def do_GET(self):
        global KEEP_RUNNING
        global conns
        global node_list
        global node_index
        
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
            #notify all reachable hosts
            node_address = node_list[int(node_index)].split()
            ip = node_address[0]
            port = node_address[1]
            for conn in conns:
                conn.request('GET', '/death_notify', headers = {'host':ip , 'port':port })
                conn.close()
            #Flush all state and die
            #skip record known hosts
            #shutdown()
            #exit()
            KEEP_RUNNING = False
        if route.path == '/die/':
            response = "DYING"
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response.encode())
            
            KEEP_RUNNING = False
            exit()
        if route.path == '/death_notify/':
            #host, port = self.client_address
            host = self.headers.get('host')
            port = self.headers.get('port')
            success = False
            #received a death notification, remove server from reachable
            for i in range(len(node_list)):
                if node_list[i] is None:
                    continue
                parts = node_list[i].split()
                if parts[0] == host and parts[1] == port:
                    conns[i].close()
                    conns[i] = None
                    node_list[i] = None
                    success = True
                    break
                if success:
                    print("successfully removed a server from reachable: host = %s, port = %d" % (host, port))

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

            # validate strings
            valid_string = self.validate_string(key)
            if (valid_string is not True):
                return

            query = (key,)

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

                else:
                    package = {"exists": "no", "former_value": "[]", "new_value": "[]"}
            else:
                value = body['value']
                valid_string = self.validate_string(value)
                if (valid_string is not True):
                    return

                if result is not None:# and result[0] != value:
                    # don't waste time updating value with same value
                    if result[0] != value:
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
            
            if entropy_counter > ENTROPY_MAX:
                # clear up entropy
                self.anti_entropy(cursor)
                entropy_counter = 0
        elif route.path == "/peer_put":
            for data in body:
                # put each data entry into the local db
                value,millisec,key = data
                print([key,value,millisec])
                
                # don't meed to validate strings here, this has been done already
                query = (key,)
                
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
                
                # don't waste time updating value with same value or with a later local timestamp
                if result is not None:
                    if result[0] != value and int(result[1]) < millisec:
                        try:
                                cursor.execute('''UPDATE 'records' SET value = ?, time = ? WHERE key = ?''', (value, millisec, key))
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
                    else:
                        try:
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
        else:
            print("unknown route")

    #def broadcast(self):
    #    return

    def anti_entropy(self, cursor):
        #naive algo: send entire db over
        cursor.execute('''SELECT * FROM 'records' ''')
        alldata = cursor.fetchall()
        alldata_json = json.dumps(alldata)
        connection = random.choice(conns)
        success = False
        attempt_count = 0
        while success is not True and attempt_count < len(conns):
            try:
                attempt_count += 1
                connection.request('POST', '/peer_put', alldata_json, {'Content-Length': len(alldata_json)})
                success = True
            except Exception:
                success = False

        # complex algo
        # select a random peer server and resolve all conflicts (1-way)
        # assume timestamp sorted DB
        
        #share a hash to the entire db to the peer
        #if hashes do not agree:
        # share the latest ENTROPY_MAX records as a temporary .db
        # check complete hash again
        #if hashes still do not agree:
        # share the entire DB

        return

###########end of handle request#####################
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
        global node_index
        global reachable_list
        global conns
        global entropy_counter

        nodes_file, node_index = sys.argv[1], sys.argv[2]
        node_list = readNodes(nodes_file)

        node_address = node_list[int(node_index)].split()
        ip = node_address[0]
        port = node_address[1]
        
        #setup connections to peer servers
        #we have this at each server, so 2 way connections
        for i in range(len(node_list)):
            if i == int(node_index):
                continue
            parts = node_list[i].split()
            conns.append(http.client.HTTPConnection(parts[0], port=parts[1]))
            print('connected to a peer')

        dbName = port + 'kv.db'
        dbPath = os.path.join(path, dbName)

        connection = sqlite3.connect(dbPath, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        cursor = connection.cursor()

        # make sure we only create the table once
        cursor.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='records' ''')
        if cursor.fetchone()[0] == 0:
            cursor.execute('''CREATE TABLE 'records' (key text PRIMARY KEY, value text, time integer )''')

        server = ThreadingHTTPServer((ip, int(port)), HandleRequests)
        print('Server initializing, reachable at http://{}:{}'.format(ip, port))
        #server.serve_forever()
        global KEEP_RUNNING
        try:
            while KEEP_RUNNING:
                server.handle_request()
        except KeyboardInterrupt:
            pass
        finally:
            # Clean-up server (close socket, etc.)
            server.server_close()
