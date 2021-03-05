#!/usr/bin/python3

from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from urllib.parse import urlparse
from socketserver import ThreadingMixIn
from datetime import datetime

import time
import http.client

import random
import json
import sys
import os
import requests

import sqlite3

path = os.path.dirname(os.path.abspath(__file__))

KEEP_RUNNING = True
ENTROPY_MAX = 1000 #in millisecs
entropy_counter = time.perf_counter() * 1000 #last time anti_entroy was triggered in millisecs

class HandleRequests(BaseHTTPRequestHandler):


    # disable logging
    #def log_message(self, format, *args):
    #    return

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
        global node_dict
        global self_ip
        
        route = urlparse(self.path)
        if route.path == '/health/':
            response = "OK"
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response.encode())
        if route.path == '/nodes/':
            #response = json.dumps({"nodes" : all_list})
            response = json.dumps({"nodes" : node_dict})
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
            port = node_dict(self_ip)
            for node in node_dict.keys():
                try:
                    conn = http.client.HTTPConnection(node, port=node_dict(node))
                    conn.request('GET', '/die_notify', headers = {'host':self_ip , 'port':port })
                    conn.close()
                    print('clean die_notify to '+node)
                except Exception:
                    print('unable to die_notify node '+node)
            KEEP_RUNNING = False
            exit()
        if route.path == '/die/':
            response = "DYING"
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response.encode())
            
            KEEP_RUNNING = False
            shutdown() #not sure if this works
            exit()
        if route.path == '/die_notify/':
            host = self.headers.get('host')
            port = self.headers.get('port')
            #received a death notification, remove server from reachable
            if host in node_dict.keys():
                node_dict.pop(node)
                print('successfully removed a server from reachable: host = %s, port = %s' % (host, port))
            else:
                print('unexpected death notifcation from: host = %s, port = %s' % (host, port))



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
                
                millisec = None
                if result is not None:
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

                #a broadcast of put?
                '''for node in node_dict:
                    key_value = json.dumps([value,millisec,key])
                    url = "http://" + node + "/peer_put"
                    try:
                        conn = http.client.HTTPConnection(node, port=node_dict(node))
                        conn.request('GET', '/peer_put', headers = {'host':self_ip , 'port':port })
                        conn.close()
                        x = requests.post(url, data = key_value)
                        if x.text == "OK":
                            print ("Successfully pushed to " + node)
                        else:
                            print ("Could not push to " + node)
                    except Exception as e:
                        print ("Put server error: {}".format(e))'''


            
            response = json.dumps(package)
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response.encode())
            
            global entropy_counter
            current = time.perf_counter() * 1000 - entropy_counter
            if current > ENTROPY_MAX:
                # clear up entropy
                self.anti_entropy(cursor)
                entropy_counter = time.perf_counter() * 1000
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

                response = "OK"
                self.send_response(200)
                self.send_header('Content-type', 'text/plain')
                self.send_header("Content-Length", str(len(response)))
                self.end_headers()
                self.wfile.write(response.encode())
        elif route.path == "/partition":
            #body should contain a dict of reachable nodes
            #i.e. ['snare-01':'5000', 'royal-01':'5000']
            global node_dict
            node_dict = dict(body)
        else:
            print("unknown route")

    #def broadcast(self):
    #    return

    def anti_entropy(self, cursor):
    
        #naive algo: send entire db over
        cursor.execute('''SELECT * FROM 'records' ''')
        alldata = cursor.fetchall()
        alldata_json = json.dumps(alldata)
        success = False
        attempt_count = 0
        
        global node_dict
        while success is not True and attempt_count < len(node_dict.keys()):
            node = random.choice(node_dict.keys())
            try:
                attempt_count += 1
                connection = http.client.HTTPConnection(node, port=node_dict(node))
                connection.request('POST', '/peer_put', alldata_json, {'Content-Length': len(alldata_json)})
                connection.close()
                success = True
            except Exception:
                node_dict.pop(node)
                print('unable to reach node %s, removed from reachable list... ' %node)
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
        global node_dict
        global self_ip
        global entropy_counter

        nodes_file, node_index = sys.argv[1], sys.argv[2]
        node_list = readNodes(nodes_file)
        
        #record self ip and port
        node_address = node_list[int(node_index)].split(':')
        self_ip = node_address[0]
        port = node_address[1]
        node_dict = dict()
        for i in range(len(node_list)):
            parts = node_list[i].split(':')
            ip = parts[0]
            port = parts[1]
            node_dict[ip] = port
        print(node_dict)
        
        #remove self from nodelist
        node_dict.pop(self_ip)
        
        
        
        dbName = port + 'kv.db'
        dbPath = os.path.join(path, dbName)

        connection = sqlite3.connect(dbPath, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        cursor = connection.cursor()

        # make sure we only create the table once
        cursor.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='records' ''')
        if cursor.fetchone()[0] == 0:
            cursor.execute('''CREATE TABLE 'records' (key text PRIMARY KEY, value text, time integer )''')

        server = ThreadingHTTPServer((self_ip, int(port)), HandleRequests)
        print('Server initializing, reachable at http://{}:{}'.format(self_ip, port))
        #server.serve_forever()
        
        entropy_counter = time.perf_counter() * 1000
        
        global KEEP_RUNNING
        try:
            while KEEP_RUNNING:
                server.handle_request()
        except KeyboardInterrupt:
            pass
        finally:
            # Clean-up server (close socket, etc.)
            server.server_close()
