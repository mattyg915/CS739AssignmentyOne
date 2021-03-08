#!/usr/bin/python3

from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from urllib.parse import urlparse
from socketserver import ThreadingMixIn
from datetime import datetime

import http.client
import threading
import time

import random
import json
import sys
import os
import requests

import sqlite3


path = os.path.dirname(os.path.abspath(__file__))

dbPath = ""
KEEP_RUNNING = True
ENTROPY_MAX = 1  # in seconds
# entropy_counter = None #last time anti_entroy was triggered in millisecs
entropy_lock = False  # in case the previous anti entropy is unfinished, do not start the next yet
node_dict = dict()
deadnode_dict = dict()
self_ip = ''
self_port = -1



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
        global node_dict
        #reflects initial list only
        global node_list
        global self_ip
        global self_port
        global deadnode_dict
        global server
        
        route = urlparse(self.path)
        if route.path == '/health/':
            response = "OK"
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response.encode())
        if route.path == '/nodes/':
            #node_list = []
            #for node in node_dict.keys():
            #    node_list.append(node+':'+node_dict[node])
            response = json.dumps({"nodes": node_list})
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response.encode())
            print(response)
        if route.path == '/die_clean/':
            print("dying clean")
            #response = "DYING"
            #self.send_response(200)
            #self.send_header('Content-type', 'text/plain')
            #self.send_header("Content-Length", str(len(response)))
            #self.end_headers()
            #self.wfile.write(response.encode())
            # notify all reachable hosts
            for node in node_dict.keys():
                try:
                    #try only once
                    #conn = http.client.HTTPConnection(node, port=node_dict[node])
                    r = requests.get('http://' + node + ":" + node_dict[node] + '/die_notify/', headers={'host': self_ip, 'port': self_port})
                    if r.status_code == 200:
                        print('clean die_notify to '+node)
                except Exception:
                    print('unable to die_notify node '+node)
            response = "DYING"
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response.encode())
            
            server.shutdown()
            #threading.Thread(target = server.shutdown, daemon=True).start()
        if route.path == '/die/':
            response = "DYING"
            print("dying")
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response.encode())
            
            #server.shutdown()
            sys.exit()
            #threading.Thread(target = server.shutdown, daemon=True).start()
        if route.path == '/die_notify/':
            print("received die notify")
            host = self.headers.get('host')
            port = self.headers.get('port')
            # received a death notification, remove server from reachable
            #if host in node_dict.keys():
            #    node_dict.pop(host)
            #    deadnode_dict[host] = port
            #    print('successfully removed a server from reachable: host = %s, port = %s' % (host, port))
            #else:
            #    print('unexpected death notifcation from: host = %s, port = %s' % (host, port))
            response = "OK"
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response.encode())


    def do_POST(self):
        global node_dict
        global deadnode_dict
        global self_ip
        global self_port
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
            print("request with method"+method)
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
            if valid_string is not True:
                package = {"error": "invalid key"}
                response = json.dumps(package)
                self.send_response(400)
                self.send_header('Content-type', 'application/json')
                self.send_header("Content-Length", str(len(response)))
                self.end_headers()
                self.wfile.write(response.encode())
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

            # cursor.execute('''SELECT value FROM 'records' WHERE key=?''', query)
            # result = cursor.fetchone()
            if method == 'get':
                if result is not None:
                    package = {"exists": "yes", "former_value": result[0], "new_value": "[]"}

                else:
                    package = {"exists": "no", "former_value": "[]", "new_value": "[]"}
            else:
                value = body['value']
                valid_string = self.validate_string(value)
                if valid_string is not True:
                    
                    package = {"error": "invalid value"}
                    response = json.dumps(package)
                    self.send_response(400)
                    self.send_header('Content-type', 'application/json')
                    self.send_header("Content-Length", str(len(response)))
                    self.end_headers()
                    self.wfile.write(response.encode())
                    return
                
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
                    if method == 'put_server':
                        #do not accept server put from unreachable nodes
                        print('Invalid server put from node %s at port %d!' % (ip, port))
                        response = "Frobidden"
                        self.send_response(403)
                        self.send_header('Content-type', 'text/plain')
                        self.send_header("Content-Length", str(len(response)))
                        self.end_headers()
                        self.wfile.write(response.encode())
                        return
                
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

                if method == 'put':
                    print ("broadcast put")
                    # a broadcast of put
                    for node in node_dict.keys():
                        key_value = {"key": key, "value": value, "method": "put_server", 'host': self_ip, 'port': self_port}
                        url = "http://" + node + ":" + node_dict[node] + "/kv739/"
                        try:
                            x = requests.post(url, json = key_value)
                            if x.status_code == 200:
                                print ("Successfully pushed to " + node)
                            else:
                                print ("Could not push to " + node)
                        except Exception as e:
                             print ("Put server error: {}".format(e))

            response = json.dumps(package)
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response.encode())
        elif route.path == "/peer_put":
            print('received peer_put...')
            
            #accept valide peer put only
            ip = self.headers.get('host')
            port = self.headers.get('port')
            if ip not in node_dict:
                #if ip in deadnode_dict and port == deadnode_dict[ip]:
                #print('Dead node %s at port %s has resurrected...' % (ip, port))
                #deadnode_dict.pop(ip)
                    node_dict[ip] = port
                #else:
                #    print('Invalid node %s at port %s!' % (ip, port))
                #    response = "Frobidden"
                #    self.send_response(403)
                #    self.send_header('Content-type', 'text/plain')
                #    self.send_header("Content-Length", str(len(response)))
                #    self.end_headers()
                #    self.wfile.write(response.encode())
                #    return
            
            
            for data in body:
                # put each data entry into the local db
                # print('data:'+str(data))
                key, value, millisec = data
                # print([key,value,millisec])
                
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
        elif route.path == "/partition/":
            print("partitioning")
            #print(body)
            # body should contain a dict of reachable nodes
            # i.e. ['snare-01':'5000', 'royal-01':'5000']
            if 'reachable' in body:
                node_dict = dict()
                for node in body['reachable']:
                    if node == '':
                        continue
                    ip, port = node.split(':')
                    #don't add self
                    if ip != self_ip != ip or port != self_port:
                        node_dict[ip] = port
                response = "OK"
                self.send_response(200)
                self.send_header('Content-type', 'text/plain')
                self.send_header("Content-Length", str(len(response)))
                self.end_headers()
                self.wfile.write(response.encode())
            else:
                print('error: reachable list not found')
        else:
            print("unknown route")                
            response = "Bad request"
            self.send_response(400)
            self.send_header('Content-type', 'text/plain')
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response.encode())


# ------------ end of handle request ------------
def anti_entropy(cursor):
    # naive algo: send entire db over
    cursor.execute('''SELECT * FROM 'records' ''')
    alldata = cursor.fetchall()
    alldata_json = json.dumps(alldata)
    success = False
    attempt_count = 0
    
    global node_dict
    while success is not True and attempt_count < len(node_dict):
        try:
            node = random.choice(list(node_dict.keys()))
            attempt_count += 1
            connection = http.client.HTTPConnection(node, port=int(node_dict[node]))
            connection.request('POST', '/peer_put', alldata_json, {'Content-Length': len(alldata_json), 'host': self_ip, 'port': self_port})
            http_response = connection.getresponse()
            if http_response.status == 200:
                print('[AntiEntropy] Executed anti entropy with node %s' % node)
            else:
                print('[AntiEntropy] Error when executing anti entropy with node %s, response status %d' % (node, http_response.status))
            
            connection.close()
            success = True
        except Exception:
            #node_dict.pop(node)
            print('[AntiEntropy] Unable to reach node %s' % node)
            success = False
    # complex algo
    # select a random peer server and resolve all conflicts (1-way)
    # assume timestamp sorted DB    
    # share a hash to the entire db to the peer
    # if hashes do not agree:
    # share the latest ENTROPY_MAX records as a temporary .db
    # check complete hash again
    # if hashes still do not agree:
    # share the entire DB
    return

def anti_entropy_wrapper():
        #give some time to start peer servers
        time.sleep(5)
        connection = sqlite3.connect(dbPath, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        cursor = connection.cursor()
        
        global KEEP_RUNNING
        try:  
            connection = sqlite3.connect(dbPath, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
            cursor = connection.cursor()
            while KEEP_RUNNING:
                time.sleep(ENTROPY_MAX)
                if not KEEP_RUNNING:
                    return
                anti_entropy(cursor)
            return
        except Exception:
            print("error opening database connection")
        return
        

        

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
        global self_port
        global node_list
        global entropy_counter
        global ENTROPY_MAX
        global server

        nodes_file, node_index = sys.argv[1], sys.argv[2]
        if len(sys.argv) == 4:
            ENTROPY_MAX = float(sys.argv[3])
        print('default anti entropy interval: %d s' % ENTROPY_MAX)
        node_list = readNodes(nodes_file)
        
        # record self ip and port
        node_address = node_list[int(node_index)].split(':')
        self_ip = node_address[0]
        self_port = node_address[1]
        for i in range(len(node_list)):
            parts = node_list[i].split(':')
            node_dict[parts[0]] = parts[1]
        print(node_dict)
        
        # remove self from nodelist
        node_dict.pop(self_ip)

        dbName = self_port + 'kv.db'
        dbPath = os.path.join(path, dbName)

        connection = sqlite3.connect(dbPath, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        cursor = connection.cursor()

        # make sure we only create the table once
        cursor.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='records' ''')
        if cursor.fetchone()[0] == 0:
            cursor.execute('''CREATE TABLE 'records' (key text PRIMARY KEY, value text, time integer )''')

        server = ThreadingHTTPServer((self_ip, int(self_port)), HandleRequests)
        print('Server initializing, reachable at http://{}:{}'.format(self_ip, self_port))
        
        entropy_counter = int((datetime.utcnow() - datetime(1970, 1, 1)).total_seconds() * 1000)
        
        #start auto anti entropy
        t = threading.Thread(target=anti_entropy_wrapper, daemon=True)
        t.start()
        
        #start server
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            # Clean-up server (close socket, etc.)
            t.join()
            server.server_close()
