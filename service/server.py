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
ENTROPY_MAX = 60  # in seconds
entropy_lock = False  # in case the previous anti entropy is unfinished, do not start the next yet
node_set = set()
node_list = set()
deadnode_set = set()
self_ip = ''
self_port = -1
entropy_counter = 0
quorum = 0


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
        global node_set

        # reflects initial list only
        global node_list

        global self_ip
        global self_port
        global deadnode_set
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
            response = json.dumps({"nodes": node_list})
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response.encode())
            print(response)
        if route.path == '/die_clean/':
            print("dying clean")
            global KEEP_RUNNING

            for node in node_set:
                try:
                    r = requests.get('http://' + node + '/die_notify/', headers={'host': self_ip, 'port': self_port})
                    if r.status_code == 200:
                        print('clean die_notify to '+node)
                except Exception as e:
                    print('unable to die_notify node ' + node + ' error: '.format(e))
            response = "DYING"
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response.encode())

            KEEP_RUNNING = False
            server.shutdown()
        if route.path == '/die/':
            response = "DYING"
            print("dying")

            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response.encode())
            
            KEEP_RUNNING = False
            server.shutdown()
            sys.exit()
            
        if route.path == '/die_notify/':
            print("received die notify")

            response = "OK"
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response.encode())

    def do_POST(self):
        global node_set
        global deadnode_set
        global self_ip
        global self_port
        try:
            connection = sqlite3.connect(dbPath, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        except Exception as e:
            package = {"error": "error opening database connection: ".format(e)}
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
            print("request with method " + method)
            if (method != 'get' and method != 'peer_get' and method != 'delete') and 'value' not in body:
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
                cursor.execute('''SELECT value, time FROM 'records' WHERE key=?''', query)
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

            if method == 'get':
                peer_results = dict()
                if result is not None:
                    cur_value = result[0]
                    cur_timestamp = int(result[1])
                else:
                    cur_value = None
                    cur_timestamp = 0

                # broadcast get to other servers to make sure no value has a higher timestamp
                for node in node_set:
                    get_package = {"key": key, "method": "peer_get"}
                    url = "http://" + node + "/kv739/"
                    try:
                        res = requests.post(url, data=json.dumps(get_package))
                        if res.status_code == 200:
                            result_body = res.json()
                            peer_result = result_body["former_value"]
                            peer_timestamp = result_body["timestamp"]
                            if int(peer_timestamp) > cur_timestamp:
                                if peer_result == "[]":
                                    cur_value = None
                                    cur_timestamp = peer_timestamp
                        else:
                            print("Could not access " + node)
                    except Exception as e:
                        print("Put server error: {}".format(e))

                if cur_value is None:
                    package = {"exists": "no", "former_value": "[]", "new_value": "[]"}
                else:
                    package = {"exists": "yes", "former_value": cur_value, "new_value": "[]"}

            elif method == 'peer_get':
                if result is not None:
                    package = {"exists": "yes", "former_value": result[0], "timestamp": result[1], "new_value": "[]"}
                else:
                    package = {"exists": "no", "former_value": "[]", "new_value": "[]"}

            elif method == 'delete':
                print("execute delete")
                cursor.execute('''DELETE FROM 'records' WHERE key = ?''', (key,))

                connection.commit()
                package = {"key": key, "status": "deleted"}
                response = json.dumps(package)
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.send_header("Content-Length", str(len(response)))
                self.end_headers()
                self.wfile.write(response.encode())
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

                new_millisec = int((datetime.utcnow() - datetime(1970, 1, 1)).total_seconds() * 1000)
                if result is not None:
                    # don't waste time updating value with same value
                    if result[0] != value:
                        try:
                            print("broadcast put update")
                            millisec = int((datetime.utcnow() - datetime(1970, 1, 1)).total_seconds() * 1000)
                            broadcast_successes = 0
                            old_value = result[0]
                            old_millisec = result[1]

                            for node in node_set:
                                key_value = {"key": key, "value": value, "millisec": millisec, "method": "put_server"}
                                url = "http://" + node + "/kv739/"
                                try:
                                    x = requests.post(url, data=json.dumps(key_value))
                                    if x.status_code == 200:
                                        print("Successfully pushed update to " + node)
                                        broadcast_successes += 1
                                    else:
                                        print("Could not push update to " + node)
                                except Exception as e:
                                    print("Put server error: {}".format(e))

                            if broadcast_successes >= quorum:
                                cursor.execute('''UPDATE 'records' SET value = ?, time = ? WHERE key = ?''',
                                               (value, millisec, key))
                                connection.commit()
                            else:
                                # undo commit
                                cursor.execute('''UPDATE 'records' SET value = ?, time = ? WHERE key = ?''',
                                               (old_value, old_millisec, key))
                                # and broadcast the commit undo
                                for node in node_set:
                                    key_value = {"key": key, "value": old_value, "millisec": old_millisec,
                                                 "method": "put_server"}
                                    url = "http://" + node + "/kv739/"
                                    try:
                                        x = requests.post(url, data=json.dumps(key_value))
                                        if x.status_code == 200:
                                            print("Successfully pushed undo commit to " + node)
                                            broadcast_successes += 1
                                        else:
                                            print("Could not push undo commit to " + node)
                                    except Exception as e:
                                        print("Put server error: {}".format(e))

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
                        # do not accept server put from unreachable nodes
                        node = self.headers.get('host')
                        if node not in node_set and node in deadnode_set:
                            print('Invalid server put from node %s!' % node)
                            response = "Forbidden"
                            self.send_response(403)
                            self.send_header('Content-type', 'text/plain')
                            self.send_header("Content-Length", str(len(response)))
                            self.end_headers()
                            self.wfile.write(response.encode())
                            return
                        new_millisec = int(body["millisec"])

                    try:
                        cursor.execute('''INSERT INTO 'records' VALUES (?, ?, ?)''', (key, value, new_millisec))
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

                if method == 'put' and result is None:
                    broadcast_successes = 0
                    print("broadcast put new value")

                    for node in node_set:
                        key_value = {"key": key, "value": value, "millisec": new_millisec, "method": "put_server"}
                        url = "http://" + node + "/kv739/"
                        try:
                            x = requests.post(url, data=json.dumps(key_value))
                            if x.status_code == 200:
                                print("Successfully pushed to " + node)
                                broadcast_successes += 1
                            else:
                                print("Could not push to " + node)
                        except Exception as e:
                            print("Put server error: {}".format(e))

                    if broadcast_successes < quorum:
                        print("write quorum failed")
                        cursor.execute('''DELETE FROM 'records' WHERE key = ?''', (key,))
                        connection.commit()
                        for node in node_set:
                            key_value = {"key": key, "method": "delete"}
                            url = "http://" + node + "/kv739/"
                            try:
                                x = requests.post(url, data=json.dumps(key_value))
                                if x.status_code == 200:
                                    print("Successfully pushed delete to " + node)
                                else:
                                    print("Could not push delete to " + node)
                            except Exception as e:
                                print("Put server error: {}".format(e))

            response = json.dumps(package)
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response.encode())
        elif route.path == "/peer_put/":
            print('received peer_put...')
            
            # accept valid peer put only
            node = self.headers.get('host')

            if node not in node_set and node in deadnode_set:
                print('Dead node %s at port %s has resurrected...' % node)
                node_set.add(node)
                deadnode_set.remove(node)

            for data in body:
                # put each data entry into the local db
                key, value, millisec = data

                # don't meed to validate strings here, this has been done already
                query = (key,)
                
                try:
                    cursor.execute('''SELECT value, time FROM 'records' WHERE key=?''', query)
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
            # body should contain a dict of reachable nodes
            # i.e. ['snare-01':'5000', 'royal-01':'5000']
            if 'reachable' in body:
                for node in body['reachable']:
                    if node == '':
                        continue
                    ip, port = node.split(':')
                    # don't add self
                    if ip != self_ip != ip or port != self_port:
                        node_set.add(node)

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
    
    global node_set
    while success is not True and attempt_count < len(node_set):
        
        node = random.choice(list(node_set))
        try:
            parts = node.split(':')
            ip = parts[0]
            port = parts[1]

            attempt_count += 1
            connection = http.client.HTTPConnection(ip, port=int(port))
            connection.request('POST', '/peer_put/', alldata_json, {'Content-Length': len(alldata_json), 'host': self_ip, 'port': self_port})
            http_response = connection.getresponse()
            if http_response.status == 200:
                print('Executed anti entropy with node %s' % node)
            else:
                print('Error executing anti entropy with node %s, response status %d' % (node, http_response.status))
            
            connection.close()
            success = True
        except Exception as e:
            print('[AntiEntropy] Unable to reach node %s: {}'.format(e) % node)
            success = False
    return


def anti_entropy_wrapper():
    # give some time to start peer servers
    time.sleep(5)

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
    except Exception as e:
        print("error opening database connection: {}".format(e))
    return
        

def readNodes(nodes_file):
    file = open(nodes_file, "r")
    temp_list = []
    for line in file.readlines():
        temp_list += [line.strip(' ').strip('\n')]
    return temp_list


class Server(ThreadingMixIn, HTTPServer):
    if __name__ == '__main__':
        global dbPath
        global node_set
        global self_ip
        global self_port
        global node_list
        global entropy_counter
        global ENTROPY_MAX
        global server
        global quorum

        nodes_file, node_index = sys.argv[1], sys.argv[2]
        if len(sys.argv) == 4:
            ENTROPY_MAX = float(sys.argv[3])
        print('default anti entropy interval: %d s' % ENTROPY_MAX)
        node_list = readNodes(nodes_file)
        
        # record self ip and port
        node_address = node_list[int(node_index)].split(':')
        self_ip = node_address[0]
        self_port = node_address[1]
        for node in node_list:
            node_set.add(node)
        print(node_set)

        # set quorum value
        quorum = (len(node_set) // 2) + 1

        # remove self from nodelist
        node_set.remove(node_list[int(node_index)])

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
        
        # start auto anti entropy
        t = threading.Thread(target=anti_entropy_wrapper, daemon=True)
        t.start()
        
        # start server
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            # Clean-up server (close socket, etc.)
            # t.join()
            server.server_close()
