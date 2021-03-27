#!/usr/bin/python3

from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from urllib.parse import urlparse
from socketserver import ThreadingMixIn
from datetime import datetime
from collections import OrderedDict

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
node_set = set() # does not include self
node_list = OrderedDict()  # includes self
deadnode_set = set()
self_ip = ''
self_port = -1
node_leader = ""
node_leader_index = 0
node_self = ""
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

    def check_cluster_health(self):
        # return list of nodes that are healthy
        healthy_list = OrderedDict()
        index = 0
        for node in node_set:
            package = {}
            url = "http://" + node + "/health/"
            try:
                res = requests.get(url, data=json.dumps(package))
                if res.status_code == 200:
                    healthy_list[index] = node
                    index += 1
            except Exception as e:
                print("Error sending leader elect message: {}".format(e))

        return healthy_list

    def elect_leader(self):
        # elect and broadcast new leader
        global node_leader_index
        global node_leader
        global KEEP_RUNNING

        # first make sure a quorum is still possible
        healthy_list = self.check_cluster_health()
        if len(healthy_list) < quorum - 1: # - 1 because self is a healthy node
            print("failed to elect new leader, quorum impossible, system shutdown")
            for node in node_set:
                package = {}
                url = "http://" + node + "/die/"
                try:
                    requests.get(url, data=json.dumps(package))
                except Exception as e:
                    print("Error killing: {}".format(e))
            KEEP_RUNNING = False
            server.shutdown()

        leader_elected = False
        while leader_elected is False:
            if node_leader_index == (len(node_list) - 1):
                node_leader_index = 0
            else:
                node_leader_index += 1
            node_leader = node_list[node_leader_index]

            # to simplify this, nodes cannot elect themselves
            if node_leader == node_self:
                continue

            package = {"node_leader_index": node_leader_index, "method": "new_leader_notify"}
            url = "http://" + node_leader + "/kv739/"
            try:
                res = requests.post(url, data=json.dumps(package))
                if res.status_code == 200 and node_leader in res.url:
                    leader_elected = True
            except Exception as e:
                print("Error sending leader elect message: {}".format(e))

        if (leader_elected):
            for node in node_set:
                if node == node_leader:
                    continue
                package = {"node_leader_index": node_leader_index, "method": "new_leader_notify"}
                url = "http://" + node + "/kv739/"
                try:
                    requests.post(url, data=json.dumps(package))
                except Exception as e:
                    print("Error sending leader elect message: {}".format(e))
        else:
            print("failed to elect new leader system shutdown")
            for node in node_set:
                package = {}
                url = "http://" + node + "/die/"
                try:
                    requests.get(url, data=json.dumps(package))
                except Exception as e:
                    print("Error killing: {}".format(e))
            KEEP_RUNNING = False
            server.shutdown()

        return

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
        global node_leader
        global node_leader_index

        content_len = int(self.headers.get('Content-Length'))
        post_body = self.rfile.read(content_len)
        decoded_body = post_body.decode()
        body = json.loads(decoded_body)

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
            if 'method' in body and body['method'] == 'new_leader_notify':
                node_leader_index = body["node_leader_index"]
                node_leader = node_list[node_leader_index]
                package = {
                    "success": "new leader elected at index: {}".format(node_leader_index)}
                response = json.dumps(package)

                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.send_header("Content-Length", str(len(response)))
                self.end_headers()
                self.wfile.write(response.encode())
                return

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
            if (method != 'get' and method != 'peer_get') and 'value' not in body:
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
                # only the leader can handle GETs
                if (node_self != node_leader):
                    leader_url = "http://" + node_leader + "/kv739/"
                    get_package = {"key": key, "method": "get"}
                    try:
                        res = requests.post(leader_url, data=json.dumps(get_package))
                        result_body = res.json()
                        response = json.dumps(result_body)
                        self.send_response(200)
                        self.send_header('Content-type', 'application/json')
                        self.send_header("Content-Length", str(len(response)))
                        self.end_headers()
                        self.wfile.write(response.encode())
                        return
                    except Exception as e:
                        # Error communicating with leader, reject and reassign leader
                        message = "Internal server error: {}".format(e)
                        package = {"error": message}
                        response = json.dumps(package)

                        self.send_response(500)
                        self.send_header('Content-type', 'application/json')
                        self.send_header("Content-Length", str(len(response)))
                        self.end_headers()
                        self.wfile.write(response.encode())

                        self.elect_leader()
                        return

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
                        print("peer_get error: {}".format(e))

                if cur_value is None:
                    package = {"exists": "no", "former_value": "[]", "new_value": "[]"}
                else:
                    package = {"exists": "yes", "former_value": cur_value, "new_value": "[]"}

            elif method == 'peer_get':
                if result is not None:
                    package = {"exists": "yes", "former_value": result[0], "timestamp": result[1], "new_value": "[]"}
                else:
                    package = {"exists": "no", "former_value": "[]", "new_value": "[]"}

            elif method == 'put' or method == 'peer_put':
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

                # only the leader can handle a put
                if method == 'put' and node_self != node_leader:
                    leader_url = "http://" + node_leader + "/kv739/"
                    put_package = {"key": key, "value": value, "method": "put"}
                    try:
                        res = requests.post(leader_url, data=json.dumps(put_package))
                        result_body = res.json()
                        response = json.dumps(result_body)
                        self.send_response(200)
                        self.send_header('Content-type', 'application/json')
                        self.send_header("Content-Length", str(len(response)))
                        self.end_headers()
                        self.wfile.write(response.encode())
                        return
                    except Exception as e:
                        # Error communicating with leader, reject and reassign leader
                        message = "Internal server error: {}".format(e)
                        package = {"error": message}
                        response = json.dumps(package)

                        self.send_response(500)
                        self.send_header('Content-type', 'application/json')
                        self.send_header("Content-Length", str(len(response)))
                        self.end_headers()
                        self.wfile.write(response.encode())

                        self.elect_leader()
                        return

                if result is not None and method != "peer_put":
                    try:
                        print("broadcast put update")
                        healthy_nodes = self.check_cluster_health()
                        if len(healthy_nodes) < (quorum - 1):
                            message = "Cannot achieve write quorum"
                            package = {"error": message}
                            response = json.dumps(package)

                            self.send_response(500)
                            self.send_header('Content-type', 'application/json')
                            self.send_header("Content-Length", str(len(response)))
                            self.end_headers()
                            self.wfile.write(response.encode())
                            return

                        millisec = int((datetime.utcnow() - datetime(1970, 1, 1)).total_seconds() * 1000)
                        cursor.execute('''UPDATE 'records' SET value = ?, time = ? WHERE key = ?''',
                                       (value, millisec, key))
                        connection.commit()

                        for node in healthy_nodes.values():
                            key_value = {"key": key, "value": value, "millisec": millisec, "method": "peer_put"}
                            url = "http://" + node + "/kv739/"
                            try:
                                x = requests.post(url, data=json.dumps(key_value))
                                if x.status_code == 200:
                                    print("Successfully pushed update to " + node)
                                else:
                                    print("Could not push update to " + node)
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
                    if method == 'peer_put':
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
                        key_value = {"key": key, "value": value, "millisec": new_millisec, "method": "peer_put"}
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
        elif route.path == "/anti_entropy_put/":
            print('received anti_entropy_put...')
            
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
            connection.request('POST', '/anti_entropy_put/', alldata_json, {'Content-Length': len(alldata_json), 'host': self_ip, 'port': self_port})
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
        global node_self
        global node_leader
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
        node_self = "{}:{}".format(self_ip, self_port)
        node_leader = node_list[node_leader_index]
        for node in node_list:
            node_set.add(node)

        # set quorum value
        quorum = (len(node_set) // 2) + 1

        # remove self from nodelist
        node_set.remove(node_list[int(node_index)])
        print(node_set)
        print(node_list)
        print(node_self)
        if (node_self == node_list[0]):
            print("I am the leader (until I die or you find someone better)")

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
