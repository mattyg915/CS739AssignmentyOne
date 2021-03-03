package main

import (
	"C"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/valyala/fasthttp"
	"math/rand"
	"strconv"
	"strings"
	"unsafe"
)

var server string
var get_path string
var put_path string
var health_path string

const MAX_SERVER_NUM = int(30)

var init_list = make([]string, MAX_SERVER_NUM)
var no_init = int(0)

var node_list = make([]string, MAX_SERVER_NUM)
var node_no = int(0)

var fastclient *fasthttp.Client
var strPost = []byte("POST")
var strGet = []byte("GET")
var has_init = int(0)

type KeyValue struct {
	Exists      string `json:"exists"`
	FormerValue string `json:"former_value"`
	NewValue    string `json:"new_value"`
}

type NodeList struct {
	NodeList []string `json: "nodes"`
}

type Reachable struct {
	Server    string   `json:"server"`
	Reachable []string `json:"reachable"`
}

func kv739_init_old(server_name *C.char) int32 {
	server = C.GoString(server_name)
	//TODO: Error handling for the server here
	err := validateServer(server)
	if err != nil {
		fmt.Println("Invalid address format: ", err)
		return -1
	}

	fastclient = &fasthttp.Client{}

	get_path = "http://" + server + "/kv739/"
	put_path = "http://" + server + "/kv739/"
	health_path = "http://" + server + "/health/"

	if err := doConnectionTest(); err == nil {
		has_init = 1
		return 0
	} else {
		fmt.Println("Init Connection error: ", err)
		return -1
	}
}

//export kv739_init
func kv739_init(server_names **C.char) int32 {
	fastclient = &fasthttp.Client{}
	// Max number of servers is MAX_SERVER_NUM
	tmpslice := (*[1 << 30]*C.char)(unsafe.Pointer(server_names))[:MAX_SERVER_NUM:MAX_SERVER_NUM]
	//servers := make([]string, MAX_SERVER_NUM)

	for i, elem := range tmpslice {
		if elem == nil {
			fmt.Println("Reached end of the name array!")
			break
		}

		init_list[i] = C.GoString(elem)
		no_init += 1
		//fmt.Printf("Element: %s\n", s)
	}

	fmt.Printf("The init servers %v\n", init_list)
	// Get the full node list from a seed node
	foundAlive := false
	var node_l NodeList
	for _, server := range init_list[:no_init] {
		nodeList, err := getNodeList(server)
		if err == nil {
			foundAlive = true
			node_l = nodeList
			break
		}
	}

	if !foundAlive {
		fmt.Println("[kv739_init_new] No nodes alive")
		return -1
	}

	copy(node_list[:], node_l.NodeList[:])
	node_no = len(node_l.NodeList)
	has_init = 1

	return 0
}

//export kv739_partition
func kv739_partition(server *C.char, reachable **C.char) int32 {
	srvr := C.GoString(server)
	err := validateServer(srvr)
	if err != nil {
		return -1
	}

	tmpslice := (*[1 << 30]*C.char)(unsafe.Pointer(reachable))[:MAX_SERVER_NUM:MAX_SERVER_NUM]
	reachable_list := make([]string, MAX_SERVER_NUM)
	no_reachable := 0

	for i, elem := range tmpslice {
		if elem == nil {
			fmt.Println("Reached end of the reachable array!")
			break
		}

		reachable_list[i] = C.GoString(elem)
		no_reachable += 1
		//fmt.Printf("Element: %s\n", s)
	}

	s_path := "http://" + srvr + "/partition/"

	var reachabl Reachable
	reachabl.Server = srvr
	reachabl.Reachable = reachable_list

	reqJSON, _ := json.Marshal(reachabl)

	req := fasthttp.AcquireRequest()
	req.SetBody(reqJSON)
	req.Header.SetMethodBytes(strPost)
	req.Header.SetContentType("application/json")
	req.SetRequestURIBytes([]byte(s_path))

	res := fasthttp.AcquireResponse()
	err = fastclient.Do(req, res)
	if err != nil {
		fmt.Println("error:", err)
		return -1
	}

	if string(res.Body()) == "OK" {
		return 0
	} else {
		return -1
	}

}

//export kv739_die
func kv739_die(server *C.char, clean C.int) int32 {
	srvr := C.GoString(server)
	err := validateServer(srvr)
	if err != nil {
		return -1
	}

	c := int(clean)
	var die_path string
	if c == 1 {
		die_path = "http://" + srvr + "/die_clean/"
	} else {
		die_path = "http://" + srvr + "/die/"
	}

	req := fasthttp.AcquireRequest()
	//req.SetBody()
	fmt.Printf("[kv739_die] The die path: %v\n", kv739_die)
	req.Header.SetMethodBytes(strGet)
	req.Header.SetContentType("text/plain")
	req.SetRequestURIBytes([]byte(die_path))

	res := fasthttp.AcquireResponse()
	err = fastclient.Do(req, res)
	if err != nil {
		fmt.Println("error: ", err)
		return -1
	}

	if string(res.Body()) == "DYING" {
		return 0
	} else {
		return -1
	}

}

// Returns the set of nodes reachable from the server
func getNodeList(server string) (NodeList, error) {
	err := validateServer(server)
	if err != nil {
		return NodeList{}, err
	}

	err = doConnectionTestWithServer(server)
	if err != nil {
		return NodeList{}, err
	}

	nodeList_path := "http://" + server + "/nodes/"
	req := fasthttp.AcquireRequest()
	//req.SetBody()
	fmt.Printf("[getNodeList] The server path %v\n", nodeList_path)
	req.Header.SetMethodBytes(strGet)
	req.Header.SetContentType("text/plain")
	req.SetRequestURIBytes([]byte(nodeList_path))

	res := fasthttp.AcquireResponse()
	err = fastclient.Do(req, res)
	if err != nil {
		fmt.Println("error: ", err)
		return NodeList{}, err
	}

	var nodeList NodeList
	fmt.Printf("[getNodeList] The list body: %s\n", res.Body())
	err = json.Unmarshal(res.Body(), &nodeList)
	if err != nil {
		fmt.Println("[getNodeList] Error unmarshalling json")
		return NodeList{}, err
	}

	return nodeList, nil
}

func doConnectionTestWithServer(server string) error {
	req := fasthttp.AcquireRequest()
	//req.SetBody()
	s_path := "http://" + server + "/health/"
	req.Header.SetMethodBytes(strGet)
	req.Header.SetContentType("text/plain")
	req.SetRequestURIBytes([]byte(s_path))

	res := fasthttp.AcquireResponse()
	err := fastclient.Do(req, res)
	if err != nil {
		fmt.Println("error: ", err)
		return err
	}

	if string(res.Body()) == "OK" {
		return nil
	} else {
		return err
	}
}

func doConnectionTest() error {

	req := fasthttp.AcquireRequest()
	//req.SetBody()
	req.Header.SetMethodBytes(strGet)
	req.Header.SetContentType("text/plain")
	req.SetRequestURIBytes([]byte(health_path))

	res := fasthttp.AcquireResponse()
	err := fastclient.Do(req, res)
	if err != nil {
		fmt.Println("error: ", err)
		return err
	}

	if string(res.Body()) == "OK" {
		return nil
	} else {
		return err
	}

}

//export kv739_shutdown
func kv739_shutdown() int32 {
	if has_init == 0 {
		return -1
	}
	has_init = 0
	fastclient.CloseIdleConnections()
	return 0
}

func validateServer(server string) error {
	s := strings.Split(server, ":")
	if len(s) != 2 {
		return errors.New("Separator Error")
	}

	_, err := strconv.Atoi(s[1])
	if err != nil {
		return errors.New("Invalid Port number")
	}

	return nil
}

func validateKey(key string) error {
	if len(key) > 128 {
		return errors.New("Key too long")
	}

	f := func(r rune) bool {

		if r == '[' || r == ']' {
			return true
		}

		if r >= 32 && r <= 126 {
			return false
		} else {
			return true
		}
	}

	if strings.IndexFunc(key, f) != -1 {
		return errors.New("Special character in key")
	}

	return nil
}

func validateValue(value string) error {
	if len(value) > 2048 {
		return errors.New("Value too long")
	}

	f := func(r rune) bool {

		if r == '[' || r == ']' {
			return true
		}

		if r >= 32 && r <= 126 {
			return false
		} else {
			return true
		}
	}

	if strings.IndexFunc(value, f) != -1 {
		return errors.New("Special character in value")
	}

	return nil
}

func randomChoice() string {
	idx := rand.Int() % node_no
	return "http://" + node_list[idx]
}

//export kv739_get
func kv739_get(key *C.char, value *C.char) int32 {
	if has_init == 0 {
		return -1
	}

	k := C.GoString(key)

	err := validateKey(k)
	if err != nil {
		return -1
	}

	for _, server := range node_list[:node_no] {
		ret := do_kv739_get(key, value, server)
		if ret != -1 {
			return ret
		}
	}

	fmt.Println("[kv739_get] No reachable servers")
	return -1
}

func do_kv739_get(key *C.char, value *C.char, server string) int32 {
	/*if has_init == 0 {
		return -1
	}*/

	k := C.GoString(key)
	var err error
	/*err := validateKey(k)
	if err != nil {
		return -1
	}*/

	m := make(map[string]string)
	m["key"] = k
	m["method"] = string("get")

	s_path := "http://" + server + "/kv739/"

	reqJSON, _ := json.Marshal(m)

	req := fasthttp.AcquireRequest()
	req.SetBody(reqJSON)
	req.Header.SetMethodBytes(strPost)
	req.Header.SetContentType("application/json")
	req.SetRequestURIBytes([]byte(s_path))

	res := fasthttp.AcquireResponse()
	err = fastclient.Do(req, res)
	if err != nil {
		fmt.Println("error:", err)
		return -1
	}

	if res.StatusCode() == 400 {
		fmt.Println("Request Error: Invalid Input")
		return -1
	}

	if res.StatusCode() == 500 {
		fmt.Println("Request Error: SQL Error")
		return -1
	}

	//println(string(res.Body()))

	var kv KeyValue
	err = json.Unmarshal(res.Body(), &kv)
	if err != nil {
		fmt.Println("error:", err)
		return -1
	}

	valuePtr := unsafe.Pointer(value)
	//Convert pointer into a slice
	cBuf := (*[2049]byte)(valuePtr)

	if kv.Exists == "yes" {
		// := json.Unmarshal(res.Body(), &kv)
		// Copy go value into
		b := kv.FormerValue
		copy(cBuf[:], []byte(b))
		cBuf[len(b)] = 0x0
		return 0
	} else {
		cBuf[0] = 0x0
		return 1
	}
}

//export kv739_put
func kv739_put(key *C.char, value *C.char, old_value *C.char) int32 {
	if has_init == 0 {
		return -1
	}

	k := C.GoString(key)

	err := validateKey(k)
	if err != nil {
		return -1
	}

	for _, server := range node_list[:node_no] {
		ret := do_kv739_put(key, value, old_value, server)
		if ret != -1 {
			return ret
		}
	}

	fmt.Println("[kv739_get] No reachable servers")
	return -1

}

func do_kv739_put(key *C.char, value *C.char, old_value *C.char, server string) int32 {
	//if has_init == 0 {
	//	return -1
	//}

	var err error
	k := C.GoString(key)
	val := C.GoString(value)

	//err := validateKey(k)
	//if err != nil {
	//	fmt.Println("Invalid Key: ", err)
	//	return -1
	//}

	//err = validateValue(val)
	//if err != nil {
	//	fmt.Println("Invalid Value: ", err)
	//	return -1
	//}

	m := make(map[string]string)
	m["key"] = k
	m["value"] = val
	m["method"] = string("put")

	s_path := "http://" + server + "/kv739/"

	reqJSON, _ := json.Marshal(m)

	req := fasthttp.AcquireRequest()
	req.SetBody(reqJSON)
	req.Header.SetMethodBytes(strPost)
	req.Header.SetContentType("application/json")
	req.SetRequestURIBytes([]byte(s_path))

	res := fasthttp.AcquireResponse()
	err = fastclient.Do(req, res)
	if err != nil {
		fmt.Println("Request Error:", err)
		return -1
	}

	if res.StatusCode() == 400 {
		fmt.Println("Request Error: Invalid Input")
		return -1
	}

	if res.StatusCode() == 500 {
		fmt.Println("Request Error: SQL Error")
		return -1
	}

	//println(string(res.Body()))
	var kv KeyValue
	err = json.Unmarshal(res.Body(), &kv)
	if err != nil {
		fmt.Println("Json Unmarshal Error:", err)
		return -1
	}

	valuePtr := unsafe.Pointer(old_value)
	//Convert pointer into a slice
	cBuf := (*[2049]byte)(valuePtr)

	if kv.Exists == "yes" {
		b := kv.FormerValue
		copy(cBuf[:], []byte(b))
		cBuf[len(b)] = 0x0
		return 0
	} else {
		cBuf[0] = 0x0
		return 1
	}
}

func main() {}
