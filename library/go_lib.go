package main

import (
	"C"
	"github.com/valyala/fasthttp"
	"unsafe"
	"encoding/json"
)

var server string
var get_path string
var put_path string
var fastclient *fasthttp.Client
var strPost = []byte("POST")
var strGet = []byte("GET")

type KeyValue struct {
	Key string `json:"key"`
	Value string `json:"value"`
}

//export kv739_init
func kv739_init(server_name *C.char) int32 {
	server = C.GoString(server_name)
	// Error handling for the server here

	fastclient = &fasthttp.Client{}

	get_path = "http://" + server + "/kv739/"
	put_path = "http://" + server + "/kv739/"
	return 0
}

//export kv739_shutdown
func kv739_shutdown() int32 {
	fastclient.CloseIdleConnections()
	return 0
}


//export kv739_get
func kv739_get(key *C.char, value *C.char) {
	k := C.GoString(key)
	/*if err := validateKey(k); err != nil {
		panic("Invalid Key")
	}*/
	m := make(map[string]string)
	m["key"] = k
	m["method"] = string("get")

	reqJSON, _ := json.Marshal(m)

	req := fasthttp.AcquireRequest()
	req.SetBody(reqJSON)
	req.Header.SetMethodBytes(strPost)
	req.Header.SetContentType("application/json")
	req.SetRequestURIBytes([]byte(get_path))

	res := fasthttp.AcquireResponse()
	fastclient.Do(req, res)

	println(string(res.Body()))
	b := string(res.Body())
	// Copy go value into
	valuePtr := unsafe.Pointer(value)
	//Convert pointer into a slice
	cBuf := (*[2049]byte)(valuePtr)
	copy(cBuf[:], []byte(b))
	cBuf[len(b)] = 0x0
}


func main() { } 
