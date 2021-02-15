package main

import (
	"C"
	"github.com/valyala/fasthttp"
	"unsafe"
	"encoding/json"
	"fmt"
	"errors"
	"strings"
	"strconv"
)

var server string
var get_path string
var put_path string
var fastclient *fasthttp.Client
var strPost = []byte("POST")
var strGet = []byte("GET")

type KeyValue struct {
	Exists string `json:"exists"`
	FormerValue string `json:"former_value"`
	NewValue string `json:"new_value"`
}

//export kv739_init
func kv739_init(server_name *C.char) int32 {
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
	return 0
}

//export kv739_shutdown
func kv739_shutdown() int32 {
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
                if r < 'A' || r > 'z' {
                        return r < '0' || r > '9'
                } else {
                        return false
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
                if r < 'A' || r > 'z' {
			return r < '0' || r > '9'
		} else {
			return false
		}
        }

	if strings.IndexFunc(value, f) != -1 {
                return errors.New("Special character in value")
        }

	return nil
}

//export kv739_get
func kv739_get(key *C.char, value *C.char) int32 {
	k := C.GoString(key)

	err := validateKey(k)
	if err != nil {
		return -1
	}

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
	err = fastclient.Do(req, res)
	if err != nil {
		fmt.Println("error:", err)
		return -1
	}
	println(string(res.Body()))

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
	k := C.GoString(key)
	val := C.GoString(value)

	err := validateKey(k)
	if err != nil {
		fmt.Println("Invalid Key: ", err)
		return -1
	}

	err = validateValue(val)
	if err != nil {
		fmt.Println("Invalid Value: ", err)
		return -1
	}

	m := make(map[string]string)
	m["key"] = k
	m["value"] = val
	m["method"] = string("put")

	reqJSON, _ := json.Marshal(m)

        req := fasthttp.AcquireRequest()
        req.SetBody(reqJSON)
        req.Header.SetMethodBytes(strPost)
        req.Header.SetContentType("application/json")
        req.SetRequestURIBytes([]byte(get_path))

        res := fasthttp.AcquireResponse()
        err = fastclient.Do(req, res)
        if err != nil {
                fmt.Println("Request Error:", err)
                return -1
        }

        println(string(res.Body()))
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


func main() { } 
