package main
/*
#cgo CFLAGS: -I../../library
#cgo LDFLAGS: -L../../library -l:lib739kv.so
#include "lib739kv.h"
#include <stdlib.h>
*/
import "C"
//import "unsafe"

func Kv739_init(address string) int32 {
	return int32(C.kv739_init(C.CString(address)))
}

func Kv739_get(key string, value []byte) int32 {
	p := C.malloc(C.size_t(len(value)))
	defer C.free(p)

	ret := int32(C.kv739_get(C.CString(key), (*C.char)(p)))
	if ret == 0 {
                cBuf := (*[2049]byte)(p)
                copy(value[:], cBuf[:])
        } else {
                value[0] = 0x0
        }

        return ret
}

func Kv739_put(key string, value string, old_value []byte) int32 {
        p := C.malloc(C.size_t(len(old_value)))
        defer C.free(p)

        ret := int32(C.kv739_put(C.CString(key), C.CString(value), (*C.char)(p)))
	if ret == 0 {
		cBuf := (*[2049]byte)(p)
                copy(old_value[:], cBuf[:])
	} else {
		old_value[0] = 0x0
	}

	return ret
}

func Kv739_shutdown() int32 {
	return int32(C.kv739_shutdown())
}
