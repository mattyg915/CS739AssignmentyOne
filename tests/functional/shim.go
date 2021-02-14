package main
/*
#cgo CFLAGS: -I../../library
#cgo LDFLAGS: -L../../library -l:lib739kv.so
#include "lib739kv.h"
#include <stdlib.h>
*/
import "C"
import "unsafe"

func Kv739_init(address string) int32 {
	return int32(C.kv739_init(C.CString(address)))
}

func Kv739_get(key string, value []byte) int32 {
	p := C.malloc(C.size_t(len(value)))
	defer C.free(p)

	return 0
	//ret := int32(C.kv739_get(C.CString(key), unsafe.Pointer(p)))
	
}
