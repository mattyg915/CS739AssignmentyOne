package main

/*
#cgo CFLAGS: -I/u/d/s/dsirone/dist_systems/project2
#cgo LDFLAGS: -L/u/d/s/dsirone/dist_systems/project2 -l:lib739kv.so
#include "739kv_wrapper.h"
#include <stdlib.h>
*/
import "C"

import "unsafe"

/*func Kv739_init(address string) int32 {
	return int32(C.kv739_init(C.CString(address)))
}*/

func Kv739_init(address []string) int32 {
	cArray := C.malloc(C.size_t(len(address)+1) * C.size_t(unsafe.Sizeof(uintptr(0))))
	a := (*[1<<30 - 1]*C.char)(cArray)

	for i, val := range address {
		a[i] = C.CString(val)
	}
	a[len(address)] = C.CString(string([]byte{0x00}))//(*C.char)(unsafe.Pointer(uintptr(0)))


	return int32(C.kv739_init((**C.char)(unsafe.Pointer(cArray))))
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

func Kv739_die(server string, clean int) int32 {
	return int32(C.kv739_die(C.CString(server), C.int(clean)))
}

func Kv739_partition(server string, reachable []string) int32 {
	cArray := C.malloc(C.size_t(len(reachable)+1) * C.size_t(unsafe.Sizeof(uintptr(0))))
        a := (*[1<<30 - 1]*C.char)(cArray)

        for i, val := range reachable {
                a[i] = C.CString(val)
        }
        a[len(reachable)] = C.CString(string([]byte{0x00}))//(*C.char)(unsafe.Pointer(uintptr(0)))

	return int32(C.kv739_partition(C.CString(server), (**C.char)(unsafe.Pointer(cArray))))
}
