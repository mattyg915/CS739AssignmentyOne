package main

import "testing"

const address = "<address>:port"

func TestServerAddress(t *testing.T) {
	if Kv739_init("aaaaaaaaaaaaa") != -1 {
		t.Errorf("Failed Address Test #1")
	}

	if Kv739_init("aaaaaaaaaaaaaaaa:123asd") != -1 {
		t.Errorf("Failed Address Test #2")
	}

	if Kv739_init("127.0.0.1:5000") != 0 {
		t.Errorf("Failed Address Test #3")
	}
}

//func TestGet(t *testing.T) {
//	launchServer(address)

