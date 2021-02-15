package main

import (
	"bytes"
	"log"
	"os"
	"os/exec"
	"testing"
	"time"
)

const server_path = "../../service/server.py"
const database_path = "../../service/kv.db"

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

func launchServer(server_exe_path string, killSwitch chan int, killDB bool, cacheSize string) error {
	if _, err := os.Stat(database_path); err == nil {
		if killDB {
			e := os.Remove(database_path)
			if e != nil {
				return e
			}
		}
	}

	cmdArgs := []string{"127.0.0.1", "5000", "--cache", cacheSize}
	cmdServer := exec.Command(server_exe_path, cmdArgs...)
	if err := cmdServer.Start(); err != nil {
		log.Printf("Failed to start cmd: %v", err)
		return err
	}

	go func() {
		<-killSwitch
		if er := cmdServer.Process.Kill(); er != nil {
			log.Fatal("failed to kill process: ", er)
		}
	}()

	time.Sleep(2 * time.Second)
	log.Printf("Started server")
	return nil
}

func TestGetInputParams(t *testing.T) {
	killSwitch := make(chan int)
	// Nuke database and start fresh
	if err := launchServer(server_path, killSwitch, true, "200"); err != nil {
		panic(err)
	}

	if e := Kv739_init("127.0.0.1:5000"); e != 0 {
		panic("failed to init server")
	}

	old_val := make([]byte, 2049)
	if e := Kv739_put("val1", "val1", old_val); e == -1 {
		t.Errorf("Failed to insert val; val1 #1")
	}

	if e := Kv739_get("val1", old_val); e != 0 {
		t.Errorf("Failed to retrieve val; val1 #1")
	}

	if string(old_val[:bytes.IndexByte(old_val, 0)]) != "val1" {
		t.Errorf("Wrong value: %v", old_val)
	}

	if e := Kv739_put("val1", "val2", old_val); e == -1 {
		t.Errorf("Failed to insert val; val2 #2")
	}

	if e := Kv739_get("val1", old_val); e != 0 {
		t.Errorf("Failed to retrieve val; val2 #2")
	}

	if string(old_val[:bytes.IndexByte(old_val, 0)]) != "val2" {
		t.Errorf("Wrong value: %v", old_val)
	}

	if e := Kv739_put("va12312p[]", "val2", old_val); e != -1 {
		t.Errorf("Failed to insert val; val3 #3")
	}

	if e := Kv739_get("va12312p[]", old_val); e != -1 {
		t.Errorf("Failed to retrieve val; val3 #3")
	}

	if e := Kv739_put("va12312p#1@%`", "val2", old_val); e != -1 {
		t.Errorf("Failed to insert val; val4 #4")
	}

	if e := Kv739_get("va12312p#1@%`", old_val); e != -1 {
		t.Errorf("Failed to retrieve val; val4 #4")
	}

	if e := Kv739_put("va12312p", "val2[]", old_val); e != -1 {
		t.Errorf("Failed to insert val; val5 #5")
	}

	if e := Kv739_put("va12312p", "val2$123&`", old_val); e != -1 {
		t.Errorf("Failed to insert val; val5 #5")
	}

	killSwitch <- 1
}

func TestHalting(t *testing.T) {
	killSwitch := make(chan int)
	// Nuke database and start fresh
	if err := launchServer(server_path, killSwitch, true, "200"); err != nil {
		panic(err)
	}

	if e := Kv739_init("127.0.0.1:5000"); e != 0 {
		panic("failed to init server")
	}

	old_val := make([]byte, 2049)
	if e := Kv739_put("val1", "val1", old_val); e == -1 {
		t.Errorf("Failed to insert val; val1 #1")
	}

	if e := Kv739_get("val1", old_val); e != 0 {
		t.Errorf("Failed to retrieve val; val1 #1")
	}

	killSwitch <- 1
	old_val1 := make([]byte, 2049)
	// Do not nuke database and start fresh
	if err := launchServer(server_path, killSwitch, false, "200"); err != nil {
		panic(err)
	}

	if e := Kv739_get("val1", old_val1); e != 0 {
		t.Errorf("Failed to retrieve val; val1 #1")
	}

	if string(old_val1[:bytes.IndexByte(old_val1, 0)]) != "val1" {
		t.Errorf("Wrong value: %v", old_val1)
	}

}

//func TestGet(t *testing.T) {
//	launchServer(address)
