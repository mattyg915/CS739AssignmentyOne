package main

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

var map_mutex sync.Mutex

const server_path = "../../service/server.py"
const database_path = "../../service/kv.db"
const nodes_path = "../../service/nodes_local.txt"

func TestShutdown(t *testing.T) {
	if Kv739_shutdown() == 0 {
		t.Errorf("Shutdown returned 0 without init")
	}
}

func TestServerAddress(t *testing.T) {
	if Kv739_init([]string{"aaaaaaaaaaaaa"}) != -1 {
		t.Errorf("Failed Address Test #1")
	}

	if Kv739_init([]string{"aaaaaaaaaaaaaaaa:123asd"}) != -1 {
		t.Errorf("Failed Address Test #2")
	}

	killSwitch := make(chan int)
	// Nuke database and start fresh
	pid := -1
	var err error
	if pid, err = launchServer(server_path, killSwitch, true, "200"); err != nil {
		panic(err)
	}

	//if Kv739_init("127.0.0.1:5000") != 0 {
	//	t.Errorf("Failed Address Test #3")
	//}

	servers := []string{"127.0.0.1:5000"}
	if Kv739_init(servers) != 0 {
		t.Errorf("Failed Address Test #4")
	}

	killSwitch <- 1
	<-killSwitch
	time.Sleep(2 * time.Second)
	if !checkAlive(pid) {
		fmt.Printf("Server died pid: %v\n", pid)
	} else {
		fmt.Printf("Server alive with pid: %v\n", pid)

	}

	//servers := []string{"localhost:5000"}
	//Kv739_init_new(servers)

}

func checkAlive(pid int) bool {
	process, err := os.FindProcess(int(pid))
	if err != nil {
		fmt.Printf("Failed to find process: %s\n", err)
		return false
	} else {
		err := process.Signal(syscall.Signal(0))
		//fmt.Printf("process.Signal on pid %d returned: %v\n", pid, err)
		return err == nil
	}
}

func launchServer(server_exe_path string, killSwitch chan int, killDB bool, cacheSize string) (int, error) {
	if _, err := os.Stat(database_path); err == nil {
		if killDB {
			e := os.Remove(database_path)
			if e != nil {
				return -1, e
			}
		}
	}

	cmdArgs := []string{nodes_path, "0"}
	cmdServer := exec.Command(server_exe_path, cmdArgs...)
	if err := cmdServer.Start(); err != nil {
		log.Printf("Failed to start cmd: %v", err)
		return -1, err
	}

	go func() {
		<-killSwitch
		if er := cmdServer.Process.Kill(); er != nil {
			log.Fatal("failed to kill process: ", er)
		}

		fmt.Printf("Killing process with pid %v\n", cmdServer.Process.Pid)
		cmdServer.Wait()
		var err error
		err = nil
		for {
			err = cmdServer.Process.Signal(syscall.Signal(0))
			if err != nil {
				break
			}
		}

		killSwitch <- 1
	}()

	time.Sleep(2 * time.Second)
	// Check if server is alive using PID
	if checkAlive(cmdServer.Process.Pid) {
		log.Printf("Started server")
	}

	return cmdServer.Process.Pid, nil
}

func TestGetInputParams(t *testing.T) {
	killSwitch := make(chan int)
	// Nuke database and start fresh
	pid := -1
	var err error
	if pid, err = launchServer(server_path, killSwitch, true, "200"); err != nil {
		panic(err)
	}

	if e := Kv739_init([]string{"127.0.0.1:5000"}); e != 0 {
		panic("failed to init server")
	}

	old_val := make([]byte, 2049)
	if e := Kv739_put("val1", "val1", old_val); e == -1 {
		t.Errorf("Failed to insert val; val1 #1")
	}

	start := time.Now()
	if e := Kv739_get("val1", old_val); e != 0 {
		t.Errorf("Failed to retrieve val; val1 #1")
	}

	duration := time.Since(start)
	fmt.Printf("Get duration: %v\n", duration)

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

	if e := Kv739_put("va12312p#1@%`\x12\x01", "val2", old_val); e != -1 {
		t.Errorf("Failed to insert val; val4 #4")
	}

	if e := Kv739_get("va12312p#1@%`\x01", old_val); e != -1 {
		t.Errorf("Failed to retrieve val; val4 #4")
	}

	if e := Kv739_put("va12312p", "val2[]", old_val); e != -1 {
		t.Errorf("Failed to insert val; val5 #5")
	}

	if e := Kv739_put("va12312p", "val2$123&\x123`", old_val); e < 0 {
		t.Errorf("Failed to insert val; val5 #5")
	}

	if er := Kv739_shutdown(); er != 0 {
		t.Errorf("Failed to shutdown")
	}

	killSwitch <- 1
	<-killSwitch
	time.Sleep(2 * time.Second)
	if !checkAlive(pid) {
		fmt.Printf("Server died pid: %v\n", pid)
	} else {
		fmt.Printf("Server alive with pid: %v\n", pid)

	}
}

func TestHalting(t *testing.T) {
	killSwitch := make(chan int)
	var pid int
	var err error
	// Nuke database and start fresh
	if pid, err = launchServer(server_path, killSwitch, true, "200"); err != nil {
		panic(err)
	}

	if e := Kv739_init([]string{"127.0.0.1:5000"}); e != 0 {
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
	<-killSwitch
	time.Sleep(time.Second)
	if !checkAlive(pid) {
		fmt.Printf("Server died pid: %v\n", pid)
	} else {
		fmt.Printf("Server alive with pid: %v\n", pid)
	}

	old_val1 := make([]byte, 2049)
	// Do not nuke database and start fresh
	if pid, err = launchServer(server_path, killSwitch, false, "200"); err != nil {
		panic(err)
	}

	if e := Kv739_init([]string{"127.0.0.1:5000"}); e != 0 {
		panic("failed to init server")
	}

	if e := Kv739_get("val1", old_val1); e != 0 {
		t.Errorf("Failed to retrieve val; val1 #1")
	}

	if string(old_val1[:bytes.IndexByte(old_val1, 0)]) != "val1" {
		t.Errorf("Wrong value: %v", old_val1)
	}

	if e := Kv739_put("val2", "val2", old_val); e == -1 {
		t.Errorf("Failed to insert val; val1 #1")
	}

	killSwitch <- 1
	<-killSwitch
	time.Sleep(time.Second)
	if !checkAlive(pid) {
		fmt.Printf("Server died pid: %v\n", pid)
	} else {
		fmt.Printf("Server alive with pid: %v\n", pid)
	}

	old_val1 = make([]byte, 2049)
	// Do not nuke database and start fresh
	if pid, err = launchServer(server_path, killSwitch, false, "200"); err != nil {
		panic(err)
	}

	if e := Kv739_get("val2", old_val1); e != 0 {
		t.Errorf("Failed to retrieve val; val1 #1")
	}

	if string(old_val1[:bytes.IndexByte(old_val1, 0)]) != "val2" {
		t.Errorf("Wrong value: %v", old_val1)
	}

	if er := Kv739_shutdown(); er != 0 {
		t.Errorf("Failed to shutdown")
	}

	killSwitch <- 1
	<-killSwitch
	time.Sleep(time.Second)
	if !checkAlive(pid) {
		fmt.Printf("Server died pid: %v\n", pid)
	} else {
		fmt.Printf("Server alive with pid: %v\n", pid)
	}

}

func generateRandom() (string, string) {
	str_map := "abcd1234"
	length := len(str_map)
	l := (rand.Int() % 5) + 1
	key := ""
	for i := 0; i < l; i++ {
		idx := rand.Int() % length
		key += string(str_map[idx])
	}

	value := ""
	l = (rand.Int() % 5) + 1
	for i := 0; i < l; i++ {
		idx := rand.Int() % length
		value += string(str_map[idx])
	}

	return key, value
}

func writer(m *map[string]string, wg *sync.WaitGroup) {
	defer wg.Done()
	old_value := make([]byte, 2049)
	for i := 0; i < 20; i++ {
		key, value := generateRandom()
		if err := Kv739_put(key, value, old_value); err == -1 {
			log.Println("Error putting value")
			break
		}
		map_mutex.Lock()
		(*m)[key] = value
		map_mutex.Unlock()

	}

}

func TestLongInput(t *testing.T) {
	key := strings.Repeat("a", 129)
	value := strings.Repeat("a", 2049)

	var pid int
	var err error
	old_val := make([]byte, 2049)
	killSwitch := make(chan int)
	// Nuke database and start fresh
	if pid, err = launchServer(server_path, killSwitch, true, "200"); err != nil {
		panic(err)
	}

	if e := Kv739_init([]string{"127.0.0.1:5000"}); e != 0 {
		panic("failed to init server")
	}

	if e := Kv739_get(key, old_val); e != -1 {
		t.Errorf("Failed to retrieve val; val4 #4")
	}

	if e := Kv739_put(key, value, old_val); e != -1 {
		t.Errorf("Failed to insert val; val4 #4")
	}

	killSwitch <- 1
	<-killSwitch
	time.Sleep(time.Second)
	if !checkAlive(pid) {
		fmt.Printf("Server died pid: %v\n", pid)
	} else {
		fmt.Printf("Server alive with pid: %v\n", pid)
	}

}

/*
func TestConcurrent(t *testing.T) {
	var wg sync.WaitGroup
	killSwitch := make(chan int)
	if err := launchServer(server_path, killSwitch, true, "200"); err != nil {
		panic(err)
	}

	if e := Kv739_init("127.0.0.1:5000"); e != 0 {
		panic("failed to init server")
	}

	m := make(map[string]string)

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go writer(&m, &wg)
	}

	wg.Wait()
	log.Println("Finished waiting for go-routines")
	killSwitch <- 1

	if err := launchServer(server_path, killSwitch, false, "200"); err != nil {
		panic(err)
	}



}
*/
//func TestGet(t *testing.T) {

//	launchServer(address)
