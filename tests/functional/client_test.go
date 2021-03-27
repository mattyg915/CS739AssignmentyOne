package main

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	//"strings"
	"errors"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"
)

var map_mutex sync.Mutex

const server_path = "../../service/server.py"
const database_path = "./5000kv.db"
const base_path = "/data/projects/phd_stuff/courses/dist_systems/project1/CS739AssignmentyOne/tests/functional"
const nodes_path = "./nodes.txt"

/*
func TestShutdown(t *testing.T) {
	if Kv739_shutdown() == 0 {
		t.Errorf("Shutdown returned 0 without init")
	}
}*/
/*
func TestAvailability3(t *testing.T) {
	chans, pids, err := launchServers(server_path, 3, true)
        if err != nil {
                t.Errorf("Failed to launch servers")
        }

	// localhost:5000
        if e := Kv739_init([]string{"localhost:5000", "localhost:5001", "localhost:5002"}); e != 0 {
                panic("failed to init server")
        }

	if e := Kv739_partition("localhost:5000", []string{}); e == -1 {
                t.Errorf("Failed to partition 5000")
        }

	if e := Kv739_partition("localhost:5001", []string{}); e == -1 {
                t.Errorf("Failed to partition 5001")
        }

	if e := Kv739_partition("localhost:5002", []string{}); e == -1 {
                t.Errorf("Failed to partition 5002")
        }

	// localhost:5000
	//if e := Kv739_init([]string{"localhost:5000"}); e != 0 {
        //        panic("failed to init server")
        //}

        old_val := make([]byte, 2049)
        if e := Kv739_put("val2", "val2", old_val); e == -1 {
                t.Errorf("Failed to insert val; val1 #1")
        }

        if e := Kv739_get("val2", old_val); e == -1 {
                t.Errorf("Failed to get val; val1 #1")
        }

        if string(old_val[:bytes.IndexByte(old_val, 0)]) != "val2" {
                t.Errorf("Wrong value: %v", old_val)
        }

	if e := Kv739_shutdown(); e != 0 {
                panic("failed to shutdown")
        }

	//localhost:5001
        if e := Kv739_init([]string{"localhost:5001"}); e != 0 {
                panic("failed to init server")
        }

        //old_val := make([]byte, 2049)
        //*if e := Kv739_put("val2", "val2", old_val); e == -1 {
                t.Errorf("Failed to insert val; val1 #1")
        //}

         e1 := Kv739_get("val2", old_val)

	 if e := Kv739_shutdown(); e != 0 {
                panic("failed to shutdown")
        }


	if e := Kv739_init([]string{"localhost:5002"}); e != 0 {
                panic("failed to init server")
        }

	e2 := Kv739_get("val2", old_val)


        //if string(old_val[:bytes.IndexByte(old_val, 0)]) != "val2" {
        //        t.Errorf("Wrong value: %v", old_val)
        //}

        if e := Kv739_shutdown(); e != 0 {
                panic("failed to shutdown")
        }


	if e := Kv739_init([]string{"localhost:5000"}); e != 0 {
                panic("failed to init server")
        }

        e3 := Kv739_get("val2", old_val)


        //if string(old_val[:bytes.IndexByte(old_val, 0)]) != "val2" {
        //        t.Errorf("Wrong value: %v", old_val)
        //}

        if e := Kv739_shutdown(); e != 0 {
                panic("failed to shutdown")
        }

	if e1 != 1 && e2 != 1 && e3 != 1 {
		t.Errorf("Partition failure")
	}


	killServers(chans, pids)


}*/

func TestServerAddress(t *testing.T) {
	/*if Kv739_init([]string{"aaaaaaaaaaaaa"}) != -1 {
		t.Errorf("Failed Address Test #1")
	}*/

	/*if Kv739_init([]string{"aaaaaaaaaaaaaaaa:123asd"}) != -1 {
		t.Errorf("Failed Address Test #2")
	}*/

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

	servers := []string{"localhost:5000"}
	if Kv739_init(servers) != 0 {
		t.Errorf("Failed Address Test #4")
	}

	if Kv739_shutdown() != 0 {
		t.Errorf("Failed Shutdown")
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

func launchServers(server_exe_path string, num_servers int, killDB bool) ([]chan int, []int, error) {
	killChans := make([]chan int, num_servers)
	pids := make([]int, num_servers)
	for i := 0; i < num_servers; i++ {
		killChans[i] = make(chan int)
		pid, err := launchServerIndex(server_exe_path, i, num_servers, killChans[i], killDB)
		pids[i] = pid
		if err != nil {
			fmt.Printf("[launchServers] Error creating server with index %v\n", i)
			for j := 0; j < i; j++ {
				killChans[j] <- 1
				<-killChans[j]
			}

			return killChans, []int{}, errors.New("Error launching server " + strconv.Itoa(i))
		}
	}

	return killChans, pids, nil
}

func launchServerIndex(server_exe_path string, idx int, num_servers int, killSwitch chan int, killDB bool) (int, error) {
	if _, err := os.Stat(base_path + strconv.Itoa(5000+idx) + "kv.db"); err == nil {
		if killDB {
			e := os.Remove(base_path + strconv.Itoa(5000+idx) + "kv.db")
			if e != nil {
				return -1, e
			}
			log.Printf("[killDb] Killed the db at %v", base_path+strconv.Itoa(5000+idx)+"kv.db")
		}
	}

	extra_args := make([]string, 2)
	extra_args[0] = nodes_path
	extra_args[1] = strconv.Itoa(idx)

	/*j := 1
	for i := 0; i < num_servers; i++ {
		if i == idx {
			continue
		}
		extra_args[j] = "localhost:" + strconv.Itoa(5000 + i)
		j += 1
	}*/
	//cmdArgs := []string{"localhost:" + string(5000 + idx), }
	log.Printf("Server command: %v", extra_args)
	cmdServer := exec.Command(server_exe_path, extra_args...)
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
		/*child, _ := os.FindProcess(cmdServer.Process.Pid + 1)
				if er := child.Kill(); er != nil {
		                        log.Fatal("failed to kill process: ", er)
		                }
				child.Wait()*/

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
		log.Printf("Started server %v", idx)
	}

	return cmdServer.Process.Pid, nil
}

func launchServer(server_exe_path string, killSwitch chan int, killDB bool, cacheSize string) (int, error) {
	if _, err := os.Stat(database_path); err == nil {
		if killDB {
			e := os.Remove(database_path)
			if e != nil {
				return -1, e
			}
			log.Printf("[killDb] Killed the db at %v", database_path)
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
		/*child, _ := os.FindProcess(cmdServer.Process.Pid + 1)
				if er := child.Kill(); er != nil {
		                        log.Fatal("failed to kill process: ", er)
		                }
				child.Wait()*/

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

/*
func TestGetInputParams(t *testing.T) {
	killSwitch := make(chan int)
	// Nuke database and start fresh
	pid := -1
	var err error
	if pid, err = launchServer(server_path, killSwitch, true, "200"); err != nil {
		panic(err)
	}

	if e := Kv739_init([]string{"localhost:5000"}); e != 0 {
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

	if e := Kv739_init([]string{"localhost:5000"}); e != 0 {
		panic("failed to init server")
	}

	old_val := make([]byte, 2049)
	if e := Kv739_put("val1", "val1", old_val); e == -1 {
		t.Errorf("Failed to insert val; val1 #1")
	}

	if e := Kv739_get("val1", old_val); e != 0 {
		t.Errorf("Failed to retrieve val; val1 #1")
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

	old_val1 := make([]byte, 2049)
	// Do not nuke database and start fresh
	if pid, err = launchServer(server_path, killSwitch, false, "200"); err != nil {
		panic(err)
	}

	if e := Kv739_init([]string{"localhost:5000"}); e != 0 {
		panic("failed to init server")
	}

	if e := Kv739_get("val1", old_val1); e != 0 {
		t.Errorf("Failed to retrieve val; val1 #1")
	}

	if string(old_val1[:bytes.IndexByte(old_val1, 0)]) != "val1" {
		t.Errorf("Wrong value: %v", string(old_val1[:bytes.IndexByte(old_val1, 0)]))
	}

	if e := Kv739_put("val2", "val2", old_val); e == -1 {
		t.Errorf("Failed to insert val; val1 #1")
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

	old_val1 = make([]byte, 2049)
	// Do not nuke database and start fresh
	if pid, err = launchServer(server_path, killSwitch, false, "200"); err != nil {
		panic(err)
	}

	if e := Kv739_init([]string{"localhost:5000"}); e != 0 {
		panic("failed to init server")
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

}*/

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
func killServers(chans []chan int, pids []int) {
	for i := 0; i < len(chans); i++ {
		chans[i] <- 1
		<-chans[i]
		time.Sleep(time.Second)
		if !checkAlive(pids[i]) {
			fmt.Printf("Server died pid: %v\n", pids[i])
		} else {
			fmt.Printf("Server alive with pid: %v\n", pids[i])
		}
	}

}

/*
func TestAvailability1(t *testing.T) {
	chans, pids, err := launchServers(server_path, 3, true)
	if err != nil {
		t.Errorf("Failed to launch servers")
	}
	//if err == nil {
	//	for i := 0; i < 3; i++ {
	//		chans[i] <- 1
	//		<-chans[i]
	//		time.Sleep(time.Second)
	//		if !checkAlive(pids[i]) {
	//			fmt.Printf("Server died pid: %v\n", pids[i])
	//		} else {
	//			fmt.Printf("Server alive with pid: %v\n", pids[i])
	//		}
	//	}
	//} else {
	//	t.Errorf("Error creating servers")
	//}

	if e := Kv739_init([]string{"localhost:5000"}); e != 0 {
                panic("failed to init server")
        }

	killServers(chans[0:1], pids[0:1])
	//if e := Kv739_die(string("localhost:5000"), 0); e != 0 {
	//	t.Errorf("Failed to kill 5001")
	//}

	old_val := make([]byte, 2049)
	if e := Kv739_put("val2", "val2", old_val); e == -1 {
                t.Errorf("Failed to insert val; val1 #1")
        }

	if e := Kv739_get("val2", old_val); e == -1 {
                t.Errorf("Failed to get val; val1 #1")
        }

	if er := Kv739_shutdown(); er != 0 {
                t.Errorf("Failed to shutdown")
        }


	killServers(chans[1:], pids[1:])

}

func TestAvailability2(t *testing.T) {
	chans, pids, err := launchServers(server_path, 3, true)
        if err != nil {
                t.Errorf("Failed to launch servers")
        }

	killServers(chans[1:], pids[1:])
	if e := Kv739_init([]string{"localhost:5000"}); e != 0 {
                panic("failed to init server")
        }

	old_val := make([]byte, 2049)
        if e := Kv739_put("val2", "val2", old_val); e == -1 {
                t.Errorf("Failed to insert val; val1 #1")
        }

        if e := Kv739_get("val2", old_val); e == -1 {
                t.Errorf("Failed to get val; val1 #1")
        }

	if string(old_val[:bytes.IndexByte(old_val, 0)]) != "val2" {
                t.Errorf("Wrong value: %v", old_val)
        }

	if er := Kv739_shutdown(); er != 0 {
                t.Errorf("Failed to shutdown")
        }


	killServers(chans[0:1], pids[0:1])
}

func TestAvailability4(t *testing.T) {
	chans, pids, err := launchServers(server_path, 3, true)
        if err != nil {
                t.Errorf("Failed to launch servers")
        }

	// localhost:5000
        if e := Kv739_init([]string{"localhost:5000", "localhost:5001", "localhost:5002"}); e != 0 {
                panic("failed to init server")
        }

	if e := Kv739_partition("localhost:5000", []string{}); e == -1 {
                t.Errorf("Failed to partition 5000")
        }

	if e := Kv739_partition("localhost:5001", []string{}); e == -1 {
                t.Errorf("Failed to partition 5001")
        }

	if e := Kv739_partition("localhost:5002", []string{}); e == -1 {
                t.Errorf("Failed to partition 5002")
        }

	// localhost:5000
	//if e := Kv739_init([]string{"localhost:5000"}); e != 0 {
        //        panic("failed to init server")
        //}

        old_val := make([]byte, 2049)
        if e := Kv739_put("val2", "val2", old_val); e == -1 {
                t.Errorf("Failed to insert val; val1 #1")
        }

        if e := Kv739_get("val2", old_val); e == -1 {
                t.Errorf("Failed to get val; val1 #1")
        }

        if string(old_val[:bytes.IndexByte(old_val, 0)]) != "val2" {
                t.Errorf("Wrong value: %v", old_val)
        }

	if e := Kv739_shutdown(); e != 0 {
                panic("failed to shutdown")
        }

	//localhost:5001
        if e := Kv739_init([]string{"localhost:5001"}); e != 0 {
                panic("failed to init server")
        }

        //old_val := make([]byte, 2049)
        //if e := Kv739_put("val2", "val2", old_val); e == -1 {
        //        t.Errorf("Failed to insert val; val1 #1")
        //}

         e1 := Kv739_get("val2", old_val)

	 if e := Kv739_shutdown(); e != 0 {
                panic("failed to shutdown")
        }


	if e := Kv739_init([]string{"localhost:5002"}); e != 0 {
                panic("failed to init server")
        }

	e2 := Kv739_get("val2", old_val)


        //if string(old_val[:bytes.IndexByte(old_val, 0)]) != "val2" {
        //        t.Errorf("Wrong value: %v", old_val)
        //}

        if e := Kv739_shutdown(); e != 0 {
                panic("failed to shutdown")
        }


	if e := Kv739_init([]string{"localhost:5000"}); e != 0 {
                panic("failed to init server")
        }

        e3 := Kv739_get("val2", old_val)


        //if string(old_val[:bytes.IndexByte(old_val, 0)]) != "val2" {
        //        t.Errorf("Wrong value: %v", old_val)
        //}

        if e := Kv739_shutdown(); e != 0 {
                panic("failed to shutdown")
        }

	if e1 != 1 && e2 != 1 && e3 != 1 {
		t.Errorf("Partition failure")
	}


	killServers(chans, pids)


}

func TestConsistency1(t *testing.T) {
	chans, pids, err := launchServers(server_path, 3, true)
        if err != nil {
                t.Errorf("Failed to launch servers")
        }

	if e := Kv739_init([]string{"localhost:5000"}); e != 0 {
                panic("failed to init server")
        }

	old_val := make([]byte, 2049)
        if e := Kv739_put("val2", "val2", old_val); e == -1 {
                t.Errorf("Failed to insert val; val1 #1")
        }


	if e := Kv739_put("val2", "val1", old_val); e == -1 {
                t.Errorf("Failed to insert val; val1 #1")
        }
	 if e := Kv739_shutdown(); e != 0 {
                panic("failed to shutdown")
        }

	if e := Kv739_init([]string{"localhost:5002"}); e != 0 {
                panic("failed to init server")
        }


	if e := Kv739_get("val2", old_val); e == -1 {
                t.Errorf("Failed to get val; val1 #1")
        }

        if string(old_val[:bytes.IndexByte(old_val, 0)]) != "val1" {
                t.Errorf("Wrong value: %v", old_val)
        }

	if e := Kv739_shutdown(); e != 0 {
                panic("failed to shutdown")
        }


	killServers(chans, pids)
}


func TestConsistency2(t *testing.T) {
	chans, pids, err := launchServers(server_path, 3, true)
        if err != nil {
                t.Errorf("Failed to launch servers")
        }

	old_val := make([]byte, 2049)
	if e := Kv739_init([]string{"localhost:5000", "localhost:5001", "localhost:5002"}); e != 0 {
                panic("failed to init server")
        }

	if e := Kv739_put("val2", "val2", old_val); e == -1 {
                t.Errorf("Failed to insert val; val1 #1")
        }

	killServers(chans[:2], pids[:2])

	if e := Kv739_put("val2", "val1", old_val); e == -1 {
                t.Errorf("Failed to insert val; val1 #1")
        }

	chan1, pids1, _ := launchServers(server_path, 2, false)

	time.Sleep(3 * time.Second)
	if e := Kv739_get("val2", old_val); e == -1 {
                t.Errorf("Failed to get val; val1 #1")
        }

	if string(old_val[:bytes.IndexByte(old_val, 0)]) != "val1" {
                t.Errorf("Wrong value: %v", string(old_val[:bytes.IndexByte(old_val, 0)]))
        }

	killServers(chan1, pids1)
	killServers(chans[2:], pids[2:])

}


func TestConsistency3(t *testing.T) {
	chans, pids, err := launchServers(server_path, 2, true)
        if err != nil {
                t.Errorf("Failed to launch servers")
        }

	old_val := make([]byte, 2049)
        if e := Kv739_init([]string{"localhost:5000", "localhost:5001"}); e != 0 {
                panic("failed to init server")
        }

	if e := Kv739_partition("localhost:5000", []string{}); e == -1 {
                t.Errorf("Failed to partition 5000")
        }

        if e := Kv739_partition("localhost:5001", []string{}); e == -1 {
                t.Errorf("Failed to partition 5001")
        }

	if e := Kv739_shutdown(); e != 0 {
                panic("failed to shutdown")
        }

	//
	if e := Kv739_init([]string{"localhost:5000"}); e != 0 {
                panic("failed to init server")
        }

	if e := Kv739_put("val1", "val1", old_val); e == -1 {
                t.Errorf("Failed to insert val; val1 #1")
        }

	if e := Kv739_shutdown(); e != 0 {
                panic("failed to shutdown")
        }

	//
	if e := Kv739_init([]string{"localhost:5000", "localhost:5001"}); e != 0 {
                panic("failed to init server")
        }

        if e := Kv739_partition("localhost:5000", []string{"localhost:5001"}); e == -1 {
                t.Errorf("Failed to partition 5000")
        }

        if e := Kv739_partition("localhost:5001", []string{"localhost:5000"}); e == -1 {
                t.Errorf("Failed to partition 5001")
        }

        if e := Kv739_shutdown(); e != 0 {
                panic("failed to shutdown")
        }

	//
	if e := Kv739_init([]string{"localhost:5001"}); e != 0 {
                panic("failed to init server")
        }

	old_val1 := make([]byte, 2049)
        if e := Kv739_get("val1", old_val1); e == -1 {
                t.Errorf("Failed to insert val; val1 #1")
        }

	if string(old_val1[:bytes.IndexByte(old_val1, 0)]) != "val1" {
                t.Errorf("Wrong value: %v", string(old_val1[:bytes.IndexByte(old_val1, 0)]))
        }

        if e := Kv739_shutdown(); e != 0 {
                panic("failed to shutdown")
        }

	killServers(chans, pids)
}*/

/*---------project 3 tests----------*/

func TestLeaderElection1(t *testing.T) {
	chans, pids, err := launchServers(server_path, 5, true)
	if err != nil {
		t.Errorf("Failed to launch servers")
	}

	old_val := make([]byte, 2049)
	if e := Kv739_init([]string{"localhost:5000"}); e != 0 {
		panic("failed to init server")
	}

	//Killing the main server now
	if e := Kv739_die("localhost:5000", 1); e != 0 {
		panic("failed to kill server")
	}

	//Checking get() and put()
	if e := Kv739_put("val1", "val1", old_val); e != -1 {
		t.Errorf("Expected failure but successed; val1 #1")
	}

	/*old_val1 := make([]byte, 2049)
	if e := Kv739_get("val1", old_val1); e == -1 {
		t.Errorf("Failed to insert val; val1 #1")
	}*/

	old_val1 := make([]byte, 2049)

	//Checking get() and put()
	if e := Kv739_put("val1", "val1", old_val); e == -1 {
		t.Errorf("Failed to insert val; val1 #1")
	}

	if e := Kv739_get("val1", old_val1); e == -1 {
		t.Errorf("Failed to get val; val1 #1")
	}

	if string(old_val1[:bytes.IndexByte(old_val1, 0)]) != "val1" {
		t.Errorf("Wrong value: %v", string(old_val1[:bytes.IndexByte(old_val1, 0)]))
	} else {
		t.Logf("Passed the value test")
	}

	killServers(chans[1:], pids[1:])
}

/*
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
*/
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
