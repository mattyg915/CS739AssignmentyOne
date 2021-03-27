#include "lib739kv.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

//using at most 2 servers in tests
//format: "host:port"
const char* server_names[3];

//preferably try a DNS hostname
void test_initshutdown()
{
    
}

//test all return values for get and put
//plus most basic put then get and null get to all servers
//no failure
void test_getput()
{
    
}

//partition the servers to 2-1
//check if both side can still serve requests
void test_partition()
{
    
}

//partition the servers to 2-1
//put some disjoin requests then merge servers back
//check for get consistency
void test_partitionmerge()
{
    
}

//connect to just one server put a KV
// check consistency across nodes in different situations
void test_consistency()
{
    
}

//bring down all but 1 server
//check for availability and some consistency
void test_availability()
{
    
}


//test parallel connections

//connect to just one server, restart it
// check persistence on the same node

int main(int argc, char** argv)
{
    //usage: ./basic_tests serverlist.lst
    
    
    ssize_t read;
    char * line = NULL;
    size_t len = 0;
    
    FILE *fp = fopen(argv[1], "r");
    
    //load server list
	int count = 0;
    while ((read = getline(&line, &len, fp)) != -1) 
    {
        char* server = strdup(line);
        server[strcspn(server, "\n")] = 0;
        server_names[count] = server;
        count++;
        //printf("%s\n", server);
    }
    
    server_names[0] = "royal-09:5000";
    server_names[1] = 0;
    count = 1;
    
    int j = 0;
    for (;j<count;j++)
    {
        printf("%s\n", server_names[j]);
    }
    
    printf("init\n");
    printf("%d\n", kv739_init(server_names));
    
    //run tests
    //char* key = NULL;
    //char* val = NULL;
    char value0[2049];
    char value1[2049];
    char value2[2049];
    printf("get\n");
    kv739_get("key", value0);
    printf("%s\n", value0);
    printf("put\n");
    kv739_put("key", "val", value1);
    printf("%s\n", value1);
    printf("get\n");
    kv739_get("key", value2);
    printf("%s\n", value2);
    
    //printf("partition\n");
    //char* reachable[] = {server_names[0], 0};
    //kv739_partition(server_names[0], reachable);
    
    //printf("die\n");
    int die = kv739_die(server_names[0], 1);
    printf("%d\n", die);
    //when a test returns, pause and rennovate severs if needed
    
    //printf("shutdown\n");
    //kv739_shutdown();
    
    //free server list
    int i = 0;
    for (;i < count;i++)
    {
        free(server_names[i]);
        server_names[i] = NULL;
    }
}
