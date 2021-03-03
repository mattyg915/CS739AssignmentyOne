#include "lib739kv.h"
#include <stdio.h>

//using at most 5 servers in tests
//format: "host:port"
const char* server_names[5];

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

int main()
{
    //usage: ./basic_tests serverlist.lst
    
    
    ssize_t read;
    char * line = NULL;
    
    FILE *fp = fopen(argv[1], "r");
    
    //load server list
	int count = 0;
    while ((read = getline(&line, &len, fp)) != -1) 
    {
        char* server = strdup(read);
        server[strcspn(server, "\n")] = 0;
        server_names[count] = server;
        count++;
    }
    
    
    //run tests
    
    //when a test returns, pause and rennovate severs if needed
    
    
    
    //free server list
    int i = 0;
    for (;i < count;i++)
    {
        free(server_names[i]);
        server_names[i] = NULL;
    }
}