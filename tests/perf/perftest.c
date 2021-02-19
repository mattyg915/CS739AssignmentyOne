#include "lib739kv.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>

const char delim[2] = "[";
const size_t keylenMax=128;
const size_t vallenMax=2048;

clock_t start, end, total;

static void split_kv(char * line) {
    
    int type = -1;
    char* key = NULL;
    char* val = NULL;
	char value[2049];
        
    char* token = strtok(line, delim);
    //token = strtok(NULL, delim, &state2);
    while( token != NULL ) {
      if (type == -1) {
          type = atoi(strdup(token));
          printf( " type: %d\n", type );
          token = strtok(NULL, delim);
      }
      else if (key == NULL) {
          key = strdup(token);
          printf( " key: %s\n", key );
          token = strtok(NULL, delim);
      }
      else {
          val = strdup(token);
          val[strcspn(val, "\n")] = 0;
          printf( " value: %s\n", val );
          token = strtok(NULL, delim);
      }
    }
    if (type) { //1 mean put
        start = clock();
        kv739_put(key, val, value);
        //kv739_get(key, value);
        total += (clock() - start);
    }
    else { //0 mean get
        start = clock();
        kv739_get(key, value);
        total += (clock() - start);
    }
    free(key);
    free(val);
}

int main( int argc, char *argv[] )
{
    //usage: ./prog dbfile #threads 
    //tracefile
    //start = clock();
    
    char * line = NULL;
    size_t len = 0;
    ssize_t read;
    
    FILE *dbfp = fopen(argv[1], "r");
    
    
	kv739_init("127.0.0.1:5000");
	printf("Putting Values now!\n");

	int count = 0;
    
    while ((read = getline(&line, &len, dbfp)) != -1) {
        //printf("Retrieved line of length %zu:\n", read);
        printf("line: %s\n", line);
        
        //char * key [keylenMax+1];
        //char * val [vallenMax+1];
        split_kv(line);
        count++;
        
        //printf( " val: %s\n", val );
        
        /*if (kv739_put(key, val, value) >= 0)
            printf("Value inserted\n");
        if (kv739_get(key, value) == 0)
            printf("%s\n", value);*/
        

        //printf("key: %s\n", key);
        //printf("val: %s\n\n", val);
    }

    kv739_shutdown();
    
    //sleep(10);
    //end = clock();
    //total = end - start;
    
    int msec = total * 1000 / CLOCKS_PER_SEC;
    printf("%d requests took %d seconds %d milliseconds %ld\n", count, msec/1000, msec%1000, total);
    printf("average response time is %d seconds %d milliseconds\n", msec/1000/(count), msec%1000/(count));

    fclose(dbfp);

    //FILE *tracefp = fopen(argv[1], "r");
    
	return 0;
}