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
    
    char* key = NULL;
    char* val = NULL;
	char value[2049];
        
    char* token = strtok(line, delim);
    //token = strtok(NULL, delim, &state2);
    while( token != NULL ) {
      if (key == NULL) {
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
    //printf( " key: %s\n", key );
    //printf( " value: %s\n", val );
    start = clock();
    kv739_put(key, val, value);
    kv739_get(key, value);
    total += (clock() - start);
    printf("Value inserted %s\n", value);
        //printf("%s\n", value);
    /*char *state1, *state2;
    char *token = strtok_r(line, delim, &state1);
    while(token){
        char *current_string = strdup(token);

        char *tk = strtok_r(current_string, delim, &state2); // KEY
        printf("key: %s \r\n", tk);
        tk = strtok_r(NULL, delim, &state2);                    // VALUE
        printf("value: %s\r\n", tk);
        printf("%s\n", token);
        free(current_string);
        break;
    }*/
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
	printf("Putting Value now!\n");

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
    printf("%d requests took %d seconds %d milliseconds %ld\n", count*2, msec/1000, msec%1000, total);
    printf("average response time is %d seconds %d milliseconds\n", msec/1000/(count*2), msec%1000/(count*2));

    fclose(dbfp);

    //FILE *tracefp = fopen(argv[1], "r");
    
	return 0;
}