//#include "lib739kv.h"
#include "kv739.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <math.h>
#include <pthread.h>
#include <stdint.h>
#include "thpool.h"

const char delim[2] = "[";
const size_t keylenMax=128;
const size_t vallenMax=2048;

int num_gets = 0;
int num_puts = 0;


int *types;
char **keys;
char **vals;
clock_t *latencies;
unsigned long long int* tot_sizes; //size in bytes
clock_t *totals; // total time

static void split_kv(void *arg) {
        /*printf("Thread #%u working on %d\n", (int)pthread_self(), (int) arg);
        printf("type: %d\n", types[(int)arg]);
        printf("key: %s\n", keys[(int)arg]);
        printf("val: %s\n\n", vals[(int)arg]);
        free(keys[(int)arg]);
        free(vals[(int)arg]);*/
    clock_t start, end;
    int type = -1;
	char value[2049];
    
    if (types[(int)arg]) { //1 means put
        start = clock();
        kv739_put(keys[(int)arg], vals[(int)arg], value);
        //kv739_get(key, value);
        end = clock();
        totals[(int)arg] += (end-start);
        
        //num_puts++;
        tot_sizes[(int)arg]+=(unsigned long long int)strlen(vals[(int)arg]);
        tot_sizes[(int)arg]+=(unsigned long long int)strlen(value);
    }
    else { //0 means get
        start = clock();
        kv739_get(keys[(int)arg], value);
        end = clock();
        totals[(int)arg] += (end-start);
        
        //num_gets++;
        tot_sizes[(int)arg]+=(unsigned long long int)strlen(value);
    }
    //printf( " value: %s\n", value );
    latencies[(int)arg] = (end-start);
    
    free(keys[(int)arg]);
    free(vals[(int)arg]);
}

int cmpfunc (const void * a, const void * b) {
   return ( *(clock_t*)a - *(clock_t*)b );
}


int main( int argc, char *argv[] )
{
    //usage: ./prog dbfile //#threads 
    //tracefile
    //start = clock();
    clock_t start, end;
    
    char * line = NULL;
    size_t len = 0;
    ssize_t read;
    
    FILE *fp = fopen(argv[1], "r");
    
    start = clock();
	kv739_init("127.0.0.1:5000");
    end = clock();
	printf("init time = %ld usec\n", (end - start));

    //count lines
	int count = 0;
    while ((read = getline(&line, &len, fp)) != -1) {
        count++;
    }
    rewind(fp);
    latencies = (clock_t*)malloc(count*sizeof(clock_t));
    
    types = (int*)malloc(count*sizeof(int));
    keys = (char**)malloc(count*sizeof(char*));
    vals = (char**)malloc(count*sizeof(char*));
    tot_sizes = (unsigned long long int* )malloc(count*sizeof(unsigned long long int)); //size in bytes
    totals = (clock_t *)malloc(count*sizeof(clock_t)); // total time
    int i = 0;
    while ((read = getline(&line, &len, fp)) != -1) {
        
        types[i] = -1;
        keys[i] = NULL;
        vals[i] = NULL;
        
        char* token = strtok(line, delim);
        while( token != NULL ) {
          if (types[i] == -1) {
              types[i] = atoi(strdup(token));
              //printf( " type: %d\n", type );
              token = strtok(NULL, delim);
          }
          else if (keys[i] == NULL) {
              keys[i] = strdup(token);
              //printf( " key: %s\n", key );
              token = strtok(NULL, delim);
          }
          else {
              vals[i] = strdup(token);
              vals[i][strcspn(vals[i], "\n")] = 0;
              //printf( " value: %s\n", val );
              token = strtok(NULL, delim);
          }
        }
    
        
        //split_kv(line, latencies, i);
        i++;
    }
    
    fclose(fp);
    
    puts("Making threadpool with 1 threads");
	threadpool thpool = thpool_init(1);
    i = 0;
    clock_t start_all, end_all;
    start_all = clock();//timing
    for (; i < count; i++) {
        thpool_add_work(thpool, split_kv, (void*)(uintptr_t)i);
    }
    thpool_wait(thpool);
    end_all = clock();//timing
	puts("Killing threadpool");
	thpool_destroy(thpool);
    
    printf("overall time = %ld usec\n", (end_all - start_all));
    
    start = clock();
	kv739_shutdown();
    end = clock();
	printf("shutdown time = %ld usec\n", (end - start));
    
    qsort(latencies, count, sizeof(clock_t), cmpfunc);
    
    //sum it up
    unsigned long long int tot_size = 0; //size in bytes
    clock_t total = 0; // total time
    
    for(i=0;i<count;i++) tot_size+=tot_sizes[i];
    for(i=0;i<count;i++) total+=totals[i];
    
    //mean, median,  and total latency
    //printf("Summary: %d gets, %d puts\n", num_gets, num_puts);
    //int msec = total * 1000 / CLOCKS_PER_SEC;
    clock_t usec_median = latencies[(int)(count/2)];
    printf("A total of %d requests took %ld useconds\n", count, total);
    printf("Average response time is %d useconds\n", ((int)total)/(count));
    printf("Median response time is %ld useconds\n", usec_median);
    //tail 99 and max min latency
    int tail_start = (int)(ceil(count*99/100));
    clock_t usec_tail = 0;
    for (i = tail_start;i<count;i++) usec_tail+=latencies[i];
    clock_t usec_max = latencies[count-1];
    clock_t usec_min = latencies[0];
    printf("99 percentile response time is %d useconds\n", ((int)usec_tail)/(count-tail_start));
    printf("max response time is %ld useconds\n", usec_max);
    printf("min response time is %ld useconds\n", usec_min);
    //per sec avg throughput measured in value bytes
    int avg_thru = (int)(tot_size * CLOCKS_PER_SEC / total);
    printf("Total throughput is %llu Bytes/sec\n", tot_size);
    printf("Average throughput is %d Bytes/sec\n", avg_thru);
    
    free(latencies);
    free(types);
    free(keys);
    free(vals);
    free(tot_sizes);
    free(totals);
    
	return 0;
}
