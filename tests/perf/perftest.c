//#include "lib739kv.h"
#include "kv739.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <math.h>

const char delim[2] = "[";
const size_t keylenMax=128;
const size_t vallenMax=2048;

clock_t start, end, total;
int num_gets = 0;
int num_puts = 0;
unsigned long long int tot_size = 0; //size in bytes

static void split_kv(char * line, clock_t *latencies, int i) {
    
    int type = -1;
    char* key = NULL;
    char* val = NULL;
	char value[2049];
        
    char* token = strtok(line, delim);
    while( token != NULL ) {
      if (type == -1) {
          type = atoi(strdup(token));
          //printf( " type: %d\n", type );
          token = strtok(NULL, delim);
      }
      else if (key == NULL) {
          key = strdup(token);
          //printf( " key: %s\n", key );
          token = strtok(NULL, delim);
      }
      else {
          val = strdup(token);
          val[strcspn(val, "\n")] = 0;
          //printf( " value: %s\n", val );
          token = strtok(NULL, delim);
      }
    }
    if (type) { //1 means put
        start = clock();
        kv739_put(key, val, value);
        //kv739_get(key, value);
        end = clock();
        total += (end-start);
        
        num_puts++;
        tot_size+=(unsigned long long int)strlen(val);
        tot_size+=(unsigned long long int)strlen(value);
    }
    else { //0 means get
        start = clock();
        kv739_get(key, value);
        end = clock();
        total += (end-start);
        
        num_gets++;
        tot_size+=(unsigned long long int)strlen(value);
    }
    //printf( " value: %s\n", value );
    latencies[i] = (end-start);
    
    free(key);
    free(val);
}

int cmpfunc (const void * a, const void * b) {
   return ( *(clock_t*)a - *(clock_t*)b );
}


int main( int argc, char *argv[] )
{
    //usage: ./prog dbfile //#threads 
    //tracefile
    //start = clock();
    
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
    clock_t *latencies = (clock_t*)malloc(count*sizeof(clock_t));
    
    int i = 0;
    while ((read = getline(&line, &len, fp)) != -1) {
        split_kv(line, latencies, i);
        i++;
    }

    start = clock();
	kv739_shutdown();
    end = clock();
	printf("shutdown time = %ld usec\n", (end - start));
    
    qsort(latencies, count, sizeof(clock_t), cmpfunc);
    //mean, median,  and total latency
    printf("Summary: %d gets, %d puts\n", num_gets, num_puts);
    //int msec = total * 1000 / CLOCKS_PER_SEC;
    clock_t usec_median = latencies[(int)(count/2)];
    printf("A total of %d requests took %ld useconds\n", count, total);
    printf("Average response time is %d useconds\n", ((int)total)/(count));
    printf("Median response time is %ld useconds\n", usec_median);
    //tail 99 and max min latency
    int tail_start = (int)(ceil(count*99/100));
    clock_t usec_tail = 0;
    for (int i = tail_start;i<count;i++) usec_tail+=latencies[i];
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
    
    fclose(fp);
    

    //FILE *tracefp = fopen(argv[1], "r");
    
	return 0;
}