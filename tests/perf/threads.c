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

#define NUM_THREADS 1
void *PrintHello(void *threadid) {
   long tid;
   tid = (long)threadid;
   printf("Hello World! Thread ID, %ld\n", tid);
   
   	kv739_init("127.0.0.1:5000");
	printf("thread Putting Value now!\n");
    kv739_shutdown();
   
   pthread_exit(NULL);
}

int main () {
   pthread_t threads[NUM_THREADS];
   int rc;
   int i;
   for( i = 0; i < NUM_THREADS; i++ ) {
      printf("main() : creating thread %d \n", i);// << i << endl;
      rc = pthread_create(&threads[i], NULL, PrintHello, (void *)i);
      if (rc) {
         printf("Error:unable to create thread, %d\n", rc);
         exit(-1);
      }
   }
   pthread_exit(NULL);
}