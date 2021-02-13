#include "lib739kv.h"
#include <stdio.h>

int main()
{
	kv739_init("127.0.0.1:5000");
	char value[2049];
	kv739_get("val1", value);
	printf("%s\n", value);
	return 0;
}
