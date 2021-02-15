#include "lib739kv.h"
#include <stdio.h>

int main()
{
	kv739_init("127.0.0.1:5000");
	char value[2049];
	printf("Putting Value now!\n");

	if (kv739_put("val1", "val1", value) >= 0)
		printf("Value inserted\n");
	if (kv739_get("val1", value) == 0)
		printf("%s\n", value);
	return 0;
}
