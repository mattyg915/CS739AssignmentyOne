#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#ifndef KV739_KEY_SIZE_MAX
#define KV739_KEY_SIZE_MAX 128
#endif

#ifndef KV739_VALUE_SIZE_MAX
#define KV739_VALUE_SIZE_MAX 2048
#endif

/**
 * Provide a server names with the format "host:port" and initialize the client
 * code. Returns 0 on success and -1 on failure.
 */
int kv739_init(char *server_name);

/**
 * Shutdown the connection to a server and free state. After calling this,
 * client code should be able to call kv739_init() again to the same or a
 * different server.
 */
int kv739_shutdown(void);

/**
 * Retrieve the value corresponding to the key. If the key is present, it
 * should return 0 and store the value in the provided string. The string must
 * be at least 1 byte larger than the maximum allowed value. If the key is not
 * present, it should return 1. If there is a failure, it should return -1.
 */
int kv739_get(char *key, char *value);

/**
 * Perform a get operation on the current value into old_value and then store
 * the specified value. Should return 0 on success if there is an old value, 1
 * on success if there was no old value, and -1 on failure. The old_value
 * parameter must be at least one byte larger than the maximum value size.
 */
int kv739_put(char *key, char *value, char *old_value);

#ifdef __cplusplus
}
#endif
