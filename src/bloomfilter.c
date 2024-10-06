#include "redismodule.h"
#include <stdlib.h>
#include <string.h>

#define BLOOM_FILTER_SIZE 1000  // Size of the bit array

// A basic hash function
unsigned int hash(const char *str, int seed) {
    unsigned int hash = seed;
    while (*str) {
        hash = (hash * 101) + *str++;
    }
    return hash % BLOOM_FILTER_SIZE;
}

// Helper to set a bit in the Bloom Filter
void set_bit(unsigned char *bits, int index) {
    bits[index / 8] |= (1 << (index % 8));
}

// Helper to check if a bit is set
int check_bit(unsigned char *bits, int index) {
    return bits[index / 8] & (1 << (index % 8));
}

// Function to create a new Bloom Filter (all bits set to 0)
unsigned char *create_bloomfilter() {
    unsigned char *bits = RedisModule_Alloc(BLOOM_FILTER_SIZE / 8);
    memset(bits, 0, BLOOM_FILTER_SIZE / 8);  // Initialize Bloom filter with 0s
    return bits;
}

// Function to retrieve the Bloom Filter from Redis or create a new one
unsigned char *get_or_create_bloomfilter(RedisModuleCtx *ctx, RedisModuleString *keyname) {
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyname, REDISMODULE_READ | REDISMODULE_WRITE);
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_STRING) {
        // If the Bloom Filter already exists, return it
        size_t len;
        unsigned char *bits = (unsigned char *)RedisModule_StringDMA(key, &len, REDISMODULE_WRITE);
        if (len == BLOOM_FILTER_SIZE / 8) {
            return bits;
        }
    }
    // If the key doesn't exist or the size is wrong, create a new Bloom Filter
    unsigned char *bits = create_bloomfilter();
    RedisModule_StringSet(key, RedisModule_CreateString(ctx, (char *)bits, BLOOM_FILTER_SIZE / 8));
    return bits;
}

// Command to add an element to the Bloom Filter
int BloomFilterAdd_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    // Retrieve or create the Bloom Filter for the given key
    unsigned char *bits = get_or_create_bloomfilter(ctx, argv[1]);

    size_t len;
    const char *element = RedisModule_StringPtrLen(argv[2], &len);

    // Set the corresponding bits for the element
    set_bit(bits, hash(element, 17));
    set_bit(bits, hash(element, 31));

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

// Command to check membership in the Bloom Filter
int BloomFilterCheck_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    // Retrieve or create the Bloom Filter for the given key
    unsigned char *bits = get_or_create_bloomfilter(ctx, argv[1]);

    size_t len;
    const char *element = RedisModule_StringPtrLen(argv[2], &len);

    // Check if the corresponding bits are set for the element
    int found = check_bit(bits, hash(element, 17)) && check_bit(bits, hash(element, 31));

    if (found) {
        RedisModule_ReplyWithSimpleString(ctx, "POSSIBLY");
    } else {
        RedisModule_ReplyWithSimpleString(ctx, "NO");
    }

    return REDISMODULE_OK;
}

// Module initialization function
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx, "bloom", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    // Register the Bloom Filter Add command
    if (RedisModule_CreateCommand(ctx, "bloom.add", BloomFilterAdd_RedisCommand, "write", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    // Register the Bloom Filter Check command
    if (RedisModule_CreateCommand(ctx, "bloom.check", BloomFilterCheck_RedisCommand, "readonly", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;
}


// #include "redismodule.h"
// #include <stdlib.h>
// #include <string.h>
// #include <math.h>

// #define BLOOM_FILTER_SIZE 1000  // Size of the bit array

// // A basic hash function
// unsigned int hash(const char *str, int seed) {
//     unsigned int hash = seed;
//     while (*str) {
//         hash = (hash * 101) + *str++;
//     }
//     return hash % BLOOM_FILTER_SIZE;
// }

// // Create a Bloom Filter structure
// typedef struct {
//     unsigned char bits[BLOOM_FILTER_SIZE / 8];
// } BloomFilter;

// // Helper to set a bit in the Bloom Filter
// void set_bit(BloomFilter *bf, int index) {
//     bf->bits[index / 8] |= (1 << (index % 8));
// }

// // Helper to check if a bit is set
// int check_bit(BloomFilter *bf, int index) {
//     return bf->bits[index / 8] & (1 << (index % 8));
// }

// int BloomFilterAdd_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
//     if (argc != 2) {
//         return RedisModule_WrongArity(ctx);
//     }

//     BloomFilter *bf = RedisModule_Alloc(sizeof(BloomFilter));
//     memset(bf, 0, sizeof(BloomFilter));  // Initialize Bloom filter with 0s

//     size_t len;
//     const char *element = RedisModule_StringPtrLen(argv[1], &len);

//     // Set the corresponding bits for the element
//     set_bit(bf, hash(element, 17));
//     set_bit(bf, hash(element, 31));

//     RedisModule_ReplyWithSimpleString(ctx, "OK");
//     return REDISMODULE_OK;
// }

// int BloomFilterCheck_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
//     if (argc != 2) {
//         return RedisModule_WrongArity(ctx);
//     }

//     BloomFilter *bf = RedisModule_Alloc(sizeof(BloomFilter));

//     size_t len;
//     const char *element = RedisModule_StringPtrLen(argv[1], &len);

//     int found = check_bit(bf, hash(element, 17)) && check_bit(bf, hash(element, 31));

//     if (found) {
//         RedisModule_ReplyWithSimpleString(ctx, "POSSIBLY");
//     } else {
//         RedisModule_ReplyWithSimpleString(ctx, "NO");
//     }

//     return REDISMODULE_OK;
// }

// // Initialization function for the module
// int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
//     if (RedisModule_Init(ctx, "bloom", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
//         return REDISMODULE_ERR;

//     // Register the Bloom Filter Add command
//     if (RedisModule_CreateCommand(ctx, "bloom.add", BloomFilterAdd_RedisCommand, "write", 1, 1, 1) == REDISMODULE_ERR)
//         return REDISMODULE_ERR;

//     // Register the Bloom Filter Check command
//     if (RedisModule_CreateCommand(ctx, "bloom.check", BloomFilterCheck_RedisCommand, "readonly", 1, 1, 1) == REDISMODULE_ERR)
//         return REDISMODULE_ERR;

//     return REDISMODULE_OK;
// }

