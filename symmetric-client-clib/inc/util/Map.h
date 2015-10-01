#ifndef SYM_MAP_H
#define SYM_MAP_H

#include <stdlib.h>
#include <stdio.h>
#include <limits.h>
#include <string.h>

typedef struct {
    char *key;
    void *value;
    int sizeBytes;
    void *next;
} SymMapEntry;


typedef struct {
    int size;
    SymMapEntry **table;
    void (*put)(void *this, char *key, void *value, int size);
    void * (*get)(void *this, char *key);
    int (*get_bytes_size)(void *this, char *key);
    void (*destroy)(void *this);
} SymMap;

SymMap *SymMap_new(SymMap *this, int size);

#endif
