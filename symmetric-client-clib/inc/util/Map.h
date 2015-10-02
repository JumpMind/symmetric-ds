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


typedef struct SymMap {
    int size;
    SymMapEntry **table;
    void (*put)(struct SymMap *this, char *key, void *value, int size);
    void * (*get)(struct SymMap *this, char *key);
    int (*getBytesSize)(struct SymMap *this, char *key);
    void (*destroy)(struct SymMap *this);
} SymMap;

SymMap *SymMap_new(SymMap *this, int size);

#endif
