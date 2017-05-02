#ifndef SYM_MAP_H
#define SYM_MAP_H

#include <stdlib.h>
#include <stdio.h>
#include <limits.h>
#include <string.h>
#include "util/List.h"
#include "util/StringArray.h"
#include "util/StringUtils.h"

typedef struct {
    char *key;
    void *value;
    void *next;
} SymMapEntry;


typedef struct SymMap {
    int size;
    SymMapEntry **table;
    void (*put)(struct SymMap *this, char *key, void *value);
    void (*putByInt)(struct SymMap *this, int key, void *value);
    void * (*get)(struct SymMap *this, char *key);
    void * (*getByInt)(struct SymMap *this, int key);
    void * (*remove)(struct SymMap *this, char *key);
    void * (*removeByInt)(struct SymMap *this, int key);
    SymStringArray * (*keys)(struct SymMap *this);
    SymList * (*values)(struct SymMap *this);
    SymList * (*entries)(struct SymMap *this);
    char * (*toString)(struct SymMap *this);
    void (*reset)(struct SymMap *this);
    void (*resetAll)(struct SymMap *this, void (*destroyObject)(void *object));
    void (*destroy)(struct SymMap *this);
    void (*destroyAll)(struct SymMap *this, void (*destroyObject)(void *object));
} SymMap;

SymMap *SymMap_new(SymMap *this, int size);

#endif
