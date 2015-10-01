/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#include "util/Map.h"

static int SymMap_hash(SymMap *this, char *key) {
    unsigned long int hash;
    unsigned int i;
    int len = strlen(key);
    for (i = 0, hash = 0; hash < ULONG_MAX && i < len; i++) {
        hash = hash << 8;
        hash += key[i];
    }
    return hash % this->size;
}

static SymMapEntry * SymMap_new_entry(char *key, void *value, int size) {
    SymMapEntry *entry;

    if ((entry = malloc(sizeof(SymMapEntry))) == NULL) {
        return NULL;
    }

    if ((entry->key = strdup(key)) == NULL) {
        return NULL;
    }

    if (value != NULL) {
        entry->value = memcpy(malloc(size), value, size);
        entry->sizeBytes = size;
    } else {
        entry->value = NULL;
        entry->sizeBytes = 0;
    }

    entry->next = NULL;
    return entry;
}

void SymMap_put(SymMap *this, char *key, void *value, int size) {
    int hash = SymMap_hash(this, key);

    SymMapEntry *next = this->table[hash];

    SymMapEntry *last = NULL;
    while (next != NULL && next->key != NULL && strcmp(key, next->key) > 0) {
        last = next;
        next = next->next;
    }

    if (next != NULL && next->key != NULL && strcmp(key, next->key) == 0) {
        free(next->value);
        next->value = malloc(size);
        if (value != NULL) {
            memcpy(next->value, value, size);
            next->sizeBytes = size;
        } else {
            next->value = NULL;
            next->sizeBytes = 0;
        }
    } else {
        SymMapEntry *entry = SymMap_new_entry(key, value, size);

        if (next == this->table[hash]) {
            entry->next = next;
            this->table[hash] = entry;
        } else if (next == NULL) {
            last->next = entry;
        } else {
            entry->next = next;
            last->next = entry;
        }
    }
}

void * SymMap_get(SymMap *this, char *key) {
    int hash = SymMap_hash(this, key);

    SymMapEntry *entry = this->table[hash];
    while (entry != NULL && entry->key != NULL && strcmp(key, entry->key) > 0) {
        entry = entry->next;
    }

    if (entry == NULL || entry->key == NULL || strcmp(key, entry->key) != 0) {
        return NULL;
    } else {
        return entry->value;
    }
}

int SymMap_get_bytes_size(SymMap *this, char *key) {
    int hash = SymMap_hash(this, key);

    SymMapEntry *entry = this->table[hash];
    while (entry != NULL && entry->key != NULL && strcmp(key, entry->key) > 0) {
        entry = entry->next;
    }

    if (entry == NULL || entry->key == NULL || strcmp(key, entry->key) != 0) {
        return 0;
    } else {
        return entry->sizeBytes;
    }
}

void SymMap_destroy(SymMap *this) {
    // TODO: free all the malloc'ed memory
    free(this);
}

SymMap * SymMap_new(SymMap *this, int size) {
    if (this == NULL) {
        this = malloc(sizeof(SymMap));
    }
    this->get = (void *) &SymMap_get;
    this->get_bytes_size = (void *) &SymMap_get_bytes_size;
    this->put = (void *) &SymMap_put;
    this->destroy = (void *) &SymMap_destroy;

    if (size < 1) {
        size = 1;
    }

    if ((this->table = malloc(sizeof(SymMapEntry *) * size)) == NULL) {
        return NULL;
    }

    int i;
    for (i = 0; i < size; i++) {
        this->table[i] = NULL;
    }
    this->size = size;
    return this;
}
