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

static SymMapEntry * SymMap_newEntry(char *key, void *value, int size) {
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
        SymMapEntry *entry = SymMap_newEntry(key, value, size);

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

void SymMap_putByInt(SymMap *this, int key, void *value, int size) {
    char *id = SymStringUtils_format("%d", key);
    SymMap_put(this, id, value, size);
    free(id);
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

void * SymMap_getByInt(SymMap *this, char *key) {
    char *id = SymStringUtils_format("%d", key);
    void *result = SymMap_get(this, id);
    free(id);
    return result;
}

SymStringArray * SymMap_keys(SymMap *this) {
	SymList *entries = this->entries(this);
	SymStringArray *keys = SymStringArray_new(NULL);
	int i;
	for (i = 0; i < entries->size; i++) {
	    SymMapEntry *entry = (SymMapEntry *) entries->get(entries, i);
	    if (entry->key) {
	        keys->add(keys, entry->key);
	    }
	}
	entries->destroy(entries);
	return keys;
}

SymList * SymMap_values(SymMap *this) {
	SymList *entries = this->entries(this);
	SymList *values = SymList_new(NULL);
	int i;
	for (i = 0; i < entries->size; i++) {
	    SymMapEntry *entry = (SymMapEntry *) entries->get(entries, i);
	    if (entry->key) {
	        values->add(values, entry->value);
	    }
	}
	entries->destroy(entries);
	return values;
}

SymList * SymMap_entries(SymMap *this) {
	SymList *entries = SymList_new(NULL);

	SymMapEntry *entry = NULL;
	int index = 0;

	while (index < this->size) {
		if (this->table[index] != NULL) {
			entry = this->table[index];
			SymMapEntry *currentEntry = entry;

			while (currentEntry != NULL) {
				entries->add(entries, currentEntry);
				currentEntry = currentEntry->next;
			}
		}
		index++;
	}

	return entries;
}

int SymMap_getBytesSize(SymMap *this, char *key) {
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

void * SymMap_remove(SymMap *this, char *key) {
    int hash = SymMap_hash(this, key);
    SymMapEntry *entry = this->table[hash];
    void *result = NULL;

    while (entry != NULL) {
        if (entry->key != NULL && strcmp(key, entry->key) == 0) {
            result = entry->value;
            free(entry->key);
            entry->key = NULL;
            entry->value = NULL;
            break;
        }
        entry = entry->next;
    }

    return result;
}

void * SymMap_removeByInt(SymMap *this, int key) {
    char *id = SymStringUtils_format("%d", key);
    void *result = SymMap_remove(this, id);
    free(id);
    return result;
}

void SymMap_reset(SymMap *this) {
    SymMapEntry *entry = NULL;
    int index = 0;

    while (index < this->size) {
        if (this->table[index] != NULL) {
            entry = this->table[index];
            SymMapEntry *currentEntry = entry;

            while (currentEntry != NULL) {
                SymMapEntry *nextEntry = currentEntry->next;
                free(currentEntry->key);
                free(currentEntry->value);
                free(currentEntry);
                currentEntry = nextEntry;
            }
        }
        index++;
    }
}

void SymMap_destroy(SymMap *this) {
    // TODO: free all the malloc'ed memory
    this->reset(this);
    free(this);
}

SymMap * SymMap_new(SymMap *this, int size) {
    if (this == NULL) {
        this = malloc(sizeof(SymMap));
    }
    this->get = (void *) &SymMap_get;
    this->getByInt = (void *) &SymMap_getByInt;
    this->keys = (void *) &SymMap_keys;
    this->values = (void *) &SymMap_values;
    this->entries = (void *) &SymMap_entries;
    this->getBytesSize = (void *) &SymMap_getBytesSize;
    this->put = (void *) &SymMap_put;
    this->putByInt = (void *) &SymMap_putByInt;
    this->remove = (void *) &SymMap_remove;
    this->removeByInt = (void *) &SymMap_removeByInt;
    this->reset = (void *) &SymMap_reset;
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
