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

static SymMapEntry * SymMap_newEntry(char *key, void *value) {
    SymMapEntry *entry;

    if ((entry = malloc(sizeof(SymMapEntry))) == NULL) {
        return NULL;
    }

    entry->key = SymStringBuilder_copy(key);
    entry->value = value;
    entry->next = NULL;
    return entry;
}

void SymMap_put(SymMap *this, char *key, void *value) {
    int hash = SymMap_hash(this, key);

    SymMapEntry *next = this->table[hash];

    SymMapEntry *last = NULL;
    while (next != NULL && next->key != NULL && strcmp(key, next->key) > 0) {
        last = next;
        next = next->next;
    }

    if (next != NULL && next->key != NULL && strcmp(key, next->key) == 0) {
        next->value = value;
    } else {
        SymMapEntry *entry = SymMap_newEntry(key, value);

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

void SymMap_putByInt(SymMap *this, int key, void *value) {
    char *id = SymStringUtils_format("%d", key);
    SymMap_put(this, id, value);
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

void SymMap_resetAll(SymMap *this, void (*destroyObject)(void *object)) {
    SymMapEntry *entry = NULL;
    int index = 0;

    while (index < this->size) {
        if (this->table[index] != NULL) {
            entry = this->table[index];
            SymMapEntry *currentEntry = entry;

            while (currentEntry != NULL) {
                SymMapEntry *nextEntry = currentEntry->next;
                if (destroyObject && currentEntry->value) {
                    destroyObject(currentEntry->value);
                }
                free(currentEntry->key);
                currentEntry->key = NULL;
                currentEntry->value = NULL;
                free(currentEntry);
                currentEntry = nextEntry;
            }
        }
        index++;
    }

    int i;
    for (i = 0; i < this->size; i++) {
        this->table[i] = NULL;
    }
}

char* SymMap_toString(SymMap *this) {
    SymStringBuilder *buff = SymStringBuilder_new(NULL);
    buff->append(buff, "{");
    SymStringArray* keys = this->keys(this);
    int i;
    for (i = 0; i < keys->size; ++i) {
        char* key = keys->get(keys, i);
        buff->append(buff, "[");
        buff->appendf(buff, "%s=%s", key, this->get(this, key));
        buff->append(buff, "]");
    }
    buff->append(buff, "}");
    return buff->destroyAndReturn(buff);
}

void SymMap_reset(SymMap *this) {
    SymMap_resetAll(this, NULL);
}

void SymMap_destroy(SymMap *this) {
    this->reset(this);
    free(this->table);
    free(this);
}

void SymMap_destroyAll(SymMap *this, void (*destroyObject)(void *object)) {
    SymMap_resetAll(this, destroyObject);
    free(this->table);
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
    this->put = (void *) &SymMap_put;
    this->putByInt = (void *) &SymMap_putByInt;
    this->remove = (void *) &SymMap_remove;
    this->removeByInt = (void *) &SymMap_removeByInt;
    this->toString = (void *) &SymMap_toString;
    this->reset = (void *) &SymMap_reset;
    this->resetAll = (void *) &SymMap_resetAll;
    this->destroy = (void *) &SymMap_destroy;
    this->destroyAll = (void *) &SymMap_destroyAll;

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
