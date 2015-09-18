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
#include "util/ArrayBuilder.h"

void SymArrayBuilder_add(SymArrayBuilder *this, char *src) {
    this->addn(this, src, strlen(src));
}

void SymArrayBuilder_addn(SymArrayBuilder *this, const char *src, int length) {
    char *str = (char *) calloc(length + 1, sizeof(char));
    memcpy(str, src, length);

    SymArrayItem *item = (SymArrayItem *) calloc(1, sizeof(SymArrayItem));
    if (this->head == NULL) {
        this->head = item;
    } else {
        this->tail->next = item;
        item->previous = this->tail;
    }
    item->str = str;
    this->tail = item;
    this->size++;
}

char * SymArrayBuilder_get(SymArrayBuilder *this, int index) {
    SymArrayItem *item = this->head;
    while (item != NULL && index-- > 0) {
        item = item->next;
    }
    return item->str;
}

char * SymArrayBuilder_to_string(SymArrayBuilder *this, int index) {
    char *value = this->get(this, index);
    char *str = (char *) malloc((strlen(value) * sizeof(char)) + 1);
    return strcpy(str, value);
}

char ** SymArrayBuilder_to_array_range(SymArrayBuilder *this, int startIndex, int endIndex) {
    char **array = (char **) calloc(endIndex - startIndex, sizeof(char *));
    SymArrayItem *item = this->head;
    int i, j;
    for (i = 0, j = 0; item != NULL; i++) {
        if (i >= startIndex && i < endIndex) {
            char *str = (char *) malloc((strlen(item->str) * sizeof(char)) + 1);
            strcpy(str, item->str);
            array[j++] = str;
        }
        item = item->next;
    }
    return array;
}

char ** SymArrayBuilder_to_array(SymArrayBuilder *this) {
    return this->to_array_range(this, 0, this->size);
}

void SymArrayBuilder_reset(SymArrayBuilder *this) {
    SymArrayItem *item = this->head;
    while (item != NULL) {
        free(item->str);
        SymArrayItem *nextItem = item->next;
        free(item);
        item = nextItem;
    }
    this->head = this->tail = NULL;
    this->size = 0;
}

void SymArrayBuilder_destroy(SymArrayBuilder *this) {
    this->reset(this);
    free(this);
}

void SymArrayBuilder_destroy_array(char **array, int size) {
    if (size > 0) {
        int i;
        for (i = 0; i < size; i++) {
            free(array[i]);
        }
        free(array);
    }
}

void SymArrayBuilder_print_array(char **array, int size) {
    if (size > 0) {
        int i;
        for (i = 0; i < size; i++) {
            printf("%s", array[i]);
            if (i + 1 < size) {
                printf("|");
            }
        }
        printf("\n");
    }
}

char ** SymArrayBuilder_copy_array(char **array, int size) {
    char **copy = NULL;
    if (array != NULL && size > 0) {
        int i;
        copy = (char **) calloc(size, sizeof(char *));
        for (i = 0; i < size; i++) {
            copy[i] = strcpy((char *) calloc(strlen(array[i]) + 1, sizeof(char)), array[i]);
        }
    }
    return copy;
}

SymArrayBuilder * SymArrayBuilder_new_with_string(char *str) {
    SymArrayBuilder *ab = SymArrayBuilder_new();
    ab->add(ab, str);
    return ab;
}

SymArrayBuilder * SymArrayBuilder_new() {
    SymArrayBuilder *this = (SymArrayBuilder *) calloc(1, sizeof(SymArrayBuilder));
    this->add = (void *) &SymArrayBuilder_add;
    this->addn = (void *) &SymArrayBuilder_addn;
    this->get = (void *) &SymArrayBuilder_get;
    this->to_string = (void *) &SymArrayBuilder_to_string;
    this->to_array = (void *) &SymArrayBuilder_to_array;
    this->to_array_range = (void *) &SymArrayBuilder_to_array_range;
    this->reset = (void *) &SymArrayBuilder_reset;
    this->destroy = (void *) &SymArrayBuilder_destroy;
    return this;
}
