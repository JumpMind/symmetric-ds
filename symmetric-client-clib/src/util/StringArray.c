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
#include "util/StringArray.h"

SymStringArray * SymStringArray_addn(SymStringArray *this, char *src, int size) {
    if (this->size == this->sizeAllocated) {
        this->sizeAllocated += this->sizeIncrement;
        this->array = realloc(this->array, this->sizeAllocated);
    }
    char *str = NULL;
    if (size > 0) {
        str = (char *) memcpy(malloc(size + 1), src, size);
        str[size] = '\0';
    }
    this->array[this->size++] = str;
    return this;
}

SymStringArray * SymStringArray_add(SymStringArray *this, char *src) {
    int size = 0;
    if (src) {
        size = strlen(src);
    }
    SymStringArray_addn(this, src, size);
    return this;
}

SymStringArray * SymStringArray_addf(SymStringArray *this, const char *fmt, ...) {
    va_list arglist;
    va_start(arglist, fmt);
    int sizeNeeded = vsnprintf(NULL, 0, fmt, arglist) + 1;
    va_end(arglist);

    char *str = malloc(sizeNeeded + 1);
    va_start(arglist, fmt);
    vsprintf(str, fmt, arglist);
    va_end(arglist);

    SymStringArray_addn(this, str, sizeNeeded);
    free(str);
    return this;
}

SymStringArray * SymStringArray_addInt(SymStringArray *this, int value) {
    return this->addf(this, "%d", value);
}

SymStringArray * SymStringArray_addLong(SymStringArray *this, long value) {
    return this->addf(this, "%ld", value);
}

SymStringArray * SymStringArray_addAll(SymStringArray *this, SymStringArray *stringArray) {
    int i;
    for (i = 0; i < stringArray->size; i++) {
        SymStringArray_add(this, stringArray->array[i]);
    }
    return this;
}

char * SymStringArray_get(SymStringArray *this, int index) {
    return this->array[index];
}

unsigned short SymStringArray_contains(SymStringArray *this, char *findStr) {
    int i;
    for (i = 0; i < this->size; i++) {
        if (strcmp(this->array[i], findStr) == 0) {
            return 1;
        }
    }
    return 0;
}

SymStringArray * SymStringArray_subarray(SymStringArray *this, int startIndex, int endIndex) {
    SymStringArray *strArray = SymStringArray_newWithSize(NULL, endIndex - startIndex, this->sizeIncrement);
    int i;
    for (i = startIndex; i < endIndex; i++) {
        strArray->add(strArray, this->array[i]);
    }
    return strArray;
}

void SymStringArray_print(SymStringArray *this) {
    if (this->size > 0) {
        int i;
        for (i = 0; i < this->size; i++) {
            printf("%s", this->array[i]);
            if (i + 1 < this->size) {
                printf("|");
            }
        }
        printf("\n");
    }
}

void SymStringArray_reset(SymStringArray *this) {
    int i;
    for (i = 0; i < this->size; i++) {
        free(this->array[i]);
    }
    this->size = 0;
}

void SymStringArray_destroy(SymStringArray *this) {
    this->reset(this);
    free(this);
}

SymStringArray * SymStringArray_new(SymStringArray *this) {
    return SymStringArray_newWithSize(this, SYM_STRING_ARRAY_SIZE_INITIAL, SYM_STRING_ARRAY_SIZE_INCREMENT);
}

SymStringArray * SymStringArray_newWithSize(SymStringArray *this, int sizeInitial, int sizeIncrement) {
    if (this == NULL) {
        this = (SymStringArray *) calloc(1, sizeof(SymStringArray));
    }
    this->sizeAllocated = sizeInitial;
    this->sizeIncrement = sizeIncrement;
    this->array = (char **) malloc(this->sizeAllocated * sizeof(char *));
    this->add = (void *) &SymStringArray_add;
    this->addn = (void *) &SymStringArray_addn;
    this->addf = (void *) &SymStringArray_addf;
    this->addInt = (void *) &SymStringArray_addInt;
    this->addLong = (void *) &SymStringArray_addLong;
    this->addAll = (void *) &SymStringArray_addAll;
    this->get = (void *) &SymStringArray_get;
    this->contains = (void *) &SymStringArray_contains;
    this->subarray = (void *) &SymStringArray_subarray;
    this->print = (void *) &SymStringArray_print;
    this->reset = (void *) &SymStringArray_reset;
    this->destroy = (void *) &SymStringArray_destroy;
    return this;
}
