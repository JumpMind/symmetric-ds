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
#ifndef SYM_LIST_H
#define SYM_LIST_H

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>

typedef struct SymListItem {
    void *object;
    struct SymListItem *previous;
    struct SymListItem *next;
} SymListItem;

typedef struct SymIterator {
    int size;
    int index;
    SymListItem *currentItem;
    unsigned short (*hasNext)(struct SymIterator *this);
    void * (*next)(struct SymIterator *this);
    void (*destroy)(struct SymIterator *this);
} SymIterator;

typedef struct SymList {
    SymListItem *head;
    SymListItem *tail;
    int size;
    void (*add)(struct SymList *this, void *object);
    void (*addAll)(struct SymList *this, struct SymList *source);
    void * (*get)(struct SymList *this, int index);
    int (*indexOf)(struct SymList *this, void *object, void *compare(void *object1, void *object2));
    unsigned short (*contains)(struct SymList *this, void *object, void *compare(void *object1, void *object2));
    void * (*remove)(struct SymList *this, int index);
    void * (*removeObject)(struct SymList *this, void *object, void *compare(void *object1, void *object2));
    SymIterator * (*iterator)(struct SymList *this);
    SymIterator * (*iteratorFromIndex)(struct SymList *this, int startIndex);
    void (*reset)(struct SymList *this);
    void (*resetAll)(struct SymList *this, void *destroy_object(void * object));
    void (*destroy)(struct SymList *this);
    void (*destroyAll)(struct SymList *this, void *destroy_object(void * object));
} SymList;

SymList * SymList_new(SymList *this);

void SymList_destroyAll(SymList *this, void *destroy_object(void * object));

#endif
