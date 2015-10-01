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

typedef struct {
    void *object;
    void *previous;
    void *next;
} SymListItem;

typedef struct {
    int size;
    int index;
    SymListItem *currentItem;
    unsigned short (*has_next)(void *this);
    void * (*next)(void *this);
    void (*destroy)(void *this);
} SymIterator;

typedef struct {
    SymListItem *head;
    SymListItem *tail;
    int size;
    void (*add)(void *this, void *object);
    void * (*get)(void *this, int index);
    SymIterator * (*iterator)(void *this);
    SymIterator * (*iterator_from_index)(void *this, int startIndex);
    void (*reset)(void *this);
    void (*reset_all)(void *this, void *destroy_object(void * object));
    void (*destroy)(void *this);
    void (*destroy_all)(void *this, void *destroy_object(void * object));
} SymList;

SymList * SymList_new(SymList *this);

#endif
