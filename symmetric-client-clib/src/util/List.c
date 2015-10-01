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
#include "util/List.h"

void SymList_add(SymList *this, void *object) {
    SymListItem *item = (SymListItem *) calloc(1, sizeof(SymListItem));
    if (this->head == NULL) {
        this->head = item;
    } else {
        this->tail->next = item;
        item->previous = this->tail;
    }
    item->object = object;
    this->tail = item;
    this->size++;
}

void * SymList_get(SymList *this, int index) {
    SymListItem *item = this->head;
    while (item != NULL && index-- > 0) {
        item = item->next;
    }
    return item->object;
}

unsigned short SymIterator_has_next(SymIterator *this) {
    return this->index + 1 < this->size && this->currentItem != NULL;
}

void * SymIterator_next(SymIterator *this) {
    this->index++;
    void *object = this->currentItem->object;
    this->currentItem = this->currentItem->next;
    return object;
}

void SymIterator_destroy(SymIterator *this) {
    free(this);
}

SymIterator * SymList_iterator_from_index(SymList *this, int startIndex) {
    SymIterator *iter = (SymIterator *) calloc(1, sizeof(SymIterator));
    iter->size = this->size;
    iter->index = -1;
    iter->currentItem = this->head;
    iter->has_next = (void *) &SymIterator_has_next;
    iter->next = (void *) &SymIterator_next;
    iter->destroy = (void *) &SymIterator_destroy;

    while (startIndex-- > 0 && iter->has_next(iter)) {
        iter->next(iter);
    }
    return iter;
}

SymIterator * SymList_iterator(SymList *this) {
    return SymList_iterator_from_index(this, 0);
}

void SymList_reset_all(SymList *this, void *destroy_object(void * object)) {
    SymListItem *item = this->head;
    while (item != NULL) {
        if (destroy_object != NULL) {
            destroy_object(item->object);
        }
        SymListItem *nextItem = item->next;
        free(item);
        item = nextItem;
    }
    this->head = this->tail = NULL;
    this->size = 0;
}

void SymList_reset(SymList *this) {
    SymList_reset_all(this, NULL);
}

void SymList_destroy(SymList *this) {
    this->reset(this);
    free(this);
}

void SymList_destroy_all(SymList *this, void *destroy_object(void * object)) {
    SymList_reset_all(this, destroy_object);
    free(this);
}

SymList * SymList_new(SymList *this) {
    if (this == NULL) {
        this = (SymList *) calloc(1, sizeof(SymList));
    }
    this->add = (void *) &SymList_add;
    this->get = (void *) &SymList_get;
    this->iterator = (void *) &SymList_iterator;
    this->iterator_from_index = (void *) &SymList_iterator_from_index;
    this->reset = (void *) &SymList_reset;
    this->reset_all = (void *) &SymList_reset_all;
    this->destroy = (void *) &SymList_destroy;
    this->destroy_all = (void *) &SymList_destroy_all;
    return this;
}
