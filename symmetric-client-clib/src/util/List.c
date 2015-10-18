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

void SymList_addAll(SymList *this, SymList *source) {
    SymListItem *item = source->head;
    while (item != NULL) {
        this->add(this, item->object);
        item = item->next;
    }
}

void * SymList_get(SymList *this, int index) {
    void *object = NULL;
    if (index < this->size) {
        SymListItem *item = this->head;
        while (item != NULL && index-- > 0) {
            item = item->next;
        }
        object = item->object;
    }
    return object;
}

int SymList_indexOf(SymList *this, void *object, void *compare(void *object1, void *object2)) {
    SymListItem *item = this->head;
    int i;
    for (i = 0; item != NULL; i++) {
        if (compare(object, item->object) == 0) {
            return i;
        }
        item = item->next;
    }
    return -1;
}

unsigned short SymList_contains(SymList *this, void *object, void *compare(void *object1, void *object2)) {
    return SymList_indexOf(this, object, compare) != -1;
}

void * SymList_remove(SymList *this, int index) {
    SymListItem *item = this->head;
    int i;
    for (i = 0; item != NULL; i++) {
        if (i == index) {
            if (item == this->head && item == this->tail) {
                this->head = this->tail = NULL;
            } else if (item == this->head) {
                this->head = item->next;
                item->next->previous = NULL;
            } else if (item == this->tail) {
                this->tail = item->previous;
                item->previous->next = NULL;
            } else {
                item->next->previous = item->previous;
                item->previous->next = item->next;
            }
            void *object = item->object;
            free(item);
            this->size--;
            return object;
        }
        item = item->next;
    }
    return NULL;
}

void * SymList_removeObject(SymList *this, void *object, void *compare(void *object1, void *object2)) {
    int index = SymList_indexOf(this, object, compare);
    if (index != -1) {
        return SymList_remove(this, index);
    }
    return 0;
}

unsigned short SymIterator_hasNext(SymIterator *this) {
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

SymIterator * SymList_iteratorFromIndex(SymList *this, int startIndex) {
    SymIterator *iter = (SymIterator *) calloc(1, sizeof(SymIterator));
    iter->size = this->size;
    iter->index = -1;
    iter->currentItem = this->head;
    iter->hasNext = (void *) &SymIterator_hasNext;
    iter->next = (void *) &SymIterator_next;
    iter->destroy = (void *) &SymIterator_destroy;

    while (startIndex-- > 0 && iter->hasNext(iter)) {
        iter->next(iter);
    }
    return iter;
}

SymIterator * SymList_iterator(SymList *this) {
    return SymList_iteratorFromIndex(this, 0);
}

void SymList_resetAll(SymList *this, void *destroy_object(void * object)) {
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
    SymList_resetAll(this, NULL);
}

void SymList_destroy(SymList *this) {
    this->reset(this);
    free(this);
}

void SymList_destroyAll(SymList *this, void *destroy_object(void * object)) {
    SymList_resetAll(this, destroy_object);
    free(this);
}

SymList * SymList_new(SymList *this) {
    if (this == NULL) {
        this = (SymList *) calloc(1, sizeof(SymList));
    }
    this->add = (void *) &SymList_add;
    this->addAll = (void *) &SymList_addAll;
    this->get = (void *) &SymList_get;
    this->indexOf = (void *) &SymList_indexOf;
    this->contains = (void *) &SymList_contains;
    this->remove = (void *) &SymList_remove;
    this->removeObject = (void *) &SymList_removeObject;
    this->iterator = (void *) &SymList_iterator;
    this->iteratorFromIndex = (void *) &SymList_iteratorFromIndex;
    this->reset = (void *) &SymList_reset;
    this->resetAll = (void *) &SymList_resetAll;
    this->destroy = (void *) &SymList_destroy;
    this->destroyAll = (void *) &SymList_destroyAll;
    return this;
}
