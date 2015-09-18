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
#ifndef SYM_ARRAY_BUILDER_H
#define SYM_ARRAY_BUILDER_H

#include <string.h>
#include <stdlib.h>
#include <stdarg.h>

typedef struct {
    char *str;
    void *previous;
    void *next;
} SymArrayItem;

typedef struct {
    SymArrayItem *head;
    SymArrayItem *tail;
    int size;
    void (*add)(void *this, char *src);
    void (*addn)(void *this, const char *src, int length);
    char * (*get)(void *this, int index);
    char * (*to_string)(void *this, int index);
    char ** (*to_array)(void *this);
    char ** (*to_array_range)(void *this, int startIndex, int endIndex);
    void (*reset)(void *this);
    void (*destroy)(void *this);
    void (*destroy_array)(char **array, int size);
} SymArrayBuilder;

SymArrayBuilder * SymArrayBuilder_new();

SymArrayBuilder * SymArrayBuilder_new_with_size(int size);

SymArrayBuilder * SymArrayBuilder_new_with_string(char *str);

void SymArrayBuilder_destroy_array(char **array, int size);

char ** SymArrayBuilder_copy_array(char **array, int size);

#endif
