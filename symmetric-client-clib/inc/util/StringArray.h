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
#ifndef SYM_STRING_ARRAY_H
#define SYM_STRING_ARRAY_H

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>

#define SYM_STRING_ARRAY_SIZE_INITIAL 100
#define SYM_STRING_ARRAY_SIZE_INCREMENT 100

typedef struct SymStringArray {
    char **array;
    int size;
    int sizeAllocated;
    int sizeIncrement;
    void (*add)(void *this, char *src);
    void (*addn)(void *this, char *src, int size);
    char * (*get)(void *this, int index);
    unsigned short (*contains)(void *this, char *findStr);
    struct SymStringArray * (*subarray)(void *this, int startIndex, int endIndex);
    void (*print)(void *this);
    void (*reset)(void *this);
    void (*destroy)(void *this);
} SymStringArray;

SymStringArray * SymStringArray_new(SymStringArray *this);

SymStringArray * SymStringArray_new_with_size(SymStringArray *this, int sizeInitial, int sizeIncrement);

#endif
