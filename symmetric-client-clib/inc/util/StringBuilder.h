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
#ifndef SYM_STRING_BUILDER_H
#define SYM_STRING_BUILDER_H

#include <string.h>
#include <stdlib.h>
#include <stdarg.h>

#define SYM_STRING_BUILDER_SIZE 255

typedef struct {
    char *str;
    int pos;
    int size;
    void (*append)(void *this, const char *src);
    void (*appendn)(void *this, const char *src, int length);
    void (*appendf)(void *this, const char *fmt, ...);
    char * (*to_string)(void * this);
    void (*reset)(void * this);
    void (*destroy)(void * this);
    char * (*destroy_and_return)(void * this);
} SymStringBuilder;

SymStringBuilder * SymStringBuilder_new();

SymStringBuilder * SymStringBuilder_new_with_size(int size);

SymStringBuilder * SymStringBuilder_new_with_string(char *str);

char * SymStringBuilder_copy(char *str);

#endif
