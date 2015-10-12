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

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>

#define SYM_STRING_BUILDER_SIZE 255

typedef struct SymStringBuilder {
    char *str;
    int pos;
    int size;
    struct SymStringBuilder * (*append)(struct SymStringBuilder *this, const char *src);
    struct SymStringBuilder * (*appendn)(struct SymStringBuilder *this, const char *src, int length);
    struct SymStringBuilder * (*appendf)(struct SymStringBuilder *this, const char *fmt, ...);
    struct SymStringBuilder * (*appendfv)(struct SymStringBuilder *this, const char *fmt, va_list varargs);
    struct SymStringBuilder * (*appendInt)(struct SymStringBuilder *this, int number);
    char * (*toString)(struct SymStringBuilder * this);
    char * (*substring)(struct SymStringBuilder *this, int startIndex, int endIndex);
    void (*reset)(struct SymStringBuilder * this);
    void (*destroy)(struct SymStringBuilder * this);
    char * (*destroyAndReturn)(struct SymStringBuilder * this);
} SymStringBuilder;

SymStringBuilder * SymStringBuilder_new();

SymStringBuilder * SymStringBuilder_newWithSize(int size);

SymStringBuilder * SymStringBuilder_newWithString(char *str);

int SymStringBuilder_hashCode(char *value);

char * SymStringBuilder_copy(char *str);

void SymStringBuilder_copyToField(char **strField, char *str);

#endif
