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
#include "util/StringBuilder.h"

SymStringBuilder * SymStringBuilder_appendn(SymStringBuilder *this, const char *src, int length) {
    int sdiff = length - (this->size - this->pos) + 1;
    if (sdiff > 0) {
        this->size = this->size + sdiff;
        this->str = (char *) realloc(this->str, this->size);
    }
    memcpy(this->str + this->pos, src, length);
    this->pos += length;
    this->str[this->pos] = '\0';
    return this;
}

SymStringBuilder * SymStringBuilder_append(SymStringBuilder *this, const char *src) {
    if (src == NULL) {
        src = "(null)";
    }
    return SymStringBuilder_appendn(this, src, strlen(src));
}

SymStringBuilder * SymStringBuilder_appendf(SymStringBuilder *this, const char *fmt, ...) {
    va_list arglist;
    va_start(arglist, fmt);
    int sizeNeeded = vsnprintf(NULL, 0, fmt, arglist) + 1;
    va_end(arglist);

    char *str = malloc(sizeNeeded + 1);
    va_start(arglist, fmt);
    vsprintf(str, fmt, arglist);
    va_end(arglist);

    SymStringBuilder_appendn(this, str, sizeNeeded);
    free(str);
    return this;
}

char * SymStringBuilder_toString(SymStringBuilder *this) {
    // TODO: this should return a copy
    return this->str;
}

char * SymStringBuilder_substring(SymStringBuilder *this, int startIndex, int endIndex) {
    int size = endIndex - startIndex + 1;
    char *str = (char *) malloc(size * sizeof(char));
    str[size] = '\0';
    return memcpy(str, str + startIndex, endIndex - startIndex);
}

void SymStringBuilder_reset(SymStringBuilder *this) {
    free(this->str);
    this->size = 255;
    this->str = (char *) calloc(255, sizeof(char));
    this->pos = 0;
}

void SymStringBuilder_destroy(SymStringBuilder *this) {
    free(this->str);
    free(this);
}

char * SymStringBuilder_destroy_and_return(SymStringBuilder *this) {
    char *str = this->str;
    free(this);
    return str;
}

char * SymStringBuilder_copy(char *str) {
    if (str != NULL) {
        return strcpy((char *) calloc(strlen(str) + 1, sizeof(char)), str);
    }
    return NULL;
}

void SymStringBuilder_copyToField(char **strField, char *str) {
    free(*strField);
    *strField = SymStringBuilder_copy(str);
}

SymStringBuilder * SymStringBuilder_new() {
    return SymStringBuilder_newWithSize(SYM_STRING_BUILDER_SIZE);
}

SymStringBuilder * SymStringBuilder_newWithString(char *str) {
    SymStringBuilder *sb = SymStringBuilder_newWithSize(strlen(str) + 1);
    sb->append(sb, str);
    return sb;
}

SymStringBuilder * SymStringBuilder_newWithSize(int size) {
    SymStringBuilder *this = (SymStringBuilder *) calloc(1, sizeof(SymStringBuilder));
    this->size = size + 255;
    this->str = (char *) calloc(size + 255, sizeof(char));
    this->pos = 0;

    this->append = (void *) &SymStringBuilder_append;
    this->appendn = (void *) &SymStringBuilder_appendn;
    this->appendf = (void *) &SymStringBuilder_appendf;
    this->toString = (void *) &SymStringBuilder_toString;
    this->substring = (void *) &SymStringBuilder_substring;
    this->destroy = (void *) &SymStringBuilder_destroy;
    this->destroyAndReturn = (void *) &SymStringBuilder_destroy_and_return;
    return this;
}
