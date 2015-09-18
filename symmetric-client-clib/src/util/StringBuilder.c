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

void SymStringBuilder_appendn(SymStringBuilder *this, const char *src, int length) {
    int sdiff = length - (this->size - this->pos) + 1;
    if (sdiff > 0) {
        this->size = this->size + sdiff;
        this->str = (char *) realloc(this->str, this->size);
    }
    memcpy(this->str + this->pos, src, length);
    this->pos += length;
    this->str[this->pos] = NULL;
}

void SymStringBuilder_append(SymStringBuilder *this, const char *src) {
    if (src != NULL) {
        SymStringBuilder_appendn(this, src, strlen(src));
    }
}

void SymStringBuilder_appendf(SymStringBuilder *this, const char *fmt, ...) {
    char *str;
    va_list arglist;

    va_start(arglist, fmt);
    vsprintf(&str, fmt, arglist);
    va_end(arglist);

    if (str) {
        SymStringBuilder_append(this, str);
        free(str);
    }
}

char * SymStringBuilder_to_string(SymStringBuilder *this) {
    return this->str;
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

SymStringBuilder * SymStringBuilder_new() {
    return SymStringBuilder_new_with_size(SYM_STRING_BUILDER_SIZE);
}

SymStringBuilder * SymStringBuilder_new_with_string(char *str) {
    SymStringBuilder *sb = SymStringBuilder_new_with_size(strlen(str) + 1);
    sb->append(sb, str);
    return sb;
}

SymStringBuilder * SymStringBuilder_new_with_size(int size) {
    SymStringBuilder *this = (SymStringBuilder *) calloc(1, sizeof(SymStringBuilder));
    this->size = size + 255;
    this->str = (char *) calloc(size + 255, sizeof(char));
    this->pos = 0;

    this->append = (void *) &SymStringBuilder_append;
    this->appendn = (void *) &SymStringBuilder_appendn;
    this->appendf = (void *) &SymStringBuilder_appendf;
    this->to_string = (void *) &SymStringBuilder_to_string;
    this->destroy = (void *) &SymStringBuilder_destroy;
    this->destroy_and_return = (void *) &SymStringBuilder_destroy_and_return;
    return this;
}
