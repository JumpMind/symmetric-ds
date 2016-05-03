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
#ifndef SYM_PROPERTIES_H
#define SYM_PROPERTIES_H

#include <string.h>
#include <stdlib.h>
#include "util/StringUtils.h"
#include "util/StringBuilder.h"
#include "util/StringArray.h"

#define SYM_NOT_FOUND -1

typedef struct {
    char *key;
    char *value;
} SymProperty;

typedef struct SymProperties {
    SymProperty *propArray;
    int index;
    char * (*get)(struct SymProperties *this, char *key, char *defaultValue);
    void (*put)(struct SymProperties *this, char *key, char *value);
    void (*putAll)(struct SymProperties *this, void *properties);
    char * (*toString)(struct SymProperties *this);
    void (*destroy)(struct SymProperties *this);
} SymProperties;

SymProperties * SymProperties_new(SymProperties *);
SymProperties * SymProperties_newWithString(SymProperties *this, char *propertiesFileContents);
SymProperties * SymProperties_newWithFile(SymProperties *, char * argPath);

#endif
