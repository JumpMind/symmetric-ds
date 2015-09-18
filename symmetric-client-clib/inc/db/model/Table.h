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
#ifndef SYM_TABLE_H
#define SYM_TABLE_H

#include <stdio.h>
#include <stdlib.h>

typedef struct {
    char *catalog;
    char *schema;
    char *name;
    char **keys;
    char **columns;
    int sizeKeys;
    int sizeColumns;
    void (*set)(char **strField, char *str);
    void (*set_array)(char ***arrayField, int *sizeField, char **array, int sizeArray);
    void (*destroy)(void *this);
} SymTable;

SymTable * SymTable_new(SymTable *this);

#endif
