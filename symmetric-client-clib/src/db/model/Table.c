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
#include "db/model/Table.h"

void SymTable_set(char **strField, char *str) {
    free(*strField);
    *strField = SymStringBuilder_copy(str);
}

void SymTable_set_array(char ***arrayField, int *sizeField, char **array, int sizeArray) {
    SymArrayBuilder_destroy_array(*arrayField, *sizeField);
    *arrayField = SymArrayBuilder_copy_array(array, sizeArray);
    *sizeField = sizeArray;
}

void SymTable_destroy(SymTable *this) {
    free(this->name);
    free(this->catalog);
    free(this->schema);
    SymArrayBuilder_destroy_array(this->columns, this->sizeColumns);
    SymArrayBuilder_destroy_array(this->keys, this->sizeKeys);
    free(this);
}

SymTable * SymTable_new(SymTable *this) {
    if (this == NULL) {
        this = (SymTable *) calloc(1, sizeof(SymTable));
    }
    this->set = (void *) &SymTable_set;
    this->set_array = (void *) &SymTable_set_array;
    this->destroy = (void *) &SymTable_destroy;
    return this;
}
