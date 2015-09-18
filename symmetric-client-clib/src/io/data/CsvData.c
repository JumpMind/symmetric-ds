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
#include "io/data/CsvData.h"

void SymCsvData_set_array(char ***arrayField, int *sizeField, char **array, int sizeArray) {
    SymArrayBuilder_destroy_array(*arrayField, *sizeField);
    *arrayField = SymArrayBuilder_copy_array(array, sizeArray);
    *sizeField = sizeArray;
}

void SymCsvData_reset(SymCsvData *this) {
    SymArrayBuilder_destroy_array(this->rowData, this->sizeRowData);
    SymArrayBuilder_destroy_array(this->pkData, this->sizePkData);
    SymArrayBuilder_destroy_array(this->oldData, this->sizeOldData);
    this->sizeRowData = 0;
    this->sizePkData = 0;
    this->sizeOldData = 0;
}

void SymCsvData_destroy(SymCsvData *this) {
    this->reset(this);
    free(this);
}

SymCsvData * SymCsvData_new(SymCsvData *this) {
    if (this == NULL) {
        this = (SymCsvData *) calloc(1, sizeof(SymCsvData));
    }
    this->set_array = (void *) &SymCsvData_set_array;
    this->reset = (void *) &SymCsvData_reset;
    this->destroy = (void *) &SymCsvData_destroy;
    return this;
}

SymCsvData * SymCsvData_new_with_settings(SymCsvData *this, SymDataEventType dataEventType, char **rowData, char **pkData, char **oldData,
        int sizeRowData, int sizePkData, int sizeOldData) {
    this = SymCsvData_new(this);
    this->rowData = SymArrayBuilder_copy_array(rowData, sizeRowData);
    this->pkData = SymArrayBuilder_copy_array(pkData, sizePkData);
    this->oldData = SymArrayBuilder_copy_array(oldData, sizeOldData);
    return this;
}
