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

SymMap * SymCsvData_toColumnNameValuePairsValues(SymCsvData *this, SymStringArray *values, SymStringArray *keyNames) {
    SymMap *map = SymMap_new(NULL, 16);
    if (values != NULL && keyNames != NULL && values->size >= keyNames->size) {
        int i;
        for (i = 0; i < keyNames->size; ++i) {
            map->put(map, keyNames->get(keyNames, i), values->get(values, i));
        }
    }
    return map;
}

SymMap * SymCsvData_toColumnNameValuePairsRowData(SymCsvData *this, SymStringArray *keyNames) {
    SymStringArray *values = this->rowData;
    return SymCsvData_toColumnNameValuePairsValues(this, values, keyNames);
}

SymMap * SymCsvData_toColumnNameValuePairsOldData(SymCsvData *this, SymStringArray *keyNames) {
    SymStringArray *values = this->oldData;
    return SymCsvData_toColumnNameValuePairsValues(this, values, keyNames);
}

void SymCsvData_reset(SymCsvData *this) {
    if (this->rowData) {
        this->rowData->destroy(this->rowData);
    }
    if (this->oldData) {
        this->oldData->destroy(this->oldData);
    }
    if (this->pkData) {
        this->pkData->destroy(this->pkData);
    }
}

void SymCsvData_destroy(SymCsvData *this) {
    this->reset(this);
    free(this);
}

SymCsvData * SymCsvData_new(SymCsvData *this) {
    if (this == NULL) {
        this = (SymCsvData *) calloc(1, sizeof(SymCsvData));
    }
    this->toColumnNameValuePairsRowData = (void *) &SymCsvData_toColumnNameValuePairsRowData;
    this->toColumnNameValuePairsOldData = (void *) &SymCsvData_toColumnNameValuePairsOldData;
    this->reset = (void *) &SymCsvData_reset;
    this->destroy = (void *) &SymCsvData_destroy;
    return this;
}
