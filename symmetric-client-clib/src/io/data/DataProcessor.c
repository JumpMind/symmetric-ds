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
#include "io/data/DataProcessor.h"


void SymDataProcessor_processWithReader(SymDataContext *context, SymDataWriter *writer, SymDataReader *reader) {
    SymBatch *batch = NULL;
    SymTable *table = NULL;
    SymData *data = NULL;

    while ((batch = reader->nextBatch(reader)) != NULL) {
        writer->startBatch(writer, batch);

        while ((table = reader->nextTable(reader))) {
            writer->startTable(writer, table);

            while ((data = reader->nextData(reader)) != NULL) {
                SymCsvData *csvData = SymCsvData_new(NULL);
                csvData->rowData = SymCsvUtils_tokenizeCsvData(data->rowData);
                csvData->pkData = SymCsvUtils_tokenizeCsvData(data->pkData);
                csvData->oldData = SymCsvUtils_tokenizeCsvData(data->oldData);
                csvData->dataEventType = data->eventType;
                writer->write(writer, csvData);
            }
            writer->endTable(writer, table);
        }
        writer->endBatch(writer, batch);
    }
}
