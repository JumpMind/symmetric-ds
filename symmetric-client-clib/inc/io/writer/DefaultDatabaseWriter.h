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
#ifndef SYM_DEFAULT_DATABASE_WRITER_H
#define SYM_DEFAULT_DATABASE_WRITER_H

#include <stdio.h>
#include <stdlib.h>
#include "io/writer/DataWriter.h"
#include "io/writer/DatabaseWriterSettings.h"
#include "io/data/Batch.h"
#include "db/model/Table.h"
#include "db/platform/DatabasePlatform.h"
#include "io/data/CsvData.h"
#include "util/StringArray.h"
#include "util/Map.h"
#include "util/List.h"
#include "util/Base64.h"
#include "util/Hex.h"
#include "db/sql/DmlStatement.h"
#include "db/sql/SqlTemplate.h"
#include "db/sql/SqlTransaction.h"
#include "db/SymDialect.h"
#include "model/IncomingBatch.h"
#include "service/IncomingBatchService.h"
#include "service/ParameterService.h"
#include "common/TableConstants.h"
#include "common/ParameterConstants.h"
#include "common/Constants.h"

typedef struct SymDefaultDatabaseWriter {
    SymDataWriter super;
    SymIncomingBatchService *incomingBatchService;
    SymParameterService *parameterService;
    SymDatabasePlatform *platform;
    SymDialect *dialect;
    SymDatabaseWriterSettings *settings;
    SymSqlTransaction *sqlTransaction;
    SymBatch *batch;
    SymTable *sourceTable;
    SymTable *targetTable;
    SymMap *targetTables;
    SymDmlStatement *dmlStatement;
    SymIncomingBatch *incomingBatch;
    SymMap *missingTables;
    unsigned short isError;
} SymDefaultDatabaseWriter;

SymDefaultDatabaseWriter * SymDefaultDatabaseWriter_new(SymDefaultDatabaseWriter *this, SymIncomingBatchService *incomingBatchService,
        SymParameterService *parameterService, SymDatabasePlatform *platform, SymDialect *dialect, SymDatabaseWriterSettings *settings);

#endif
