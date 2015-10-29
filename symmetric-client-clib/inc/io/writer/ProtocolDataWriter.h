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
#ifndef SYM_PROTOCOL_DATA_WRITER_H
#define SYM_PROTOCOL_DATA_WRITER_H

#include <stdio.h>
#include <stdlib.h>
#include "io/data/Batch.h"
#include "io/data/CsvData.h"
#include "io/data/CsvConstants.h"
#include "io/data/DataProcessor.h"
#include "io/reader/DataReader.h"
#include "db/model/Table.h"
#include "model/Data.h"
#include "util/StringBuilder.h"
#include "util/Map.h"
#include "util/StringUtils.h"
#include "common/Log.h"
#include "util/List.h"

typedef struct SymProtocolDataWriter {
    SymDataProcessor super;
    char *sourceNodeId;
    SymDataReader *reader;

    SymStringBuilder *sb;
    SymBatch *batch;
    SymTable *table;
    SymMap *processedTables;
    unsigned short isFirstBatch;
} SymProtocolDataWriter;

SymProtocolDataWriter * SymProtocolDataWriter_new(SymProtocolDataWriter *this, char *sourceNodeId, SymDataReader *reader);

#endif
