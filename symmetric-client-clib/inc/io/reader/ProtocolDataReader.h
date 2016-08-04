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
#ifndef SYM_PROTOCOL_DATA_READER_H
#define SYM_PROTOCOL_DATA_READER_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <curl/curl.h>
#include <csv.h>
#include "common/Log.h"
#include "io/data/DataProcessor.h"
#include "io/writer/DataWriter.h"
#include "model/Node.h"
#include "transport/IncomingTransport.h"
#include "util/StringArray.h"
#include "util/Map.h"
#include "io/data/CsvConstants.h"
#include "io/data/CsvData.h"
#include "util/StringUtils.h"

typedef struct SymProtocolDataReader {
    SymDataProcessor super;
    char *targetNodeId;
    SymDataWriter *writer;
    struct csv_parser *csvParser;
    SymStringArray *fields;
    SymBatch *batch;
    SymTable *table;
    char *catalog;
    char *schema;
    SymStringArray *oldData;
    SymStringArray *keys;
    SymMap *parsedTables;
    unsigned short isError;
} SymProtocolDataReader;

SymProtocolDataReader * SymProtocolDataReader_new(SymProtocolDataReader *this, char *targetNodeId, SymDataWriter *writer);

#endif
