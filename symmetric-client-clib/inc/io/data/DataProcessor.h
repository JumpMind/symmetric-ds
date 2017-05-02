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
#ifndef SYM_DATA_PROCESSOR_H
#define SYM_DATA_PROCESSOR_H

#include <stdio.h>
#include <stdlib.h>
#include "util/List.h"
#include "io/writer/DataWriter.h"
#include "io/reader/DataReader.h"
#include "io/data/DataContext.h"
#include "util/CsvUtils.h"

typedef struct SymDataProcessor {
    void (*open)(struct SymDataProcessor *this);
    size_t (*process)(struct SymDataProcessor *this, char *data, size_t size, size_t count);
    SymList * (*getBatchesProcessed)(struct SymDataProcessor *this);
    void (*close)(struct SymDataProcessor *this);
    void (*destroy)(struct SymDataProcessor *this);
} SymDataProcessor;

SymDataProcessor * SymDataProcessor_new(SymDataProcessor *this);
void SymDataProcessor_processWithReader(SymDataContext *context, SymDataWriter *writer, SymDataReader *reader);

#endif
