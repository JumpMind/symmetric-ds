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
#ifndef SYM_DATA_READER_H
#define SYM_DATA_READER_H

#include <stdio.h>
#include <stdlib.h>
#include "io/data/Batch.h"
#include "db/model/Table.h"
#include "io/data/CsvData.h"
#include "model/Data.h"

typedef struct SymDataReader {
    SymList *batchesProcessed;
    void (*open)(struct SymDataReader *this);
    SymBatch * (*nextBatch)(struct SymDataReader *this);
    SymTable * (*nextTable)(struct SymDataReader *this);
    SymData * (*nextData)(struct SymDataReader *this);
    void (*close)(struct SymDataReader *this);
    void (*destroy)(struct SymDataReader *this);
} SymDataReader;

SymDataReader * SymDataReader_new(SymDataReader *this);

#endif
