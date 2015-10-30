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
#ifndef SYM_DATA_LOADER_FACTORY_H
#define SYM_DATA_LOADER_FACTORY_H

#include <stdlib.h>
#include "service/ParameterService.h"
#include "service/IncomingBatchService.h"
#include "db/platform/DatabasePlatform.h"
#include "db/SymDialect.h"
#include "io/writer/DataWriter.h"
#include "io/writer/DatabaseWriterSettings.h"
#include "io/writer/DefaultDatabaseWriter.h"

typedef struct SymDefaultDataLoaderFactory {
    SymParameterService *parameterService;
    SymIncomingBatchService *incomingBatchService;
    SymDatabasePlatform *platform;
    SymDialect *dialect;
    SymDataWriter *(*getDataWriter)(struct SymDefaultDataLoaderFactory *this);
    void (*destroy)(struct SymDefaultDataLoaderFactory *this);
} SymDefaultDataLoaderFactory;

SymDefaultDataLoaderFactory * SymDefaultDataLoaderFactory_new(SymDefaultDataLoaderFactory *this, SymParameterService *parameterService,
        SymIncomingBatchService *incomingBatchService, SymDatabasePlatform *platform, SymDialect *dialect);

#endif
