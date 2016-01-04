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
#include "load/DefaultDataLoaderFactory.h"

static SymDatabaseWriterSettings * buildDatabaseWriterSettings(SymDefaultDataLoaderFactory *this) {
    SymDatabaseWriterSettings *settings = SymDatabaseWriterSettings_new(NULL);
    settings->ignoreMissingTables = this->parameterService->is(this->parameterService, SYM_PARAMETER_DATA_LOADER_IGNORE_MISSING_TABLES, 1);
    settings->usePrimaryKeysFromSource = this->parameterService->is(this->parameterService, SYM_PARAMETER_DATA_LOADER_USE_PRIMARY_KEYS_FROM_SOURCE, 1);
    return settings;
}

SymDataWriter * SymDefaultDataLoaderFactory_getDataWriter(SymDefaultDataLoaderFactory *this) {
    SymDatabaseWriterSettings *settings = buildDatabaseWriterSettings(this);
    SymDataWriter *writer = (SymDataWriter *) SymDefaultDatabaseWriter_new(NULL, this->incomingBatchService, this->parameterService,
            this->platform, this->dialect, settings);
    return writer;
}

void SymDefaultDataLoaderFactory_destroy(SymDefaultDataLoaderFactory *this) {
    free(this);
}

SymDefaultDataLoaderFactory * SymDefaultDataLoaderFactory_new(SymDefaultDataLoaderFactory *this, SymParameterService *parameterService,
        SymIncomingBatchService *incomingBatchService, SymDatabasePlatform *platform, SymDialect *dialect) {
    if (this == NULL) {
        this = (SymDefaultDataLoaderFactory *) calloc(1, sizeof(SymDefaultDataLoaderFactory));
    }
    this->parameterService = parameterService;
    this->incomingBatchService = incomingBatchService;
    this->platform = platform;
    this->dialect = dialect;
    this->getDataWriter = SymDefaultDataLoaderFactory_getDataWriter;
    this->destroy = (void *) &SymDefaultDataLoaderFactory_destroy;
    return this;
}
