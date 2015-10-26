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
#include "route/DataGapRouteReader.h"

static SymData * SymDataGapRouteReader_dataMapper(SymRow *row, SymDataGapRouteReader *this) {
    return this->dataService->mapData(this->dataService, row);
}

SymList * SymDataGapRouteReader_selectDataFor(SymDataGapRouteReader *this) {
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    SymStringBuilder *sql = SymStringBuilder_newWithString(SYM_SQL_SELECT_DATA_USING_CHANNEL_ID);
    if (this->parameterService->is(this->parameterService, SYM_PARAMETER_ROUTING_DATA_READER_ORDER_BY_DATA_ID_ENABLED, 1)) {
        sql->append(sql, SYM_SQL_ORDER_BY_DATA_ID);
    }

    SymStringArray *args = SymStringArray_new(NULL);
    args->add(args, this->context->channel->channelId);

    int error;
    SymList *dataList = sqlTemplate->queryWithUserData(sqlTemplate, sql->str, args, NULL, &error, (void *) &SymDataGapRouteReader_dataMapper, this);

    sql->destroy(sql);
    args->destroy(args);
    return dataList;
}

void SymDataGapRouteReader_destroy(SymDataGapRouteReader *this) {
    free(this);
}

SymDataGapRouteReader * SymDataGapRouteReader_new(SymDataGapRouteReader *this, SymDatabasePlatform *platform, SymParameterService *parameterService,
        SymDataService *dataService, SymChannelRouterContext *context) {
    if (this == NULL) {
        this = (SymDataGapRouteReader *) calloc(1, sizeof(SymDataGapRouteReader));
    }
    this->platform = platform;
    this->parameterService = parameterService;
    this->dataService = dataService;
    this->context = context;
    this->selectDataFor = (void *) &SymDataGapRouteReader_selectDataFor;
    this->destroy = (void *) &SymDataGapRouteReader_destroy;
    return this;
}
