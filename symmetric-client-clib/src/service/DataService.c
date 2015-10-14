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
#include "service/DataService.h"

SymData * SymDataService_dataMapper(SymRow *row) {
    SymData *data = SymData_new(NULL);
    data->dataId = row->getLong(row, "data_id");
    data->rowData = row->getString(row, "row_data");
    data->oldData = row->getString(row, "old_data");
    data->pkData = row->getString(row, "pk_data");
    data->channelId = row->getString(row, "channel_id");
    data->transactionId = row->getString(row, "transaction_id");
    data->tableName = row->getString(row, "table_name");
    data->eventType = row->getString(row, "event_type");
    data->sourceNodeId = row->getString(row, "source_node_id");
    data->externalData = row->getString(row, "external_data");
    data->nodeList = row->getString(row, "node_list");
    data->createTime = row->getDate(row, "create_time");
    data->routerId = row->getString(row, "router_id");
    data->triggerHistId = row->getInt(row, "trigger_hist_id");

    // TODO: add triggerHistory
    /*
    SymTriggerHistory *triggerHistory = this->triggerRouterService->getTriggerHistory(this->triggerRouterService, triggerHistId);
    if (triggerHistory == NULL) {
        triggerHistory = SymTriggerHistory_newWithId(triggerHistId);
    } else {
        if (strcmp(triggerHistory->sourceTableName, data->tableName) != 0) {
            SymLog_warn("There was a mismatch between the data table name {} and the trigger_hist table name %s for data_id {}.  Attempting to look up a valid trigger_hist row by table name",
                    data->tableName, triggerHistory->sourceTableName, data->dataId);
            SymList *list = this->triggerRouterService->getActiveTriggerHistories(this->triggerRouterService, data->tableName);
            triggerHistory = list->get(list, 0);
        }
    }
    data->triggerHistory = triggerHistory;
    */
    return data;
}
SymList * SymDataService_selectDataFor(SymDataService *this, SymBatch *batch) {
    SymStringBuilder *sb = SymStringBuilder_new(SYM_SQL_SELECT_EVENT_DATA_TO_EXTRACT);
    sb->append(sb, " order by d.data_id asc");

    SymStringArray *args = SymStringArray_new(NULL);
    args->addLong(args, batch->batchId)->add(args, batch->targetNodeId);

    int error;
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    SymList *list = sqlTemplate->query(sqlTemplate, sb->str, args, NULL, &error, (void *) SymDataService_dataMapper);

    args->destroy(args);
    sb->destroy(sb);

    // TODO: return SymSqlReadCursor instead
    return list;
}

void SymDataService_destroy(SymDataService *this) {
    free(this);
}

SymDataService * SymDataService_new(SymDataService *this, SymDatabasePlatform *platform, SymTriggerRouterService *triggerRouterService) {
    if (this == NULL) {
        this = (SymDataService *) calloc(1, sizeof(SymDataService));
    }
    this->platform = platform;
    this->triggerRouterService = triggerRouterService;
    this->selectDataFor = (void *) &SymDataService_selectDataFor;
    this->destroy = (void *) &SymDataService_destroy;
    return this;
}
