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
    data->dataId = row->getLong(row, "DATA_ID");
    data->rowData = row->getString(row, "ROW_DATA");
    data->oldData = row->getString(row, "OLD_DATA");
    data->pkData = row->getString(row, "PK_DATA");
    data->channelId = row->getString(row, "CHANNEL_ID");
    data->transactionId = row->getString(row, "TRANSACTION_ID");
    data->tableName = row->getString(row, "TABLE_NAME");
    data->eventType = row->getString(row, "EVENT_TYPE");
    data->sourceNodeId = row->getString(row, "SOURCE_NODE_ID");
    data->externalData = row->getString(row, "EXTERNAL_DATA");
    data->nodeList = row->getString(row, "NODE_LIST");
    data->createTime = row->getDate(row, "CREATE_TIME");
    data->routerId = row->getString(row, "ROUTER_ID");
    data->triggerHistId = row->getInt(row, "TRIGGER_HIST_ID");

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
SymData * SymDataService_selectDataFor(SymDataService *this, SymBatch *batch) {
    SymStringBuilder *sb = SymStringBuilder_new(SYM_SQL_SELECT_EVENT_DATA_TO_EXTRACT);
    sb->append(sb, " order by d.data_id asc");
    SymStringArray *args = SymStringArray_new(NULL);
    args->addLong(args, batch->batchId)->add(args, batch->targetNodeId);


    sb->destroy(sb);
    return NULL;
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
