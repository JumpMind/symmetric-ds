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

SymData * SymDataService_dataMapper(SymRow *row, SymDataService *this) {
    SymData *data = SymData_new(NULL);
    data->dataId = row->getLong(row, "data_id");
    data->rowData = row->getStringNew(row, "row_data");
    data->oldData = row->getStringNew(row, "old_data");
    data->pkData = row->getStringNew(row, "pk_data");
    data->channelId = row->getStringNew(row, "channel_id");
    data->transactionId = row->getStringNew(row, "transaction_id");
    data->tableName = row->getStringNew(row, "table_name");
    data->eventType = SymDataEvent_getEventType(row->getString(row, "event_type"));
    data->sourceNodeId = row->getStringNew(row, "source_node_id");
    data->externalData = row->getStringNew(row, "external_data");
    data->nodeList = row->getStringNew(row, "node_list");
    data->createTime = row->getDate(row, "create_time");
    data->routerId = row->getStringNew(row, "router_id");
    data->triggerHistId = row->getInt(row, "trigger_hist_id");

    SymTriggerHistory *triggerHistory = this->triggerRouterService->getTriggerHistory(this->triggerRouterService, data->triggerHistId);
    if (triggerHistory == NULL) {
        triggerHistory = SymTriggerHistory_newWithId(NULL, data->triggerHistId);
    } else {
        if (strcmp(triggerHistory->sourceTableName, data->tableName) != 0) {
            SymLog_warn("There was a mismatch between the data table name {} and the trigger_hist table name %s for data_id {}.  Attempting to look up a valid trigger_hist row by table name",
                    data->tableName, triggerHistory->sourceTableName, data->dataId);
            SymList *list = this->triggerRouterService->getActiveTriggerHistoriesByTableName(this->triggerRouterService, data->tableName);
            triggerHistory = list->get(list, 0);
            list->destroy(list);
        }
    }
    data->triggerHistory = triggerHistory;
    return data;
}

SymList * SymDataService_selectDataFor(SymDataService *this, SymBatch *batch) {
    SymStringBuilder *sb = SymStringBuilder_newWithString(SYM_SQL_SELECT_EVENT_DATA_TO_EXTRACT);
    sb->append(sb, " order by d.data_id asc");

    SymStringArray *args = SymStringArray_new(NULL);
    args->addLong(args, batch->batchId)->add(args, batch->targetNodeId);

    int error;
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    SymList *list = sqlTemplate->queryWithUserData(sqlTemplate, sb->str, args, NULL, &error, (void *) SymDataService_dataMapper, this);

    args->destroy(args);
    sb->destroy(sb);

    // TODO: return SymSqlReadCursor instead
    return list;
}

void SymDataService_heartbeat(SymDataService *this, unsigned short force) {
    SymNode *me = this->nodeService->findIdentity(this->nodeService);

    if (me != NULL) {
        if (this->parameterService->is(this->parameterService, SYM_PARAMETER_HEARTBEAT_ENABLED, 1)) {
            unsigned short updateWithBatchStatus = this->parameterService->is(this->parameterService,
                    SYM_PARAMETER_HEARTBEAT_UPDATE_NODE_WITH_BATCH_STATUS, 0);

            char *syncUrl = this->parameterService->getSyncUrl(this->parameterService);
            char *schemaVersion = this->parameterService->getString(this->parameterService, SYM_PARAMETER_SCHEMA_VERSION, NULL);

            int outgoingErrorCount = -1;
            int outgoingUnsentCount = -1;
            if (updateWithBatchStatus) {
                outgoingUnsentCount = this->outgoingBatchService->countOutgoingBatchesUnsent(this->outgoingBatchService);
                outgoingErrorCount = this->outgoingBatchService->countOutgoingBatchesInError(this->outgoingBatchService);
            }

            if (! SymStringUtils_equals(this->parameterService->getExternalId(this->parameterService), me->externalId)
                    || !SymStringUtils_equals(this->parameterService->getNodeGroupId(this->parameterService), me->nodeGroupId)
                    || (syncUrl != NULL && !SymStringUtils_equals(syncUrl, me->syncUrl))
                    || (schemaVersion != NULL && !SymStringUtils_equals(schemaVersion, me->schemaVersion))
                    || !SymStringUtils_equals(SYM_VERSION, me->symmetricVersion)
                    || !SymStringUtils_equals(this->platform->name, me->databaseType)
                    || !SymStringUtils_equals(this->platform->version, me->databaseVersion)
                    || !SymStringUtils_equals(SYM_DEPLOYMENT_TYPE, me->deploymentType)
                    || me->batchInErrorCount != outgoingErrorCount
                    || me->batchToSendCount != outgoingUnsentCount) {
                SymLog_info("Some attribute(s) of node changed.  Recording changes");

                me->deploymentType = SYM_DEPLOYMENT_TYPE;
                me->symmetricVersion = SYM_VERSION;
                me->databaseType = this->platform->name;
                me->databaseVersion = this->platform->version;
                me->batchInErrorCount = outgoingErrorCount;
                me->batchToSendCount = outgoingUnsentCount;
                me->schemaVersion = schemaVersion;
                if (this->parameterService->is(this->parameterService, SYM_PARAMETER_AUTO_UPDATE_NODE_VALUES, 0)) {
                    SymLog_info("Updating my node configuration info according to the symmetric properties");
                    me->externalId = this->parameterService->getExternalId(this->parameterService);
                    me->nodeGroupId = this->parameterService->getNodeGroupId(this->parameterService);
                    if (SymStringUtils_isNotBlank(this->parameterService->getSyncUrl(this->parameterService))) {
                        me->syncUrl = this->parameterService->getSyncUrl(this->parameterService);
                    }
                }

                this->nodeService->save(this->nodeService, me);
            }
        }
        SymLog_debug("Updating my node info");
        this->nodeService->updateNodeHostForCurrentNode(this->nodeService);
        SymLog_debug("Done updating my node info");
    }
}

void SymDataService_insertDataEvents(SymDataService *this, SymSqlTransaction *transaction, SymList *events) {
    if (events->size > 0) {
        int error = 0;
        transaction->prepare(transaction, SYM_SQL_INSERT_INTO_DATA_EVENT, &error);
        if (!error) {
            SymIterator *iter = events->iterator(events);
            while (iter->hasNext(iter)) {
                SymDataEvent *dataEvent = (SymDataEvent *) iter->next(iter);
                char *routerId = dataEvent->routerId;
                if (SymStringUtils_isBlank(routerId)) {
                    routerId = SYM_UNKNOWN_ROUTER_ID;
                }
                SymStringArray *args = SymStringArray_new(NULL);
                args->addLong(args, dataEvent->dataId);
                args->addLong(args, dataEvent->batchId);
                args->add(args, dataEvent->routerId);
                int error;
                transaction->addRow(transaction, args, NULL, &error);
                args->destroy(args);
            }
            iter->destroy(iter);
        }
    }
}

SymData * SymDataService_mapData(SymDataService *this, SymRow *row) {
    return SymDataService_dataMapper(row, this);
}

void SymDataService_destroy(SymDataService *this) {
    free(this);
}

SymDataService * SymDataService_new(SymDataService *this, SymDatabasePlatform *platform, SymTriggerRouterService *triggerRouterService,
        SymNodeService *nodeService, SymDialect *dialect, SymOutgoingBatchService *outgoingBatchService,
        SymParameterService *parameterService) {
    if (this == NULL) {
        this = (SymDataService *) calloc(1, sizeof(SymDataService));
    }
    this->platform = platform;
    this->triggerRouterService = triggerRouterService;
    this->nodeService = nodeService;
    this->dialect = dialect;
    this->outgoingBatchService = outgoingBatchService;
    this->parameterService = parameterService;
    this->heartbeat = (void *) &SymDataService_heartbeat;
    this->selectDataFor = (void *) &SymDataService_selectDataFor;
    this->insertDataEvents = (void *) &SymDataService_insertDataEvents;
    this->mapData = (void *) &SymDataService_mapData;
    this->destroy = (void *) &SymDataService_destroy;
    return this;
}
