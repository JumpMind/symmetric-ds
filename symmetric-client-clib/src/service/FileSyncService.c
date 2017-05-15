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
#include "service/FileSyncService.h"
#include "core/SymEngine.h"

static SymFileSnapshot * SymFileSyncService_mapFileSnapshot(SymRow *row) {
    SymFileSnapshot *fileSnapshot = SymFileSnapshot_new(NULL);
    fileSnapshot->crc32Checksum = row->getLong(row, "crc32_checksum");
    fileSnapshot->createTime = row->getDate(row, "create_time");
    fileSnapshot->channelId = row->getStringNew(row, "channel_id");
    fileSnapshot->reloadChannelId = row->getStringNew(row, "reload_channel_id");
    fileSnapshot->lastUpdateBy = row->getStringNew(row, "last_update_by");
    fileSnapshot->lastUpdateTime = row->getDate(row, "last_update_time");
    fileSnapshot->fileModifiedTime = row->getLong(row, "file_modified_time");
    fileSnapshot->fileName = row->getStringNew(row, "file_name");
    fileSnapshot->relativeDir = row->getStringNew(row, "relative_dir");
    fileSnapshot->fileSize = row->getLong(row, "file_size");
    fileSnapshot->lastEventType = row->getStringNew(row, "last_event_type");
    fileSnapshot->triggerId = row->getStringNew(row, "trigger_id");
    fileSnapshot->routerId = row->getStringNew(row, "router_id");
    return fileSnapshot;
}

static SymFileTriggerRouter * SymFileSyncService_mapFileTriggerRouter(SymRow *row) {
    SymFileTriggerRouter *fileTriggerRouter = SymFileTriggerRouter_new(NULL);
    fileTriggerRouter->triggerId = row->getStringNew(row, "trigger_id");
    fileTriggerRouter->conflictStrategy = row->getStringNew(row, "conflict_stategy");
    fileTriggerRouter->createTime = row->getDate(row, "create_time");
    fileTriggerRouter->lastUpdateBy = row->getStringNew(row, "last_update_by");
    fileTriggerRouter->lastUpdateTime = row->getDate(row, "last_update_time");
    fileTriggerRouter->enabled = row->getBoolean(row, "enabled");
    fileTriggerRouter->initialLoadEnabled = row->getBoolean(row, "initial_load_enabled");
    fileTriggerRouter->targetBaseDir = row->getStringNew(row, "target_base_dir");
    fileTriggerRouter->routerId = row->getStringNew(row, "router_id");
    return fileTriggerRouter;
}

static SymFileTrigger * SymFileSyncService_mapFileTrigger(SymRow *row) {
    SymFileTrigger *fileTrigger = SymFileTrigger_new(NULL);
    fileTrigger->baseDir = row->getStringNew(row, "base_dir");
    fileTrigger->createTime = row->getDate(row, "create_time");
    fileTrigger->excludesFiles = row->getStringNew(row, "excludes_files");
    fileTrigger->includesFiles = row->getStringNew(row, "includes_files");
    fileTrigger->lastUpdateBy = row->getStringNew(row, "last_update_by");
    fileTrigger->lastUpdateTime = row->getDate(row, "last_update_time");
    fileTrigger->recurse = row->getBoolean(row, "recurse");
    fileTrigger->syncOnCreate = row->getBoolean(row, "sync_on_create");
    fileTrigger->syncOnDelete = row->getBoolean(row, "sync_on_delete");
    fileTrigger->afterCopyScript = row->getStringNew(row, "after_copy_script");
    fileTrigger->beforeCopyScript = row->getStringNew(row, "before_copy_script");
    fileTrigger->syncOnModified = row->getBoolean(row, "sync_on_modified");
    fileTrigger->syncOnCtlFile = row->getBoolean(row, "sync_on_ctl_file");
    fileTrigger->deleteAfterSync = row->getBoolean(row, "delete_after_sync");
    fileTrigger->triggerId = row->getStringNew(row, "trigger_id");
    fileTrigger->channelId = row->getStringNew(row, "channel_id");
    fileTrigger->reloadChannelId = row->getStringNew(row, "reload_channel_id");
    return fileTrigger;

}

void SymFileSyncService_trackChangesImpl(SymFileSyncService *this, unsigned short useCrc) {
    SymList *fileTriggerRouters = this->getFileTriggerRoutersForCurrentNode(this);
    int i;
    for (i = 0; i < fileTriggerRouters->size; ++i) {
        SymFileTriggerRouter *fileTriggerRouter = fileTriggerRouters->get(fileTriggerRouters, i);
        if (fileTriggerRouter->enabled) {
            SymDirectorySnapshot *lastSnapshot = this->getDirectorySnapshot(this, fileTriggerRouter);

            SymFileTriggerTracker *tracker = SymFileTriggerTracker_new(NULL);
            tracker->lastSnapshot = lastSnapshot;
            tracker->fileTriggerRouter = fileTriggerRouter;
            tracker->engine = this->engine;
            tracker->useCrc = useCrc;
            SymDirectorySnapshot *dirSnapshot = tracker->trackChanges(tracker);
            this->saveDirectorySnapshot(this, fileTriggerRouter, dirSnapshot);
            tracker->destroy(tracker);
        }
    }
    fileTriggerRouters->destroy(fileTriggerRouters);
}

long SymFileSyncService_saveDirectorySnapshot(SymFileSyncService *this, SymFileTriggerRouter *fileTriggerRouter, SymDirectorySnapshot *dirSnapshot) {
    long totalBytes = 0;
    int i;
    for (i = 0; i < dirSnapshot->fileSnapshots->size; ++i) {
        SymFileSnapshot *fileSnapshot = dirSnapshot->fileSnapshots->get(dirSnapshot->fileSnapshots, i);
        char *filePath = fileSnapshot->relativeDir;
        char *fileName = fileSnapshot->fileName;
        char *nodeId = NULL;
        if (this->engine->parameterService->is(this->engine->parameterService, SYM_PARAMETER_FILE_SYNC_PREVENT_PING_BACK, 1)) {
            nodeId = this->findSourceNodeIdFromFileIncoming(this, filePath, fileName, fileSnapshot->fileModifiedTime);
        }
        if (SymStringUtils_isNotBlank(nodeId)) {
            fileSnapshot->lastUpdateBy = nodeId;
        } else {
            fileSnapshot->lastUpdateBy = NULL;
        }

        SymLog_debug("Captured change %s/%s", fileSnapshot->relativeDir, fileSnapshot->fileName);
        totalBytes += fileSnapshot->fileSize;
    }
    this->save(this, dirSnapshot);
    return totalBytes;
}

void SymFileSyncService_save(SymFileSyncService *this, SymDirectorySnapshot *dirSnapshot) {
    if (dirSnapshot != NULL && dirSnapshot->fileSnapshots != NULL) {
        SymSqlTemplate *sqlTemplate = this->engine->platform->getSqlTemplate(this->engine->platform);
        SymSqlTransaction *transaction = sqlTemplate->startSqlTransaction(sqlTemplate);
        int i;
        int error = 0;

        for (i = 0; i < dirSnapshot->fileSnapshots->size; ++i) {
            SymFileSnapshot *fileSnapshot = dirSnapshot->fileSnapshots->get(dirSnapshot->fileSnapshots, i);
            error = this->saveSnapshot(this, transaction, fileSnapshot);
            if (error != 0) {
                break;
            }
        }
        if (error == 0) {
            transaction->commit(transaction);
        } else {
            transaction->rollback(transaction);
        }
        transaction->close(transaction);
    }
}

int SymFileSyncService_saveSnapshot(SymFileSyncService *this, SymSqlTransaction *transaction, SymFileSnapshot *snapshot) {
    snapshot->lastUpdateTime = SymDate_new();

    char *crc32Checksum = SymStringUtils_format("%d", snapshot->crc32Checksum);
    char *fileSize = SymStringUtils_format("%d", snapshot->fileSize);
    char *fileModifiedTime = SymStringUtils_format("%d", snapshot->fileModifiedTime);
    char *lastUpdateTime = SymStringUtils_format("%d", snapshot->lastUpdateTime);

    SymStringArray *updateArgs = SymStringArray_new(NULL);
    updateArgs->add(updateArgs, snapshot->lastEventType);
    updateArgs->add(updateArgs, crc32Checksum);
    updateArgs->add(updateArgs, fileSize);
    updateArgs->add(updateArgs, fileModifiedTime);
    updateArgs->add(updateArgs, lastUpdateTime);
    updateArgs->add(updateArgs, snapshot->lastUpdateBy);
    updateArgs->add(updateArgs, snapshot->channelId);
    updateArgs->add(updateArgs, snapshot->reloadChannelId);
    updateArgs->add(updateArgs, snapshot->triggerId);
    updateArgs->add(updateArgs, snapshot->routerId);
    updateArgs->add(updateArgs, snapshot->relativeDir);
    updateArgs->add(updateArgs, snapshot->fileName);
    int error;
    int updateCount = transaction->update(transaction, SYM_SQL_UPDATE_FILE_SNAPSHOT, updateArgs, NULL, &error);

    if (updateCount == 0 && error == 0) {
        snapshot->createTime = SymDate_newWithTime(snapshot->lastUpdateTime->time);
        char *createTime = SymStringUtils_format("%d", snapshot->createTime);
        SymStringArray *insertArgs = SymStringArray_new(NULL);
        insertArgs->add(insertArgs, snapshot->lastEventType);
        insertArgs->add(insertArgs, crc32Checksum);
        insertArgs->add(insertArgs, fileSize);
        insertArgs->add(insertArgs, fileModifiedTime);
        insertArgs->add(insertArgs, createTime);
        insertArgs->add(insertArgs, lastUpdateTime);
        insertArgs->add(insertArgs, snapshot->lastUpdateBy);
        insertArgs->add(insertArgs, snapshot->channelId);
        insertArgs->add(insertArgs, snapshot->reloadChannelId);
        insertArgs->add(insertArgs, snapshot->triggerId);
        insertArgs->add(insertArgs, snapshot->routerId);
        insertArgs->add(insertArgs, snapshot->relativeDir);
        insertArgs->add(insertArgs, snapshot->fileName);

        transaction->update(transaction, SYM_SQL_INSERT_FILE_SNAPSHOT, insertArgs, NULL, &error);
        insertArgs->destroy(insertArgs);
    }

    updateArgs->destroy(updateArgs);

    // now that we have captured an update, delete the row for cleanup
    if (SymStringUtils_equals(snapshot->lastEventType, "D") && error == 0) {
        SymStringArray *deleteArgs = SymStringArray_new(NULL);
        deleteArgs->add(deleteArgs, snapshot->triggerId);
        deleteArgs->add(deleteArgs, snapshot->routerId);
        deleteArgs->add(deleteArgs, snapshot->relativeDir);
        deleteArgs->add(deleteArgs, snapshot->fileName);

        transaction->update(transaction, SYM_SQL_DELETE_FILE_SNAPSHOT, deleteArgs, NULL, &error);
        deleteArgs->destroy(deleteArgs);
    }

    free(crc32Checksum);
    free(fileSize);
    free(fileModifiedTime);
    free(lastUpdateTime);

    return error;
}

char * SymFileSyncService_findSourceNodeIdFromFileIncoming(SymFileSyncService *this, char *filePath, char *fileName, long lastUpdateDate) {
    char *lastUpdateDateString = SymStringUtils_format("%d", lastUpdateDate);

    SymStringArray *args = SymStringArray_new(NULL);
    args->add(args, filePath);
    args->add(args, fileName);
    args->add(args, lastUpdateDateString);

    int error;
    SymSqlTemplate *sqlTemplate = this->engine->platform->getSqlTemplate(this->engine->platform);

    char *nodeId = sqlTemplate->queryForString(sqlTemplate, SYM_SQL_FIND_NODE_ID_FROM_FILE_INCOMING, args,
            NULL, &error);
    free(lastUpdateDateString);
    args->destroy(args);
    return nodeId;
}

SymFileTrigger * SymFileSyncService_getFileTrigger(SymFileSyncService *this, char* triggerId) {
    SymStringArray *args = SymStringArray_new(NULL);
    args->add(args, triggerId);

    int error;
    SymSqlTemplate *sqlTemplate = this->engine->platform->getSqlTemplate(this->engine->platform);
    char *sql = SymStringUtils_format("%s%s", SYM_SQL_SELECT_FILE_TRIGGERS, SYM_SQL_TRIGGER_ID_WHERE);

    SymList *list = sqlTemplate->query(sqlTemplate, sql, args,
            NULL, &error, (void *) SymFileSyncService_mapFileTrigger);
    SymIterator *iter = list->iterator(list);
    SymFileTrigger *fileTrigger = NULL;
    if (iter->hasNext(iter)) {
        fileTrigger = (SymFileTrigger *) iter->next(iter);
    }
    free(sql);
    iter->destroy(iter);
    list->destroy(list);
    args->destroy(args);
    return fileTrigger;
}

void SymFileSyncService_trackChanges(SymFileSyncService *this) {
    SymLog_debug("Tracking changes for file sync");
    // SymNode *local = this->engine->nodeService->findIdentity(this->engine->nodeService);
    unsigned short useCrc = this->engine->parameterService->is(
            this->engine->parameterService, SYM_PARAMETER_FILE_SYNC_USE_CRC, 1);
    SymFileSyncService_trackChangesImpl(this, useCrc);
    SymLog_debug("Done tracking changes for file sync");
}

SymDirectorySnapshot * SymFileSyncService_getDirectorySnapshot(SymFileSyncService *this, SymFileTriggerRouter *fileTriggerRouter) {
    SymStringArray *args = SymStringArray_new(NULL);
    args->add(args, fileTriggerRouter->triggerId);
    args->add(args, fileTriggerRouter->routerId);

    int error;
    SymSqlTemplate *sqlTemplate = this->engine->platform->getSqlTemplate(this->engine->platform);
    SymList *list = sqlTemplate->query(sqlTemplate, SYM_SQL_SELECT_FILE_SNAPSHOT, args,
            NULL, &error, (void *) SymFileSyncService_mapFileSnapshot);
    SymIterator *iter = list->iterator(list);

    while (iter->hasNext(iter)) {
        SymFileTriggerRouter *fileTriggerRouter = (SymFileTriggerRouter *) iter->next(iter);
        fileTriggerRouter->fileTrigger = this->getFileTrigger(this, fileTriggerRouter->triggerId);
        fileTriggerRouter->router = this->engine->triggerRouterService->getRouterById(
                this->engine->triggerRouterService, fileTriggerRouter->routerId, 0);
    }

    SymDirectorySnapshot *directorySnapshot = SymDirectorySnapshot_new(NULL);
    directorySnapshot->fileSnapshots = list;
    directorySnapshot->fileTriggerRouter = fileTriggerRouter;

    iter->destroy(iter);
    args->destroy(args);

    return directorySnapshot;
}

SymList * SymFileSyncService_getFileTriggerRoutersForCurrentNode(SymFileSyncService *this) {
    SymStringArray *args = SymStringArray_new(NULL);
    args->add(args, this->engine->parameterService->getNodeGroupId(this->engine->parameterService));

    int error;
    SymSqlTemplate *sqlTemplate = this->engine->platform->getSqlTemplate(this->engine->platform);
    char *sql = SymStringUtils_format("%s%s", SYM_SQL_FILE_TRIGGER_ROUTERS, SYM_SQL_FILE_TRIGGER_ROUTERS_FOR_CURRENT_NODE_WHERE);
    SymList *list = sqlTemplate->query(sqlTemplate, sql, args,
            NULL, &error, (void *) SymFileSyncService_mapFileTriggerRouter);
    SymIterator *iter = list->iterator(list);

    while (iter->hasNext(iter)) {
        SymFileTriggerRouter *fileTriggerRouter = (SymFileTriggerRouter *) iter->next(iter);
        fileTriggerRouter->fileTrigger = this->getFileTrigger(this, fileTriggerRouter->triggerId);
        fileTriggerRouter->router = this->engine->triggerRouterService->getRouterById(
                this->engine->triggerRouterService, fileTriggerRouter->routerId, 0);
    }
    free(sql);
    iter->destroy(iter);
    args->destroy(args);
    return list;
}

SymRemoteNodeStatuses* SymFileSyncService_pushFilesToNodes(SymFileSyncService *this) {
    SymList*/*<SymChannel>*/ fileSyncChannels = this->engine->configurationService->getFileSyncChannels(this->engine->configurationService, 0);
    SymMap*/*<String,SymChannel>*/ fileSyncChannelsMap = SymMap_new(NULL, 4);
    int i;
    for (i = 0; i < fileSyncChannels->size; ++i) {
        SymChannel *channel = fileSyncChannels->get(fileSyncChannels, i);
        fileSyncChannelsMap->put(fileSyncChannelsMap, channel->channelId, channel);
    }

    SymRemoteNodeStatuses *remoteNodeStatuses = SymRemoteNodeStatuses_new(NULL, fileSyncChannelsMap);
    int communicationType = SYM_COMMUNICATION_TYPE_FILE_PUSH;
    if (this->engine->parameterService->is(this->engine->parameterService, SYM_PARAMETER_NODE_OFFLINE, 0)) {
        communicationType = SYM_COMMUNICATION_TYPE_OFF_FSPUSH;
    }
    SymList/*<SymNode>*/ *nodes = this->engine->nodeCommunicationService->list(this->engine->nodeCommunicationService, communicationType);

    for (i = 0; i < nodes->size; ++i) {
        SymNode *node = nodes->get(nodes, i);
        SymRemoteNodeStatus *remoteNodeStatus = remoteNodeStatuses->add(remoteNodeStatuses, node->nodeId);
        this->pushFilesToNode(this, node, remoteNodeStatus);
    }

    return remoteNodeStatuses;
}

void SymFileSyncService_pushFilesToNode(SymFileSyncService *this, SymNode *remote, SymRemoteNodeStatus *status) {
    SymNode *identity = this->engine->nodeService->findIdentityWithCache(this->engine->nodeService, 0);
    SymNodeSecurity *identitySecurity = this->engine->nodeService->findNodeSecurity(this->engine->nodeService, identity->nodeId);

    SymOutgoingTransport *transport = NULL;
    if (this->engine->parameterService->is(this->engine->parameterService, SYM_PARAMETER_NODE_OFFLINE, 0)) {
        transport = this->engine->offlineTransportManager->getFileSyncPushTransport(this->engine->offlineTransportManager,
                remote, identity, identitySecurity->nodePassword,
                this->engine->parameterService->getRegistrationUrl(this->engine->parameterService));
    } else {
        transport = this->engine->transportManager->getFileSyncPushTransport(this->engine->transportManager,
                remote, identity, identitySecurity->nodePassword,
                this->engine->parameterService->getRegistrationUrl(this->engine->parameterService));
    }

    SymList* batches = this->sendFiles(this, identity, remote, transport);

    if (batches->size > 0) {
        SymList*/*<BatchAck>*/ batchAcks =
                this->engine->pushService->readAcks(batches, transport, this->engine->transportManager, this->engine->acknowledgeService);

        status->updateOutgoingStatus(status, batches, batchAcks);
        batchAcks->destroy(batchAcks);
    }
    if (!status->failed && batches->size > 0) {
        SymLog_info("Pushed files to %s. %d files and %d batches were processed", remote->nodeId, status->dataProcessed, status->batchesProcessed);
    } else if (status->failed) {
        SymLog_info("There was a failure while pushing files to %s. %d files and %d batches were processed", remote->nodeId, status->dataProcessed, status->batchesProcessed);
    }

    transport->destroy(transport);
}

SymList*/*<OutgoingBatch>*/ SymFileSyncService_sendFiles(SymFileSyncService *this, SymNode *sourceNode, SymNode *targetNode, SymOutgoingTransport *transport) {
    SymList*/*<OutgoingBatch>*/ batchesToProcess = this->getBatchesToProcess(this, targetNode);

    if (batchesToProcess->size == 0) {
        return batchesToProcess;
    }

    SymFileUtils_deleteDir("./tmp/staging/filesync_outgoing");

    SymList * /*<SymOutgoingBatch>*/ processedBatches = SymList_new(NULL);

    SymOutgoingBatch *currentBatch = NULL;
    SymFileSyncZipDataWriter *dataWriter = NULL;

    int i;
    for (i = 0; i < batchesToProcess->size; ++i) {
        currentBatch = batchesToProcess->get(batchesToProcess, i);
        if (dataWriter == NULL) {
            dataWriter = SymFileSyncZipDataWriter_new(NULL, sourceNode->nodeId);
            dataWriter->nodeService = this->engine->nodeService;
            dataWriter->fileSyncService = this;
            dataWriter->super.open((SymDataWriter*)dataWriter);
        }
        this->engine->dataExtractorService->extractOutgoingBatch(this->engine->dataExtractorService,
                targetNode, (SymDataWriter*)dataWriter, currentBatch);
        processedBatches->add(processedBatches, currentBatch);
    }

    if (dataWriter != NULL) {
        dataWriter->super.close((SymDataWriter*)dataWriter);
    }

    if (processedBatches->size > 0) {
        long result = transport->process(transport, NULL, NULL);
        if (result == SYM_OFFLINE_TRANSPORT_OK) {

            SymList *batchIds = SymList_new(NULL);

            SymStringBuilder *buff = SymStringBuilder_new(NULL);
            int i;
            for (i = 0; i < processedBatches->size; ++i) {
                SymOutgoingBatch *batch = processedBatches->get(processedBatches, i);
                buff->append(buff, SYM_WEB_CONSTANTS_ACK_BATCH_NAME);
                buff->append(buff, SymStringUtils_format("%ld", batch->batchId));
                buff->append(buff, "=");
                buff->append(buff, SYM_WEB_CONSTANTS_ACK_BATCH_OK);
                buff->append(buff, "&");
            }

            transport->ackString = buff->destroyAndReturn(buff);
        }
    }

    return processedBatches;
}

SymList*/*<OutgoingBatch>*/ SymFileSyncService_getBatchesToProcess(SymFileSyncService *this, SymNode *targetNode) {
    SymList*/*<SymOutgoingBatch>*/ batchesToProcess = SymList_new(NULL);
    SymList*/*<SymChannel>*/ fileSyncChannels = this->engine->configurationService->getFileSyncChannels(this->engine->configurationService, 0);
    SymOutgoingBatches *batches = this->engine->outgoingBatchService->getOutgoingBatches(this->engine->outgoingBatchService,
            targetNode->nodeId);

    int i;
    for (i = 0; i < fileSyncChannels->size; ++i) {
        SymChannel *channel = fileSyncChannels->get(fileSyncChannels, i);
        SymList * /*<SymChannel>*/ batchesForChannel = batches->filterBatchesForChannel(batches, channel->channelId);
        batchesToProcess->addAll(batchesToProcess, batchesForChannel);
        batchesForChannel->destroy(batchesForChannel);
    }

    batches->destroy(batches);
    return batchesToProcess;
}

SymFileTriggerRouter * SymFileSyncService_getFileTriggerRouter(SymFileSyncService *this, char* triggerId, char* routerId) {
    SymStringArray *args = SymStringArray_new(NULL);
    args->add(args, triggerId);
    args->add(args, routerId);

    int error;
    SymSqlTemplate *sqlTemplate = this->engine->platform->getSqlTemplate(this->engine->platform);
    char *sql = SymStringUtils_format("%s%s", SYM_SQL_SELECT_FILE_TRIGGER_ROUTERS, SYM_SQL_WHERE_TRIGGER_ROUTER_ID);

    SymList *list = sqlTemplate->query(sqlTemplate, sql, args,
            NULL, &error, (void *) SymFileSyncService_mapFileTriggerRouter);
    SymIterator *iter = list->iterator(list);
    SymFileTriggerRouter *fileTriggerRouter = NULL;
    if (iter->hasNext(iter)) {
        fileTriggerRouter = (SymFileTriggerRouter *) iter->next(iter);
        fileTriggerRouter->fileTrigger = this->getFileTrigger(this, fileTriggerRouter->triggerId);
        fileTriggerRouter->router = this->engine->triggerRouterService->getRouterById(
                this->engine->triggerRouterService, fileTriggerRouter->routerId, 0);
    }
    free(sql);
    iter->destroy(iter);
    list->destroy(list);
    args->destroy(args);
    return fileTriggerRouter;
}

SymRemoteNodeStatuses* SymFileSyncService_pullFilesFromNodes(SymFileSyncService *this) {
    SymList*/*<SymChannel>*/ fileSyncChannels = this->engine->configurationService->getFileSyncChannels(this->engine->configurationService, 0);
    SymMap*/*<String,SymChannel>*/ fileSyncChannelsMap = SymMap_new(NULL, 4);
    int i;
    for (i = 0; i < fileSyncChannels->size; ++i) {
        SymChannel *channel = fileSyncChannels->get(fileSyncChannels, i);
        fileSyncChannelsMap->put(fileSyncChannelsMap, channel->channelId, channel);
    }

    SymRemoteNodeStatuses *remoteNodeStatuses = SymRemoteNodeStatuses_new(NULL, fileSyncChannelsMap);
    int communicationType = SYM_COMMUNICATION_TYPE_FILE_PULL;
    if (this->engine->parameterService->is(this->engine->parameterService, SYM_PARAMETER_NODE_OFFLINE, 0)) {
        communicationType = SYM_COMMUNICATION_TYPE_OFF_FSPULL;
    }
    SymList/*<SymNode>*/ *nodes = this->engine->nodeCommunicationService->list(this->engine->nodeCommunicationService, communicationType);

    for (i = 0; i < nodes->size; ++i) {
        SymNode *node = nodes->get(nodes, i);
        SymRemoteNodeStatus *remoteNodeStatus = remoteNodeStatuses->add(remoteNodeStatuses, node->nodeId);
        this->pullFilesFromNode(this, node, remoteNodeStatus);
    }

    return remoteNodeStatuses;
}

void SymFileSyncService_pullFilesFromNode(SymFileSyncService *this, SymNode *remote, SymRemoteNodeStatus *status) {
    SymNode *identity = this->engine->nodeService->findIdentityWithCache(this->engine->nodeService, 0);
    SymNodeSecurity *identitySecurity = this->engine->nodeService->findNodeSecurity(this->engine->nodeService, identity->nodeId);

    SymIncomingTransport *transport = NULL;

    unsigned short offlineFlag =
            this->engine->parameterService->is(this->engine->parameterService, SYM_PARAMETER_NODE_OFFLINE, 0);

    if (offlineFlag) {
        transport = this->engine->offlineTransportManager->getFileSyncPullTransport(this->engine->offlineTransportManager,
                remote, identity, identitySecurity->nodePassword,
                this->engine->parameterService->getRegistrationUrl(this->engine->parameterService));
    } else {
        transport = this->engine->transportManager->getFileSyncPullTransport(this->engine->transportManager,
                remote, identity, identitySecurity->nodePassword,
                this->engine->parameterService->getRegistrationUrl(this->engine->parameterService));
    }

    int rc = transport->process(transport, NULL, status);
    if (!status->failed && (rc == SYM_TRANSPORT_OK || rc == SYM_OFFLINE_TRANSPORT_OK)) {
        SymList * /*<IncomingBatch>*/ batchesProcessed = this->processZip(this, transport, remote->nodeId, identity);

        if (batchesProcessed->size > 0) {
            this->cleanExtractDir(this);

            status->updateIncomingStatus(status, batchesProcessed);
            if (!offlineFlag) {
                this->engine->dataLoaderService->sendAck(this->engine->dataLoaderService,
                        remote, identity, identitySecurity, batchesProcessed);
            }
        }

        if (!status->failed && batchesProcessed->size > 0) {
            SymLog_info("Pull files received from %s.  %ld files and %ld batches were processed",
                    remote->nodeId, status->dataProcessed, status->batchesProcessed );
        }
    }

    if (status->failed) {
        SymLog_info("There was a failure while pulling files from %s.  %ld files and %ld batches were processed. %s",
                remote->nodeId, status->dataProcessed, status->batchesProcessed, status->failureMessage );
    }

    transport->destroy(transport);
}

SymList * /*<IncomingBatch>*/ SymFileSyncService_processZip(SymFileSyncService *this, SymIncomingTransport *transport, char* sourceNodeId, SymNode* identity) {
    // At this point there should be a zip file already downloaded.
    char * zipFile = "tmp/staging/filesync_incoming/filesync.zip";

    time_t now;
    struct tm *tm;
    now = time(0);
    tm = localtime(&now);
    char *timestamp = SymStringUtils_format("%04d-%02d-%02dT%02d-%02d-%02d",
        tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday,
        tm->tm_hour, tm->tm_min, tm->tm_sec);

    char * unzipDir = SymStringUtils_format("tmp/staging/filesync_incoming/extract/%s", timestamp);

    int rc = SymFileUtils_unzip(zipFile, unzipDir);
    if (rc) {
        SymLog_error("Failed to unzip %s to %s", zipFile, unzipDir);
    }

    SymList * /*<char*>*/ batchIds = SymList_new(NULL);
    SymList * /*<SymFileEntry>*/ files = SymFileUtils_listFiles(unzipDir);

    int i;
    if (files != NULL) {
        for (i = 0; i < files->size; i++) {
            SymFileEntry *fileEntry = files->get(files, i);
            if (fileEntry->fileType == DT_DIR && SymStringUtils_isNumeric(fileEntry->fileName)) {
                batchIds->add(batchIds, fileEntry->fileName);
            }
        }
    }

    SymList * /*<IncomingBatch>*/ batchesProcessed = SymList_new(NULL);

    SymIncomingBatchService *incomingBatchService = this->engine->incomingBatchService;

    for (i = 0; i < batchIds->size; ++i) {
        char* batchId = batchIds->get(batchIds, i);
        char* batchDir = SymStringUtils_format("%s/%s", unzipDir, batchId);

        SymIncomingBatch *incomingBatch = SymIncomingBatch_new(NULL);
        char* batchInfo = SymStringUtils_format("%s/%s", batchDir, "batch-info.txt");

        if (SymFileUtils_exists(batchInfo)) {
            SymStringArray *info = SymFileUtils_readLines(batchInfo);
            if (info != NULL && info->size > 0) {
                incomingBatch->channelId = info->get(info, 0);
            } else {
                incomingBatch->channelId = SYM_CHANNEL_FILESYNC;
            }
        } else {
            incomingBatch->channelId = SYM_CHANNEL_FILESYNC;
        }

        incomingBatch->batchId = atol(batchId);
        incomingBatch->status = "LD";
        incomingBatch->nodeId = sourceNodeId;

        incomingBatch->byteCount = SymFileUtils_sizeOfDirectory(batchDir);

        batchesProcessed->add(batchesProcessed, incomingBatch);
        if (incomingBatchService->acquireIncomingBatch(incomingBatchService, incomingBatch)) {
            char* syncScript = SymStringUtils_format("%s/%s", batchDir, "sync.sh");
            unsigned short syncScriptExists = SymFileUtils_exists(syncScript);

            SymLog_debug("syncScriptExists=%d for path %s", syncScriptExists, syncScript);

            if (syncScriptExists) {
                // In Java this would be a beanshell.  Here we assume a shell script we'll exec.
                char *tmpOutputDir = SymStringUtils_format("tmp/staging/filesync_incoming/processing/%s/", batchId);
                SymFileUtils_mkdir(tmpOutputDir);

                char *fileListProcessed = SymStringUtils_format("tmp/staging/filesync_incoming/processing/%s/fileList.txt", batchId);
                char *outputFileName = SymStringUtils_format("tmp/staging/filesync_incoming/processing/%s/stdout.txt", batchId);
                remove(fileListProcessed);
                remove(outputFileName);
                char* cmd = SymStringUtils_format("/bin/sh %s \"%s\" \"%s\" \"%s\" 1>&2 > %s", syncScript, batchDir, sourceNodeId, fileListProcessed, outputFileName);

                SymLog_debug("Exec script %s", cmd);

                int rc = system(cmd);

                unsigned short outputFileExists = SymFileUtils_exists(fileListProcessed);

                SymLog_debug("Script rc=%d, outputFileExists=%d", rc, outputFileExists);

                if (rc == 0 && outputFileExists) {
                    SymProperties *filesToEventType = SymProperties_newWithFile(NULL, fileListProcessed);

                    if (this->engine->parameterService->is(this->engine->parameterService, SYM_PARAMETER_FILE_SYNC_PREVENT_PING_BACK, 1)) {
                        this->updateFileIncoming(this, sourceNodeId, filesToEventType);
                    }
                    incomingBatch->statementCount = filesToEventType != NULL ? filesToEventType->index : 0;
                    incomingBatch->status = "OK";
                    if (incomingBatchService->isRecordOkBatchesEnabled) {
                        incomingBatchService->updateIncomingBatch(incomingBatchService, incomingBatch);
                    } else if (incomingBatch->retry) {
                        incomingBatchService->deleteIncomingBatch(incomingBatchService, incomingBatch);
                    }
                } else {
                    incomingBatch->errorFlag = 1;
                    incomingBatch->status = SymStringUtils_format("%s", "ER");
                    char * output = NULL;
                    if (SymFileUtils_exists(outputFileName)) {
                        output = SymFileUtils_readFile(outputFileName);
                        incomingBatch->sqlMessage = SymStringUtils_format("Error %d, cmd failed: %s output: %s", rc, cmd, output);
                    } else {
                        incomingBatch->sqlMessage = SymStringUtils_format("Error %d, cmd failed: %s", rc, cmd);
                    }
                    if (incomingBatchService->isRecordOkBatchesEnabled || incomingBatch->retry) {
                        incomingBatchService->updateIncomingBatch(incomingBatchService, incomingBatch);
                    } else {
                        incomingBatchService->insertIncomingBatch(incomingBatchService, incomingBatch);
                    }
                }

                SymFileUtils_deleteDir(tmpOutputDir);
                free(syncScript);
                free(batchInfo);
                free(batchDir);
                free(tmpOutputDir);
                free(fileListProcessed);
                free(outputFileName);
            }
        }
    }

    free(unzipDir);
    free(timestamp);

    return batchesProcessed;
}


void SymFileSyncService_updateFileIncoming(SymFileSyncService *this, char* nodeId, SymProperties* filesToEventType) {

    int i;
    for (i = 0; i < filesToEventType->index; i++) {

        char* filePath = filesToEventType->propArray[i].key;
        char* eventType = filesToEventType->get(filesToEventType, filePath, "C");

        filePath = SymStringUtils_replace(filePath, "//", "/");
        SymStringArray *parts = SymStringArray_split(filePath, "/");

        char* fileName = parts->get(parts, parts->size-1);
        SymStringBuilder *buff = SymStringBuilder_new();

        int j;
        for (j = 0; j < parts->size-1; ++j) {
            buff->append(buff, parts->get(parts, j));
            if (j < parts->size-1) {
                buff->append(buff, "/");
            }
        }

        char *dirName = buff->destroyAndReturn(buff);

        time_t lastUpdateTime = SymFileUtils_getFileLastModified(filePath);

        SymStringArray *args = SymStringArray_new(NULL);
        args->add(args, nodeId);
        args->add(args, SymStringUtils_format("%ld", lastUpdateTime));
        args->add(args, eventType);
        args->add(args, dirName);
        args->add(args, fileName);

        int error;
        SymSqlTemplate *sqlTemplate = this->engine->platform->getSqlTemplate(this->engine->platform);

        int updateCount = sqlTemplate->update(sqlTemplate, SYM_SQL_UPDATE_FILE_INCOMING, args, NULL, &error);

        if (updateCount == 0) {
            updateCount = sqlTemplate->update(sqlTemplate, SYM_SQL_INSERT_FILE_INCOMING, args, NULL, &error);
        }
        args->destroy(args);
    }
}

void SymFileSyncService_cleanExtractDir(SymFileSyncService *this) {
    SymList/*<char*>*/ * filesNames = SymFileUtils_listFiles("./tmp/staging/filesync_incoming/extract/");
    int i;
    for (i = 0; i < filesNames->size; ++i) {
        char* fileName = SymStringUtils_format("./tmp/staging/filesync_incoming/extract/%s", filesNames->get(filesNames, i));
        time_t now = time(0);
        long ONE_DAY = 24*60*60;
        if (SymFileUtils_isDir(fileName) && (now - SymFileUtils_getFileLastModified(fileName)) > ONE_DAY) {
            SymFileUtils_deleteDir(fileName);
        }
        free(fileName);
    }
    filesNames->destroy(filesNames);
}


void SymFileSyncService_destroy(SymFileSyncService *this) {
    free(this);
}

SymFileSyncService * SymFileSyncService_new(SymFileSyncService *this, SymEngine *engine) {
    if (this == NULL) {
        this = (SymFileSyncService *) calloc(1, sizeof(SymFileSyncService));
    }
    this->engine = engine;
    this->trackChanges = (void *) &SymFileSyncService_trackChanges;
    this->getFileTrigger = (void *) &SymFileSyncService_getFileTrigger;
    this->getFileTriggerRoutersForCurrentNode = (void *)&SymFileSyncService_getFileTriggerRoutersForCurrentNode;
    this->getDirectorySnapshot = (void *) &SymFileSyncService_getDirectorySnapshot;
    this->saveDirectorySnapshot = (void *) &SymFileSyncService_saveDirectorySnapshot;
    this->saveSnapshot = (void *) &SymFileSyncService_saveSnapshot;
    this->findSourceNodeIdFromFileIncoming = (void *) &SymFileSyncService_findSourceNodeIdFromFileIncoming;
    this->save = (void *) &SymFileSyncService_save;
    this->pushFilesToNodes = (void *) &SymFileSyncService_pushFilesToNodes;
    this->pushFilesToNode = (void *) &SymFileSyncService_pushFilesToNode;
    this->sendFiles = (void *) &SymFileSyncService_sendFiles;
    this->getBatchesToProcess = (void *) &SymFileSyncService_getBatchesToProcess;
    this->getFileTriggerRouter = (void *)&SymFileSyncService_getFileTriggerRouter;
    this->pullFilesFromNodes = (void *)&SymFileSyncService_pullFilesFromNodes;
    this->pullFilesFromNode = (void *)&SymFileSyncService_pullFilesFromNode;
    this->processZip = (void *)&SymFileSyncService_processZip;
    this->cleanExtractDir = (void *)&SymFileSyncService_cleanExtractDir;
    this->updateFileIncoming = (void *)&SymFileSyncService_updateFileIncoming;
    this->destroy = (void *) &SymFileSyncService_destroy;
    return this;
}
