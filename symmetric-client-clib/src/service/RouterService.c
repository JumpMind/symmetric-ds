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
#include "service/RouterService.h"
#include "common/Log.h"

static SymDataRouter * SymRouterService_getDataRouter(SymRouterService *this, SymRouter *router) {
    return this->routers->get(this->routers, SYM_ROUTER_DEFAULT);
}

static void SymRouterService_completeBatchesAndCommit(SymRouterService *this, SymChannelRouterContext *context) {
    SymList *usedRouters = context->usedDataRouters;
    SymList *batches = context->batchesByNodes->values(context->batchesByNodes);
    context->commit(context);

    SymIterator *iter = batches->iterator(batches);
    while (iter->hasNext(iter)) {
        SymOutgoingBatch *batch = (SymOutgoingBatch *) iter->next(iter);
        batch->routerMillis = (time(NULL) - batch->createTime->time) * 1000;
        SymIterator *routerIter = usedRouters->iterator(usedRouters);
        while (routerIter->hasNext(routerIter)) {
            SymDataRouter *router = (SymDataRouter *) routerIter->next(routerIter);
            router->completeBatch(router, context, batch);
        }
        routerIter->destroy(routerIter);
        if (SymStringUtils_equals(SYM_UNROUTED_NODE_ID, batch->nodeId)) {
            batch->status = SYM_OUTGOING_BATCH_OK;
        } else {
            batch->status = SYM_OUTGOING_BATCH_NEW;
        }
        this->outgoingBatchService->updateOutgoingBatch(this->outgoingBatchService, batch);
        context->batchesByNodes->remove(context->batchesByNodes, batch->nodeId);
    }
    iter->destroy(iter);

    SymIterator *routerIter = usedRouters->iterator(usedRouters);
    while (routerIter->hasNext(routerIter)) {
        SymDataRouter *router = (SymDataRouter *) routerIter->next(routerIter);
        router->contextCommitted(router, context);
    }
    routerIter->destroy(routerIter);
    context->needsCommitted = 0;
}

static int SymRouterService_insertDataEvents(SymRouterService *this, SymChannelRouterContext *context, SymDataMetaData *dataMetaData, SymList *nodeIds) {
    int numberOfDataEventsInserted = 0;
    if (nodeIds == NULL || nodeIds->size == 0) {
        nodeIds = SymList_new(NULL);
        nodeIds->add(nodeIds,SYM_UNROUTED_NODE_ID);
    }
    time_t ts = time(NULL);
    SymIterator *iter = nodeIds->iterator(nodeIds);
    while (iter->hasNext(iter)) {
        char *nodeId = (char *) iter->next(iter);
        if (nodeId) {
            SymMap *batches = context->batchesByNodes;
            SymOutgoingBatch *batch = batches->get(batches, nodeId);
            if (batch == NULL) {
                batch = SymOutgoingBatch_newWithNode(NULL, nodeId, dataMetaData->nodeChannel->channelId, SYM_OUTGOING_BATCH_ROUTING);

                SymLog_debug("About to insert a new batch for node %s on the '%s' channel.", nodeId, batch->channelId);

                this->outgoingBatchService->insertOutgoingBatch(this->outgoingBatchService, batch);
                context->batchesByNodes->put(context->batchesByNodes, nodeId, batch, sizeof(SymOutgoingBatch));
            }

            batch->incrementEventCount(batch, dataMetaData->data->eventType);
            batch->dataEventCount++;

            SymRouter *router = dataMetaData->router;
            context->addDataEvent(context, dataMetaData->data->dataId, batch->batchId, router ? router->routerId : SYM_UNKNOWN_ROUTER_ID);
            numberOfDataEventsInserted++;

            if (batch->dataEventCount >= dataMetaData->nodeChannel->maxBatchSize) {
                context->needsCommitted = 1;
            }
        }
    }
    context->statInsertDataEventsMs += ((time(NULL) - ts) * 1000);
    return numberOfDataEventsInserted;
}

SymList * SymRouterService_findAvailableNodes(SymRouterService *this, SymTriggerRouter *triggerRouter, SymChannelRouterContext *context) {
    SymList *nodes = context->availableNodes->get(context->availableNodes, triggerRouter->router->routerId);
    if (nodes == NULL) {
        nodes = SymList_new(NULL);
        SymRouter *router = triggerRouter->router;
        SymNodeGroupLink *link = this->configurationService->getNodeGroupLinkFor(this->configurationService,
                router->nodeGroupLink->sourceNodeGroupId, router->nodeGroupLink->targetNodeGroupId, 0);
        if (link != NULL) {
            // TODO: missing implementation
            //nodes->addAll(nodes, this->nodeService->findEnabledNodesFromNodeGroup(router->nodeGroupLink->targetNodeGroupId));
        } else {
            SymLog_error("The router %s has no node group link configured from %s to %s",
                    router->routerId, router->nodeGroupLink->sourceNodeGroupId, router->nodeGroupLink->targetNodeGroupId);
        }
        // TODO: the map can't be copying memory to do things like this
        //context->availableNodes->put(context->availableNodes, triggerRouter->router->routerId, nodes);
    }
    return nodes;
}

SymList * SymRouterService_getTriggerRoutersForData(SymRouterService *this, SymData *data) {
    SymList *triggerRouters = NULL;
    if (data != NULL) {
        if (data->triggerHistory) {
            // TODO: implement
            //triggerRouters = engine.getTriggerRouterService().getTriggerRoutersForCurrentNode(false).get((data.getTriggerHistory().getTriggerId()));
            if (triggerRouters == NULL || triggerRouters->size == 0) {
                //triggerRouters = engine.getTriggerRouterService().getTriggerRoutersForCurrentNode(true).get((data.getTriggerHistory().getTriggerId()));
            }
        } else {
            SymLog_warn("Could not find a trigger hist record for recorded data %ld.  Was the trigger hist record deleted manually?", data->dataId);
        }
    }
    return triggerRouters;
}

static int SymRouterService_routeDataToNodes(SymRouterService *this, SymData *data, SymChannelRouterContext *context) {
    int numberOfDataEventsInserted = 0;
    SymList *triggerRouters = SymRouterService_getTriggerRoutersForData(this, data);
    SymTable *table = this->platform->getTableFromCache(this->platform, data->triggerHistory->sourceCatalogName,
            data->triggerHistory->sourceSchemaName, data->triggerHistory->sourceTableName, 0);
    if (triggerRouters && triggerRouters->size > 0) {
        SymIterator *iter = triggerRouters->iterator(triggerRouters);
        while (iter->hasNext(iter)) {
            SymTriggerRouter *triggerRouter = (SymTriggerRouter *) iter->next(iter);

            SymDataMetaData *dataMetaData = SymDataMetaData_new(NULL, data, table, triggerRouter->router, context->channel);
            SymList *nodeIds = NULL;
            if (triggerRouter->isRouted(triggerRouter, data->eventType)) {
                SymDataRouter *dataRouter = SymRouterService_getDataRouter(this, triggerRouter->router);
                time_t ts = time(NULL);
                nodeIds = dataRouter->routeToNodes(dataRouter, context, dataMetaData,
                        SymRouterService_findAvailableNodes(this, triggerRouter, context), 0, 0, triggerRouter);
                context->statDataRouterMs += ((time(NULL) - ts) * 1000);

                if (nodeIds) {
                    if (!triggerRouter->pingBackEnabled && data->sourceNodeId) {
                        nodeIds->removeObject(nodeIds, data->sourceNodeId, (void *) strcmp);
                    }

                    // should never route to self
                    nodeIds->removeObject(nodeIds, context->nodeId, (void *) strcmp);
                }
            }
            numberOfDataEventsInserted += SymRouterService_insertDataEvents(this, context, dataMetaData, nodeIds);
        }
    } else {
        SymLog_warn("Could not find trigger routers for trigger history id of %d.  There is a good chance that data was captured and the trigger router link was removed before the data could be routed",
                data->triggerHistId);
        SymLog_info("Data with the id of %ld will be assigned to an unrouted batch", data->dataId);
        SymDataMetaData *dataMetaData = SymDataMetaData_new(NULL, data, table, NULL, context->channel);
        numberOfDataEventsInserted += SymRouterService_insertDataEvents(this, context, dataMetaData, NULL);
    }
    context->statDataEventsInserted += numberOfDataEventsInserted;
    return numberOfDataEventsInserted;
}

static int SymRouterService_selectDataAndRoute(SymRouterService *this, SymChannelRouterContext *context) {
    SymData *data = NULL;
    int totalDataCount = 0;
    int totalDataEventCount = 0;
    int maxNumberOfEventsBeforeFlush = this->parameterService->getInt(this->parameterService, SYM_PARAMETER_ROUTING_FLUSH_JDBC_BATCH_SIZE, 50000);

    // TODO: implement reader that returns data
    while (data != NULL) {
        totalDataCount++;
        int dataEventsInserted = SymRouterService_routeDataToNodes(this, data, context);
        totalDataEventCount += dataEventsInserted;

        if (maxNumberOfEventsBeforeFlush <= context->dataEventList->size || context->needsCommitted) {
            // TODO: implement dataService insertDataEvents
            //this->dataService->insertDataEvents(this->dataService, context->sqlTransaction, context->dataEventsList);
            context->dataEventList->reset(context->dataEventList);
        }
        if (context->needsCommitted) {
            SymRouterService_completeBatchesAndCommit(this, context);
        }
    }
    context->statDataRoutedCount += totalDataCount;
    return totalDataEventCount;
}

static int SymRouterService_routeDataForChannel(SymRouterService *this, SymChannel *nodeChannel, SymNode *sourceNode) {
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    SymChannelRouterContext *context = SymChannelRouterContext_new(NULL, sourceNode->nodeId, nodeChannel,
            sqlTemplate->startSqlTransaction(sqlTemplate));
    int dataCount = SymRouterService_selectDataAndRoute(this, context);
    if (dataCount > 0) {
        long insertTs = time(NULL);
        // TODO: implement insertDataEvents
        //this->dataService->insertDataEvents(this->dataService, context->sqlTransaction, context->dataEventsList);
        SymRouterService_completeBatchesAndCommit(this, context);
        context->statInsertDataEventsMs += ((time(NULL) - insertTs) * 1000);
    }
    return dataCount;
}

static int SymRouterService_routeDataForEachChannel(SymRouterService *this) {
    int dataCount = 0;
    SymNode *sourceNode = this->nodeService->findIdentity(this->nodeService);
    SymMap *channels = this->configurationService->getChannels(this->configurationService, 0);
    SymList *channelList = channels->values(channels);
    SymIterator *iter = channelList->iterator(channelList);
    while (iter->hasNext(iter)) {
        SymChannel *nodeChannel = (SymChannel *) iter->next(iter);
        if (nodeChannel->enabled) {
            dataCount += SymRouterService_routeDataForChannel(this, nodeChannel, sourceNode);
        } else {
            SymLog_debug("Not routing the %s channel.  It is either disabled or suspended.", nodeChannel->channelId);
        }
    }
    iter->destroy(iter);
    channelList->destroy(channelList);
    channels->destroy(channels);
    return dataCount;
}

long SymRouterService_routeData(SymRouterService *this) {
	time_t ts = time(NULL);
	int dataCount = SymRouterService_routeDataForEachChannel(this);
    ts = time(NULL) - ts;
    if (dataCount > 0 || ts * 1000 > SYM_LONG_OPERATION_THRESHOLD) {
        SymLog_info("Routed %d data events in %d ms", dataCount, ts);
    }
    return dataCount;
}

void SymRouterService_destroy(SymRouterService *this) {
    free(this);
}

SymRouterService * SymRouterService_new(SymRouterService *this, SymOutgoingBatchService *outgoingBatchService, SymSequenceService *sequenceService,
        SymDataService *dataService, SymNodeService *nodeService, SymConfigurationService *configurationService, SymParameterService *parameterService,
        SymDatabasePlatform *platform) {
    if (this == NULL) {
        this = (SymRouterService *) calloc(1, sizeof(SymRouterService));
    }
    this->outgoingBatchService = outgoingBatchService;
    this->sequenceService = sequenceService;
    this->nodeService = nodeService;
    this->configurationService = configurationService;
    this->parameterService = parameterService;
    this->platform = platform;
    this->routers = SymMap_new(NULL, 20);
    this->routers->put(this->routers, SYM_ROUTER_DEFAULT, SymDefaultDataRouter_new(NULL), sizeof(SymDefaultDataRouter));
    this->routeData = (void *) &SymRouterService_routeData;
    this->destroy = (void *) &SymRouterService_destroy;
    return this;
}
