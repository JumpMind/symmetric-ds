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
#include "service/ConfigurationService.h"
#include "common/Log.h"

static SymChannel * SymConfigurationService_channelMapper(SymRow *row) {
    SymChannel *channel = SymChannel_new(NULL);
    channel->channelId = row->getStringNew(row, "channel_id");
    channel->processingOrder = row->getInt(row, "processing_order");
    channel->maxBatchSize = row->getInt(row, "max_batch_size");
    channel->enabled = row->getBoolean(row, "enabled");
    channel->maxBatchToSend = row->getInt(row, "max_batch_to_send");
    channel->maxDataToRoute = row->getInt(row, "max_data_to_route");
    channel->useOldDataToRoute = row->getBoolean(row, "use_old_data_to_route");
    channel->useRowDataToRoute = row->getBoolean(row, "use_row_data_to_route");
    channel->usePkDataToRoute = row->getBoolean(row, "use_pk_data_to_route");
    channel->containsBigLob = row->getBoolean(row, "contains_big_lob");
    channel->batchAlgorithm = row->getStringNew(row, "batch_algorithm");
    channel->extractPeriodMillis = row->getLong(row, "extract_period_millis");
    channel->dataLoaderType = row->getStringNew(row, "data_loader_type");
    channel->createTime = row->getDate(row, "create_time");
    channel->lastUpdateBy = row->getStringNew(row, "last_update_by");
    channel->lastUpdateTime = row->getDate(row, "last_update_time");
    channel->reloadFlag = row->getBoolean(row, "reload_flag");
    channel->fileSyncFlag = row->getBoolean(row, "file_sync_flag");
    return channel;
}

static SymNodeGroupLink * SymConfigurationService_nodeGroupLinkMapper(SymRow *row) {
    SymNodeGroupLink *link = SymNodeGroupLink_new(NULL);
    link->sourceNodeGroupId = row->getStringNew(row, "source_node_group_id");
    link->targetNodeGroupId = row->getStringNew(row, "target_node_group_id");
    link->dataEventAction = SymNodeGroupLinkAction_fromCode(row->getStringNew(row, "data_event_action"));
    link->syncConfigEnabled = row->getBoolean(row, "sync_config_enabled");
    link->createTime = row->getDate(row, "create_time");
    link->lastUpdateBy = row->getStringNew(row, "last_update_by");
    link->lastUpdateTime = row->getDate(row, "last_update_time");
    return link;
}

SymList * SymConfigurationService_getNodeGroupLinks(SymConfigurationService *this, unsigned short refreshCache) {
    if (refreshCache) {
        this->nodeGroupLinkCacheTime = 0;
    }
    long cacheTimeoutInMs = this->parameterService->getLong(this->parameterService, SYM_PARAMETER_CACHE_TIMEOUT_NODE_GROUP_LINK_IN_MS, 600000);
    if ((time(NULL) - this->nodeGroupLinkCacheTime) * 1000 >= cacheTimeoutInMs || this->nodeGroupLinksCache == NULL) {
        SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
        int error;
        this->nodeGroupLinksCache = sqlTemplate->query(sqlTemplate, SYM_SQL_GROUPS_LINKS_SQL, NULL, NULL, &error,
                (void *) &SymConfigurationService_nodeGroupLinkMapper);
        this->nodeGroupLinkCacheTime = time(NULL);
    }

    return this->nodeGroupLinksCache;
}

SymList * SymConfigurationService_getNodeGroupLinksFor(SymConfigurationService *this, char *sourceNodeGroupId, unsigned short refreshCache) {
    SymList *links = SymConfigurationService_getNodeGroupLinks(this, refreshCache);
    SymList *target = SymList_new(NULL);
    SymIterator *iter = links->iterator(links);
    while (iter->hasNext(iter)) {
        SymNodeGroupLink *nodeGroupLink = (SymNodeGroupLink *) iter->next(iter);
        if (SymStringUtils_equals(nodeGroupLink->sourceNodeGroupId, sourceNodeGroupId)) {
            target->add(target, nodeGroupLink);
        }
    }
    iter->destroy(iter);
    return target;
}

SymNodeGroupLink * SymConfigurationService_getNodeGroupLinkFor(SymConfigurationService *this, char *sourceNodeGroupId, char *targetNodeGroupId,
        unsigned short refreshCache) {
    SymList *links = SymConfigurationService_getNodeGroupLinks(this, refreshCache);
    SymNodeGroupLink *link = NULL;
    SymIterator *iter = links->iterator(links);
    while (iter->hasNext(iter)) {
        SymNodeGroupLink *nodeGroupLink = (SymNodeGroupLink *) iter->next(iter);
        if (SymStringUtils_equals(nodeGroupLink->targetNodeGroupId, targetNodeGroupId) &&
                SymStringUtils_equals(nodeGroupLink->sourceNodeGroupId, sourceNodeGroupId)) {
            link = nodeGroupLink;
            break;
        }
    }
    iter->destroy(iter);
    return link;
}

// TODO: cache the channels
SymMap * SymConfigurationService_getChannels(SymConfigurationService *this, unsigned int refreshCache) {
    int error;
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    SymList *list = sqlTemplate->query(sqlTemplate, SYM_SQL_SELECT_CHANNEL, NULL, NULL, &error, (void *) SymConfigurationService_channelMapper);
    SymMap *channels = SymMap_new(NULL, list->size);
    SymIterator *iter = list->iterator(list);
    while (iter->hasNext(iter)) {
        SymChannel *channel = (SymChannel *) iter->next(iter);
        channels->put(channels, channel->channelId, channel);
    }
    iter->destroy(iter);
    list->destroy(list);
    return channels;
}

SymList * /*<SymChannel>*/ SymConfigurationService_getFileSyncChannels(SymConfigurationService *this, unsigned int refreshCache) {
    SymMap* channels = this->getChannels(this, refreshCache);
    SymList* list = channels->values(channels);
    SymList* fileSyncChannels = SymList_new(NULL);

    int i;
    for (i = 0; i < list->size; ++i) {
        SymChannel *channel = list->get(list, i);
        if (channel->fileSyncFlag) {
            fileSyncChannels->add(fileSyncChannels, channel);
        }
    }

    channels->destroy(channels);
    return fileSyncChannels;
}

SymChannel * SymConfigurationService_getChannel(SymConfigurationService *this, char *channelId) {
    // TODO refactor to use getNodeChannels like the Java code.
    SymMap *channels = SymConfigurationService_getChannels(this, 0);
    SymChannel *channel = channels->remove(channels, channelId);
    channels->destroyAll(channels, (void *)SymChannel_destroy);
    return channel;
}

void SymConfigurationService_clearCache(SymConfigurationService *this) {
	// TODO
}

void SymConfigurationService_destroy(SymConfigurationService * this) {
    free(this);
}

SymConfigurationService * SymConfigurationService_new(SymConfigurationService *this, SymParameterService *parameterService, SymDatabasePlatform *platform) {
    if (this == NULL) {
        this = (SymConfigurationService *) calloc(1, sizeof(SymConfigurationService));
    }
    this->parameterService = parameterService;
    this->platform = platform;
    this->getChannels = (void *) &SymConfigurationService_getChannels;
    this->getChannel = (void *) &SymConfigurationService_getChannel;
    this->getNodeGroupLinks = (void *) &SymConfigurationService_getNodeGroupLinks;
    this->getNodeGroupLinkFor = (void *) &SymConfigurationService_getNodeGroupLinkFor;
    this->getNodeGroupLinksFor = (void *) &SymConfigurationService_getNodeGroupLinksFor;
    this->getFileSyncChannels = (void *) &SymConfigurationService_getFileSyncChannels;
    this->clearCache = (void *) &SymConfigurationService_clearCache;
    this->destroy = (void *) &SymConfigurationService_destroy;
    return this;
}
