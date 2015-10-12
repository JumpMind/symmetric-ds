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

SymNodeChannel* SymConfigurationService_getNodeChannels(SymConfigurationService *this) {
	SymLog_info("SymConfigurationService_getNodeChannels");
    return NULL;
}

static SymChannel * SymConfigurationService_channelMapper(SymRow *row) {
    SymChannel *channel = SymChannel_new(NULL);
    channel->channelId = row->getString(row, "channel_id");
    channel->processingOrder = row->getInt(row, "processing_order");
    channel->maxBatchSize = row->getInt(row, "max_batch_size");
    channel->enabled = row->getBoolean(row, "enabled");
    channel->maxBatchToSend = row->getInt(row, "max_batch_to_send");
    channel->maxDataToRoute = row->getInt(row, "max_data_to_route");
    channel->useOldDataToRoute = row->getBoolean(row, "use_old_data_to_route");
    channel->useRowDataToRoute = row->getBoolean(row, "use_row_data_to_route");
    channel->usePkDataToRoute = row->getBoolean(row, "use_pk_data_to_route");
    channel->containsBigLob = row->getBoolean(row, "contains_big_lob");
    channel->batchAlgorithm = row->getString(row, "batch_algorithm");
    channel->extractPeriodMillis = row->getLong(row, "extract_period_millis");
    channel->dataLoaderType = row->getString(row, "data_loader_type");
    channel->createTime = row->getDate(row, "create_time");
    channel->lastUpdateBy = row->getString(row, "last_update_by");
    channel->lastUpdateTime = row->getDate(row, "last_update_time");
    channel->reloadFlag = row->getBoolean(row, "reload_flag");
    channel->fileSyncFlag = row->getBoolean(row, "file_sync_flag");
    return channel;
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
        channels->put(channels, channel->channelId, channel, sizeof(SymChannel));
    }
    iter->destroy(iter);
    list->destroy(list);
    return channels;
}

void SymConfigurationService_clearCache(SymConfigurationService *this) {
	// TODO
}

void SymConfigurationService_destroy(SymConfigurationService * this) {
    free(this);
}

SymConfigurationService * SymConfigurationService_new(SymConfigurationService *this, SymDatabasePlatform *platform) {
    if (this == NULL) {
        this = (SymConfigurationService *) calloc(1, sizeof(SymConfigurationService));
    }
    this->platform = platform;
    this->getNodeChannels = (void *) &SymConfigurationService_getNodeChannels;
    this->getChannels = (void *) &SymConfigurationService_getChannels;
    this->clearCache = (void *) &SymConfigurationService_clearCache;
    this->destroy = (void *) &SymConfigurationService_destroy;
    return this;
}
