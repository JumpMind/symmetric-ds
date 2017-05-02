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
#include "service/NodeService.h"

static SymDataLoadStatus * SymNodeService_dataLoadStatusMapper(SymRow *row) {
    SymDataLoadStatus *status = SymDataLoadStatus_new();
    status->initialLoadEnabled = row->getInt(row, "initial_load_enabled");
    status->initialLoadTime = row->getDate(row, "initial_load_time");
    return status;
}

static SymNode * SymNodeService_nodeMapper(SymRow *row) {
    SymNode *node = SymNode_new(NULL);
    node->nodeId = row->getStringNew(row, "node_id");
    node->nodeGroupId = row->getStringNew(row, "node_group_id");
    node->externalId = row->getStringNew(row, "external_id");
    node->syncEnabled = row->getBoolean(row, "sync_enabled");
    node->syncUrl = row->getStringNew(row, "sync_url");
    node->schemaVersion = row->getStringNew(row, "schema_version");
    node->databaseType = row->getStringNew(row, "database_type");
    node->databaseVersion = row->getStringNew(row, "database_version");
    node->symmetricVersion = row->getStringNew(row, "symmetric_version");
    node->createdAtNodeId = row->getStringNew(row, "created_at_node_id");
    node->batchToSendCount = row->getInt(row, "batch_to_send_count");
    node->batchInErrorCount = row->getInt(row, "batch_in_error_count");
    node->deploymentType = row->getStringNew(row, "deployment_type");
    return node;
}

static SymNodeSecurity * SymNodeService_nodeSecurityMapper(SymRow * row) {
    SymNodeSecurity *nodeSecurity = SymNodeSecurity_new(NULL);
    nodeSecurity->nodeId = row->getStringNew(row, "node_id");
    nodeSecurity->nodePassword = row->getStringNew(row, "node_password");
    nodeSecurity->registrationEnabled = row->getBoolean(row, "registration_enabled");
    nodeSecurity->registrationTime = row->getDate(row, "registration_time");
    nodeSecurity->initialLoadEnabled = row->getBoolean(row, "initial_load_enabled");
    nodeSecurity->initialLoadTime = row->getDate(row, "initial_load_time");
    nodeSecurity->createdAtNodeId = row->getStringNew(row, "created_at_node_id");
    nodeSecurity->revInitialLoadEnabled = row->getBoolean(row, "rev_initial_load_enabled");
    nodeSecurity->revInitialLoadTime = row->getDate(row, "rev_initial_load_time");
    nodeSecurity->initialLoadId = row->getLong(row, "initial_load_id");
    nodeSecurity->initialLoadCreateBy = row->getStringNew(row, "initial_load_create_by");
    nodeSecurity->revInitialLoadId = row->getLong(row, "rev_initial_load_id");
    nodeSecurity->revInitialLoadCreateBy = row->getStringNew(row, "rev_initial_load_create_by");
    return nodeSecurity;
}

void SymDataLoadStatus_destroy(SymDataLoadStatus *this) {
    this->initialLoadTime->destroy(this->initialLoadTime);
    free(this);
}

SymDataLoadStatus * SymDataLoadStatus_new() {
    SymDataLoadStatus *this = calloc(1, sizeof(SymDataLoadStatus));
    this->destroy = (void *) &SymDataLoadStatus_destroy;
    return this;
}

SymNode * SymNodeService_findIdentityWithCache(SymNodeService *this, unsigned short useCache) {
    if (this->cachedNodeIdentity == NULL || useCache == 0) {
        if (this->cachedNodeIdentity) {
            this->cachedNodeIdentity->destroy(this->cachedNodeIdentity);
        }
        int error;
        SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
        SymStringBuilder *sb = SymStringBuilder_newWithString(SYM_SQL_SELECT_NODE_PREFIX);
        sb->append(sb, SYM_SQL_FIND_NODE_IDENTITY);

        SymList *nodes = sqlTemplate->query(sqlTemplate, sb->str, NULL, NULL, &error, (void *) SymNodeService_nodeMapper);
        this->cachedNodeIdentity = nodes->get(nodes, 0);
        nodes->destroy(nodes);
        sb->destroy(sb);
    }
    return this->cachedNodeIdentity;
}

SymNode * SymNodeService_findIdentity(SymNodeService *this) {
    return SymNodeService_findIdentityWithCache(this, 1);
}

SymNodeSecurity * SymNodeService_findNodeSecurity(SymNodeService *this, char *nodeId) {
    SymStringArray *args = SymStringArray_new(NULL);
    args->add(args, nodeId);

    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    int error;
    SymList *nodes = sqlTemplate->query(sqlTemplate, SYM_SQL_FIND_NODE_SECURITY, args, NULL, &error, (void *) SymNodeService_nodeSecurityMapper);

    SymNodeSecurity *nodeSecurity = (SymNodeSecurity *) nodes->get(nodes, 0);
    args->destroy(args);
    nodes->destroy(nodes);
    return nodeSecurity;
}

SymList * SymNodeService_findNodesToPull(SymNodeService *this) {
    return this->findSourceNodesFor(this, SymNodeGroupLinkAction_W);
}

SymList * SymNodeService_findNodesToPushTo(SymNodeService *this) {
    return this->findTargetNodesFor(this, SymNodeGroupLinkAction_P);
}

SymList * SymNodeService_findSourceNodesFor(SymNodeService *this, SymNodeGroupLinkAction nodeGroupLinkAction) {
    SymList *nodes = NULL;
    SymNode *node = this->findIdentity(this);
    if (node != NULL) {
        int error;
        SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
        SymStringBuilder *sb = SymStringBuilder_newWithString(SYM_SQL_SELECT_NODE_PREFIX);
        sb->append(sb, SYM_SQL_FIND_NODES_WHO_TARGET_ME);
        SymStringArray *args = SymStringArray_new(NULL);
        args->add(args, node->nodeGroupId)->add(args, SymNodeGroupLinkAction_toString(nodeGroupLinkAction));

        nodes = sqlTemplate->query(sqlTemplate, sb->str, args, NULL, &error, (void *) SymNodeService_nodeMapper);
        args->destroy(args);
        sb->destroy(sb);
    } else {
        nodes = SymList_new(NULL);
    }
    return nodes;
}

SymList * SymNodeService_findTargetNodesFor(SymNodeService *this, SymNodeGroupLinkAction nodeGroupLinkAction) {
    SymList *nodes = NULL;
    SymNode *node = this->findIdentity(this);
    if (node != NULL) {
        int error;
        SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
        SymStringBuilder *sb = SymStringBuilder_newWithString(SYM_SQL_SELECT_NODE_PREFIX);
        sb->append(sb, SYM_SQL_FIND_NODES_WHO_I_TARGET);
        SymStringArray *args = SymStringArray_new(NULL);
        args->add(args, node->nodeGroupId)->add(args, SymNodeGroupLinkAction_toString(nodeGroupLinkAction));

        nodes = sqlTemplate->query(sqlTemplate, sb->str, args, NULL, &error, (void *) SymNodeService_nodeMapper);
        args->destroy(args);
        sb->destroy(sb);
    } else {
        nodes = SymList_new(NULL);
    }
    return nodes;
}

unsigned short SymNodeService_isDataloadStarted(SymNodeService *this) {
    return this->getNodeStatus(this) ==  SYM_NODE_STATUS_DATA_LOAD_STARTED;
}

unsigned short SymNodeService_isDataloadCompleted(SymNodeService *this) {
    return this->getNodeStatus(this) ==  SYM_NODE_STATUS_DATA_LOAD_COMPLETED;
}

int SymNodeService_getNodeStatus(SymNodeService *this) {
    int error, status = SYM_NODE_STATUS_DATA_LOAD_NOT_STARTED;
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    SymList *loadStatuses = sqlTemplate->query(sqlTemplate, SYM_SQL_GET_DATALOAD_STATUS, NULL, NULL, &error, (void *) SymNodeService_dataLoadStatusMapper);
    if (loadStatuses->size > 0) {
        SymDataLoadStatus *loadStatus = (SymDataLoadStatus *) loadStatuses->get(loadStatuses, 0);
        if (loadStatus->initialLoadEnabled == 1) {
            status = SYM_NODE_STATUS_DATA_LOAD_STARTED;
        } else if (loadStatus->initialLoadTime != NULL) {
            status = SYM_NODE_STATUS_DATA_LOAD_COMPLETED;
        }
    }
    loadStatuses->destroy(loadStatuses);
    return status;
}

unsigned short SymNodeService_updateNode(SymNodeService *this, SymNode *node) {
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);

    SymDate *now = SymDate_new(NULL);

    char *syncEnabled = SymStringUtils_format("%d", node->syncEnabled);
    char *batchToSendCount = SymStringUtils_format("%d", node->batchToSendCount);
    char *batchInErrorCount = SymStringUtils_format("%d", node->batchInErrorCount);
    char *timezoneOffset = SymAppUtils_getTimezoneOffset();

    SymStringArray *args = SymStringArray_new(NULL);
    args->add(args, node->nodeGroupId);
    args->add(args, node->externalId);
    args->add(args, node->databaseType);
    args->add(args, node->databaseVersion);
    args->add(args, node->schemaVersion);
    args->add(args, node->symmetricVersion);
    args->add(args, node->syncUrl);
    args->add(args, now->dateTimeString);
    args->add(args, syncEnabled);
    args->add(args, timezoneOffset);
    args->add(args, batchToSendCount);
    args->add(args, batchInErrorCount);
    args->add(args, node->createdAtNodeId);
    args->add(args, node->deploymentType);
    args->add(args, node->nodeId);

    int error;
    unsigned short updated = sqlTemplate->update(sqlTemplate,
            SYM_SQL_UPDATE_NODE, args, NULL, &error) == 1;

    free(timezoneOffset);
    free(syncEnabled);
    free(batchToSendCount);
    free(batchInErrorCount);
    args->destroy(args);
    now->destroy(now);

    return updated;
}

void SymNodeService_save(SymNodeService *this, SymNode *node) {

    if (! SymNodeService_updateNode(this, node)) {
        SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);

        SymDate *now = SymDate_new(NULL);

        char *syncEnabled = SymStringUtils_format("%d", node->syncEnabled);
        char *batchToSendCount = SymStringUtils_format("%d", node->batchToSendCount);
        char *batchInErrorCount = SymStringUtils_format("%d", node->batchInErrorCount);

        SymStringArray *args = SymStringArray_new(NULL);
        args->add(args, node->nodeGroupId);
        args->add(args, node->externalId);
        args->add(args, node->databaseType);
        args->add(args, node->databaseVersion);
        args->add(args, node->schemaVersion);
        args->add(args, node->symmetricVersion);
        args->add(args, node->syncUrl);
        args->add(args, now->dateTimeString);
        args->add(args, syncEnabled);
        args->add(args, SymAppUtils_getTimezoneOffset());
        args->add(args, batchToSendCount);
        args->add(args, batchInErrorCount);
        args->add(args, node->createdAtNodeId);
        args->add(args, node->deploymentType);
        args->add(args, node->nodeId);

        int error;
        sqlTemplate->update(sqlTemplate, SYM_SQL_INSERT_NODE, args, NULL, &error);

        free(syncEnabled);
        free(batchToSendCount);
        free(batchInErrorCount);
        args->destroy(args);
        now->destroy(now);
    }
}

void SymNodeService_updateNodeHost(SymNodeService *this, SymNodeHost *nodeHost) {
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);

    SymStringArray *params = SymStringArray_new(NULL);
    params->add(params, nodeHost->ipAddress);
    params->add(params, nodeHost->osUser);
    params->add(params, nodeHost->osName);
    params->add(params, nodeHost->osArch);
    params->add(params, nodeHost->osVersion);
    // TODO support memory stats?
//    params->add(params, nodeHost->availableProcessors);
//    params->add(params, nodeHost->freeMemoryBytes);
//    params->add(params, nodeHost->totalMemoryBytes);
//    params->add(params, nodeHost->maxMemoryBytes);
    params->add(params, "");
    params->add(params, "");
    params->add(params, "");
    params->add(params, "");
    params->add(params, nodeHost->javaVersion);
    params->add(params, nodeHost->javaVendor);
    params->add(params, nodeHost->jdbcVersion);
    params->add(params, nodeHost->symmetricVersion);
    params->add(params, nodeHost->timezoneOffset);
    params->add(params, nodeHost->heartbeatTime->dateTimeString);
    params->add(params, nodeHost->lastRestartTime->dateTimeString);
    params->add(params, nodeHost->nodeId);
    params->add(params, nodeHost->hostName);

    int error;
    int updateCount = sqlTemplate->update(sqlTemplate, SYM_SQL_UPDATE_NODE_HOST, params, NULL, &error);

    if (updateCount == 0) {
        sqlTemplate->update(sqlTemplate, SYM_SQL_INSERT_NODE_HOST, params, NULL, &error);
    }

    params->destroy(params);
}

void SymNodeService_updateNodeHostForCurrentNode(SymNodeService *this) {
    SymNode *node = SymNodeService_findIdentity(this);
    SymNodeHost *nodeHostForCurrentNode = SymNodeHost_new(NULL);
    nodeHostForCurrentNode->nodeId = node->nodeId;
    nodeHostForCurrentNode->refresh(nodeHostForCurrentNode);
    nodeHostForCurrentNode->lastRestartTime = this->lastRestartTime;
    SymNodeService_updateNodeHost(this, nodeHostForCurrentNode);
    nodeHostForCurrentNode->destroy(nodeHostForCurrentNode);
}

SymList * SymNodeService_findEnabledNodesFromNodeGroup(SymNodeService *this, char *nodeGroupId) {
    int error;
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    SymStringBuilder *sb = SymStringBuilder_newWithString(SYM_SQL_SELECT_NODE_PREFIX);
    sb->append(sb, SYM_SQL_FIND_ENABLED_NODES_FROM_NODE_GROUP_SQL);

    SymStringArray *args = SymStringArray_new(NULL);
    args->add(args, nodeGroupId);

    SymList *nodes = sqlTemplate->query(sqlTemplate, sb->str, args, NULL, &error, (void *) SymNodeService_nodeMapper);
    sb->destroy(sb);
    args->destroy(args);
    return nodes;
}

SymNode * SymNodeService_findNode(SymNodeService *this, char* nodeId) {
    int error;
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    SymStringBuilder *sb = SymStringBuilder_newWithString(SYM_SQL_SELECT_NODE_PREFIX);
    sb->append(sb, SYM_SQL_FIND_NODE);

    SymStringArray *args = SymStringArray_new(NULL);
    args->add(args, nodeId);

    SymList *nodes = sqlTemplate->query(sqlTemplate, sb->str, args, NULL, &error, (void *) SymNodeService_nodeMapper);
    SymNode *node = NULL;

    if (nodes->size > 0) {
        node = nodes->get(nodes, 0);
    }

    nodes->destroy(nodes);
    sb->destroy(sb);
    args->destroy(args);
    return node;
}

void SymNodeService_destroy(SymNodeService *this) {
    if (this->lastRestartTime) {
        this->lastRestartTime->destroy(this->lastRestartTime);
    }
    free(this);
}

SymNodeService * SymNodeService_new(SymNodeService *this, SymDatabasePlatform *platform) {
    if (this == NULL) {
        this = (SymNodeService *) calloc(1, sizeof(SymNodeService));
    }
    this->platform = platform;
    this->lastRestartTime = SymDate_new();
    this->findIdentity = (void *) &SymNodeService_findIdentity;
    this->findIdentityWithCache = (void *) &SymNodeService_findIdentityWithCache;
    this->findNodeSecurity = (void *) &SymNodeService_findNodeSecurity;
    this->findNodesToPull = (void *) &SymNodeService_findNodesToPull;
    this->findNodesToPushTo = (void *) &SymNodeService_findNodesToPushTo;
    this->findSourceNodesFor = (void *) &SymNodeService_findSourceNodesFor;
    this->findTargetNodesFor = (void *) &SymNodeService_findTargetNodesFor;
    this->isDataloadStarted = (void *) &SymNodeService_isDataloadStarted;
    this->isDataloadCompleted = (void *) &SymNodeService_isDataloadCompleted;
    this->getNodeStatus = (void *) &SymNodeService_getNodeStatus;
    this->save = (void *) &SymNodeService_save;
    this->updateNodeHostForCurrentNode = (void *) &SymNodeService_updateNodeHostForCurrentNode;
    this->findEnabledNodesFromNodeGroup = (void*) &SymNodeService_findEnabledNodesFromNodeGroup;
    this->findNode = (void*) &SymNodeService_findNode;
    this->destroy = (void *) &SymNodeService_destroy;
    return this;
}
