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

static void * SymNodeService_node_mapper(SymRow *row) {
    SymNode *node = SymNode_new(NULL);
    node->nodeId = row->getStringNew(row, "node_id");
    node->nodeGroupId = row->getStringNew(row, "node_group_id");
    node->externalId = row->getStringNew(row, "external_id");
    node->syncEnabled = row->getInt(row, "sync_enabled");
    node->syncUrl = row->getString(row, "sync_url");
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

SymNode * SymNodeService_findIdentity(SymNodeService *this) {
    SymNode *node = NULL;
    int error;
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    SymStringBuilder *sb = SymStringBuilder_newWithString(SYM_SQL_SELECT_NODE_PREFIX);
    sb->append(sb, SYM_SQL_FIND_NODE_IDENTITY);
    SymList *nodes = sqlTemplate->query(sqlTemplate, sb->str, NULL, NULL, &error, SymNodeService_node_mapper);
    sb->destroy(sb);
    if (nodes->size > 0) {
        node = nodes->get(nodes, 0);
    }
    return node;
}

SymNodeSecurity * SymNodeService_findNodeSecurity(SymNodeService *this, char *nodeId) {
    printf("SymNodeService_find_node_security\n");
    return NULL;
}

SymList * SymNodeService_findNodesToPull(SymNodeService *this) {
    SymList *list = SymList_new(NULL);
    return list;
}

SymList * SymNodeService_findNodesToPushTo(SymNodeService *this) {
    printf("SymNodeService_find_nodes_to_push_to\n");
    return NULL;
}

int SymNodeService_isDataloadStarted(SymNodeService *this) {
    printf("SymNodeService_is_dataload_started\n");
    return 0;
}

void SymNodeService_destroy(SymNodeService *this) {
    free(this);
}

SymNodeService * SymNodeService_new(SymNodeService *this, SymDatabasePlatform *platform) {
    if (this == NULL) {
        this = (SymNodeService *) calloc(1, sizeof(SymNodeService));
    }
    this->platform = platform;
    this->findIdentity = (void *) &SymNodeService_findIdentity;
    this->findNodeSecurity = (void *) &SymNodeService_findNodeSecurity;
    this->findNodesToPull = (void *) &SymNodeService_findNodesToPull;
    this->findNodesToPushTo = (void *) &SymNodeService_findNodesToPushTo;
    this->isDataloadStarted = (void *) &SymNodeService_isDataloadStarted;
    this->destroy = (void *) &SymNodeService_destroy;
    return this;
}
