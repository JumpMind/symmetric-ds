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
#include "transport/file/FileTransportManager.h"

int SymFileTransportManager_sendAcknowledgement(SymFileTransportManager *this, SymNode *remote, SymList *batches, SymNode *local, char *securityToken, char *registrationUrl) {
    return SYM_TRANSPORT_OK;
}

char * SymFileTransportManager_getDirName(SymFileTransportManager *this, char *paramName, SymNode *localNode, char *defaultDirName) {
    SymTransportManager *super = &this->super;
    char *dirName = super->parameterService->getString(super->parameterService, paramName, defaultDirName);
    return dirName;
}

SymFileIncomingTransport * SymFileTransportManager_getPullTransport(SymFileTransportManager *this, SymNode *remote, SymNode *local, char *securityToken, SymProperties *requestProperties, char *registrationUrl) {
     return SymFileIncomingTransport_new(NULL, remote, local,
             SymFileTransportManager_getDirName(this, SYM_PARAMETER_NODE_OFFLINE_INCOMING_DIR, local, "./tmp/offline/incoming"),
             SymFileTransportManager_getDirName(this, SYM_PARAMETER_NODE_OFFLINE_ARCHIVE_DIR, local, ""),
             SymFileTransportManager_getDirName(this, SYM_PARAMETER_NODE_OFFLINE_ERROR_DIR, local, ""));
}

SymOfflineFileSyncIncomingTransport * SymFileTransportManager_getFileSyncPullTransport(SymFileTransportManager *this, SymNode *remote, SymNode *local, char *securityToken, SymProperties *requestProperties, char *registrationUrl) {
     return SymOfflineFileSyncIncomingTransport_new(NULL, remote, local,
             SymFileTransportManager_getDirName(this, SYM_PARAMETER_NODE_OFFLINE_INCOMING_DIR, local, "./tmp/offline/incoming"),
             SymFileTransportManager_getDirName(this, SYM_PARAMETER_NODE_OFFLINE_ARCHIVE_DIR, local, ""),
             SymFileTransportManager_getDirName(this, SYM_PARAMETER_NODE_OFFLINE_ERROR_DIR, local, ""));
}

SymFileOutgoingTransport * SymFileTransportManager_getPushTransport(SymFileTransportManager *this, SymNode *remote, SymNode *local, char *securityToken, char *registrationUrl) {
    return SymFileOutgoingTransport_new(NULL, remote, local,
            SymFileTransportManager_getDirName(this, SYM_PARAMETER_NODE_OFFLINE_OUTGOING_DIR, local, "./tmp/offline/outgoing"));
}

SymOfflineFileSyncOutgoingTransport * SymFileTransportManager_getFileSyncPushTransport(SymFileTransportManager *this, SymNode *remote, SymNode *local, char *securityToken, char *registrationUrl) {
    return SymOfflineFileSyncOutgoingTransport_new(NULL, remote, local,
            SymFileTransportManager_getDirName(this, SYM_PARAMETER_NODE_OFFLINE_OUTGOING_DIR, local, "./tmp/offline/outgoing"));
}

SymList * SymFileTransportManager_readAcknowledgement(SymFileTransportManager *this, char *parameterString1, char *parameterString2) {
    SymStringBuilder *sb = SymStringBuilder_newWithString(parameterString1);
    sb->append(sb, "&")->append(sb, parameterString2);

    SymMap *parameters = SymHttpTransportManager_getParametersFromQueryUrl(parameterString1);
    SymList *batchAcks = SymList_new(NULL);
    SymStringArray *keys = parameters->keys(parameters);
    int i;
    for (i = 0; i < keys->size; i++) {
        char *key = keys->get(keys, i);
        if (strncmp(key, SYM_WEB_CONSTANTS_ACK_BATCH_NAME, strlen(SYM_WEB_CONSTANTS_ACK_BATCH_NAME)) == 0) {
            char *batchId = SymStringUtils_substring(key, strlen(SYM_WEB_CONSTANTS_ACK_BATCH_NAME), strlen(key));
            SymBatchAck *batchAck = SymHttpTransportManager_getBatchInfo(parameters, batchId);
            batchAcks->add(batchAcks, batchAck);
            free(batchId);
        }
    }
    keys->destroy(keys);
    parameters->destroy(parameters);
    sb->destroy(sb);
    return batchAcks;
}

void SymFileTransportManager_destroy(SymFileTransportManager *this) {
    free(this);
}

SymFileTransportManager * SymFileTransportManager_new(SymFileTransportManager *this, SymParameterService *parameterService) {
    if (this == NULL) {
        this = (SymFileTransportManager *) calloc(1, sizeof(SymFileTransportManager));
    }
    SymTransportManager *super = &this->super;
    super->parameterService = parameterService;
    super->sendAcknowledgement = (void *) &SymFileTransportManager_sendAcknowledgement;
    super->getPullTransport = (void *) &SymFileTransportManager_getPullTransport;
    super->getPushTransport = (void *) &SymFileTransportManager_getPushTransport;
    super->getFileSyncPullTransport = (void *) &SymFileTransportManager_getFileSyncPullTransport;
    super->getFileSyncPushTransport = (void *) &SymFileTransportManager_getFileSyncPushTransport;
    super->readAcknowledgement = (void *) SymFileTransportManager_readAcknowledgement;
    super->destroy = (void *) &SymFileTransportManager_destroy;
    return this;
}
