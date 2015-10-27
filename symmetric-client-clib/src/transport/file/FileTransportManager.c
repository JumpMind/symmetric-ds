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
//    long httpResponseCode = 0;
//    if (batches->size > 0) {
//        char *url = buildUrl("ack", remote, local, securityToken, registrationUrl);
//        char *ackData = getAcknowledgementData(batches);
//        SymStringBuilder *sb = SymStringBuilder_newWithString(url);
//        sb->append(sb, ackData);
//        httpResponseCode = sendMessage(url, ackData);
//        sb->destroy(sb);
//        free(ackData);
//        free(url);
//    }
//    return httpResponseCode;

    return 0;
}

SymFileIncomingTransport * SymFileTransportManager_getPullTransport(SymFileTransportManager *this, SymNode *remote, SymNode *local, char *securityToken, SymProperties *requestProperties, char *registrationUrl) {
    // return SymFileIncomingTransport_new(NULL, buildUrl("pull", remote, local, securityToken, registrationUrl));
    return 0;
}

SymFileOutgoingTransport * SymFileTransportManager_getPushTransport(SymFileTransportManager *this, SymNode *remote, SymNode *local, char *securityToken, char *registrationUrl) {
    // return SymFileOutgoingTransport_new(NULL, buildUrl("push", remote, local, securityToken, registrationUrl));
    return 0;
}

//SymHttpIncomingTransport * SymFileTransportManager_getRegisterTransport(SymFileTransportManager *this, SymNode *local, char *registrationUrl) {
//    return SymHttpIncomingTransport_new(NULL, buildRegistrationUrl(local, registrationUrl));
//}

SymList * SymFileTransportManager_readAcknowledgement(SymFileTransportManager *this, char *parameterString1, char *parameterString2) {
//    SymStringBuilder *sb = SymStringBuilder_newWithString(parameterString1);
//    sb->append(sb, "&")->append(sb, parameterString2);
//
//    SymMap *parameters = SymFileTransportManager_getParametersFromQueryUrl(parameterString1);
//    SymList *batchAcks = SymList_new(NULL);
//    SymStringArray *keys = parameters->keys(parameters);
//    int i;
//    for (i = 0; i < keys->size; i++) {
//        char *key = keys->get(keys, i);
//        if (strncmp(key, SYM_WEB_CONSTANTS_ACK_BATCH_NAME, strlen(SYM_WEB_CONSTANTS_ACK_BATCH_NAME)) == 0) {
//            char *batchId = SymStringUtils_substring(key, strlen(SYM_WEB_CONSTANTS_ACK_BATCH_NAME), strlen(key));
//            SymBatchAck *batchAck = SymFileTransportManager_getBatchInfo(parameters, batchId);
//            batchAcks->add(batchAcks, batchAck);
//            free(batchId);
//        }
//    }
//    keys->destroy(keys);
//    parameters->destroy(parameters);
//    sb->destroy(sb);
//    return batchAcks;

    return NULL;
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
//    super->getRegisterTransport = (void *) &SymHttpTransportManager_getRegisterTransport;
    super->readAcknowledgement = (void *) SymFileTransportManager_readAcknowledgement;
    this->destroy = (void *) &SymFileTransportManager_destroy;
    return this;
}
