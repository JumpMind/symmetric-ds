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
#include "transport/http/HttpTransportManager.h"

static void append(SymStringBuilder *sb, char *name, char *value) {
    if (strstr(sb->str, SYM_WEB_CONSTANTS_QUERY) == NULL) {
        sb->append(sb, SYM_WEB_CONSTANTS_QUERY);
    } else {
        sb->append(sb, SYM_WEB_CONSTANTS_AND);
    }
    sb->append(sb, name);
    sb->append(sb, SYM_WEB_CONSTANTS_EQUALS);
    sb->append(sb, value);
}

static char * buildUrl(char *action, SymNode *remote, SymNode *local, char *securityToken, char *registrationUrl) {
    SymStringBuilder *sb = SymStringBuilder_new();
    if (strcmp(remote->syncUrl, "") == 0) {
        sb->append(sb, registrationUrl);
    } else {
        sb->append(sb, remote->syncUrl);
    }
    sb->append(sb, SYM_WEB_CONSTANTS_SLASH);
    sb->append(sb, action);
    append(sb, SYM_WEB_CONSTANTS_NODE_ID, local->nodeId);
    append(sb, SYM_WEB_CONSTANTS_SECURITY_TOKEN, securityToken);
    append(sb, SYM_WEB_CONSTANTS_HOST_NAME, SymAppUtils_getHostName());
    append(sb, SYM_WEB_CONSTANTS_IP_ADDRESS, SymAppUtils_getIpAddress());
    return sb->destroyAndReturn(sb);
}

static char * buildRegistrationUrl(SymNode *local, char *registrationUrl) {
    SymStringBuilder *sb = SymStringBuilder_newWithString(registrationUrl);
    sb->append(sb, "/registration");
    append(sb, SYM_WEB_CONSTANTS_NODE_GROUP_ID, local->nodeGroupId);
    append(sb, SYM_WEB_CONSTANTS_EXTERNAL_ID, local->externalId);
    append(sb, SYM_WEB_CONSTANTS_SYNC_URL, local->syncUrl);
    append(sb, SYM_WEB_CONSTANTS_SCHEMA_VERSION, local->schemaVersion);
    append(sb, SYM_WEB_CONSTANTS_DATABASE_TYPE, local->databaseType);
    append(sb, SYM_WEB_CONSTANTS_DATABASE_VERSION, local->databaseVersion);
    append(sb, SYM_WEB_CONSTANTS_SYMMETRIC_VERSION, local->symmetricVersion);
    append(sb, SYM_WEB_CONSTANTS_HOST_NAME, SymAppUtils_getHostName());
    append(sb, SYM_WEB_CONSTANTS_IP_ADDRESS, SymAppUtils_getIpAddress());
    return sb->destroyAndReturn(sb);
}

static char * getAcknowledgementData(SymList *batches) {
    SymStringBuilder *sb = SymStringBuilder_new();
    SymIterator *iter = batches->iterator(batches);
    while (iter->hasNext(iter)) {
        SymIncomingBatch *batch = (SymIncomingBatch *) iter->next(iter);
        sb->append(sb, SYM_WEB_CONSTANTS_ACK_BATCH_NAME);
        sb->appendf(sb, "%ld", batch->batchId);
        sb->append(sb, SYM_WEB_CONSTANTS_EQUALS);

        if (strcmp(batch->status, SYM_INCOMING_BATCH_STATUS_OK) == 0 ||
                strcmp(batch->status, SYM_INCOMING_BATCH_STATUS_IGNORED) == 0) {
            sb->append(sb, SYM_WEB_CONSTANTS_ACK_BATCH_OK);
        } else {
            sb->appendf(sb, "%ld", batch->failedRowNumber);
        }
    }
    iter->destroy(iter);
    return sb->destroyAndReturn(sb);
}

static int sendMessage(char *url, char *postData) {
    long httpResponseCode = -1;
    printf("Sending message '%s' to URL '%s'\n", postData, url);
    CURL *curl = curl_easy_init();
    if (curl) {
        curl_easy_setopt(curl, CURLOPT_URL, url);
        curl_easy_setopt(curl, CURLOPT_POST, 1);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, postData);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, strlen(postData));
        CURLcode rc = curl_easy_perform(curl);
        if (rc != CURLE_OK) {
            fprintf(stderr, "Error %d, cannot retrieve %s\n", rc, url);
            fprintf(stderr, "%s", curl_easy_strerror(rc));
        } else {
            curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpResponseCode);
        }
        curl_easy_cleanup(curl);
    } else {
        fprintf(stderr, "Error cannot initialize curl\n");
    }
    return httpResponseCode;
}

int SymHttpTransportManager_sendAcknowledgement(SymHttpTransportManager *this, SymNode *remote, SymList *batches, SymNode *local, char *securityToken, char *registrationUrl) {
    long httpResponseCode = 0;
    if (batches->size > 0) {
        char *url = buildUrl("ack", remote, local, securityToken, registrationUrl);
        char *ackData = getAcknowledgementData(batches);
        SymStringBuilder *sb = SymStringBuilder_newWithString(url);
        sb->append(sb, ackData);
        sendMessage(url, ackData);
        sb->destroy(sb);
        free(ackData);
        free(url);
    }
    return httpResponseCode;
}

SymBatchAck * SymHttpTransportManager_readAcknowledgement(SymHttpTransportManager *this, char *parameterString1, char *parameterString2) {
    return NULL;
}

SymHttpIncomingTransport * SymHttpTransportManager_getPullTransport(SymHttpTransportManager *this, SymNode *remote, SymNode *local, char *securityToken, SymProperties *requestProperties, char *registrationUrl) {
    return SymHttpIncomingTransport_new(NULL, buildUrl("pull", remote, local, securityToken, registrationUrl));
}

SymHttpOutgoingTransport * SymHttpTransportManager_getPushTransport(SymHttpTransportManager *this, SymNode *remote, SymNode *local, char *securityToken, char *registrationUrl) {
    return SymHttpOutgoingTransport_new(NULL, buildUrl("push", remote, local, securityToken, registrationUrl));
}

SymHttpIncomingTransport * SymHttpTransportManager_getRegisterTransport(SymHttpTransportManager *this, SymNode *local, char *registrationUrl) {
    return SymHttpIncomingTransport_new(NULL, buildRegistrationUrl(local, registrationUrl));
}

void SymHttpTransportManager_destroy(SymTransportManager *this) {
    free(this);
}

SymHttpTransportManager * SymHttpTransportManager_new(SymHttpTransportManager *this, SymParameterService *parameterService) {
    if (this == NULL) {
        this = (SymHttpTransportManager *) calloc(1, sizeof(SymHttpTransportManager));
    }
    SymTransportManager *super = &this->super;
    super->sendAcknowledgement = (void *) &SymHttpTransportManager_sendAcknowledgement;
    super->readAcknowledgement = (void *) &SymHttpTransportManager_readAcknowledgement;
    super->getPullTransport = (void *) &SymHttpTransportManager_getPullTransport;
    super->getPushTransport = (void *) &SymHttpTransportManager_getPushTransport;
    super->getRegisterTransport = (void *) &SymHttpTransportManager_getRegisterTransport;
    super->destroy = (void *) &SymHttpTransportManager_destroy;
    return this;
}
