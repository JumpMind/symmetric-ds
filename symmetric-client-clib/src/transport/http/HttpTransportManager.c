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
#include "common/Log.h"

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
    if (SymStringUtils_isBlank(remote->syncUrl)) {
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
        if (iter->index > 0) {
            sb->append(sb, SYM_WEB_CONSTANTS_AND);
        }
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

static int sendMessage(SymHttpTransportManager *this, char *url, char *postData) {
    long httpResponseCode = -1;
    SymLog_debug("Sending message '%s' to URL '%s'", postData, url);
    CURL *curl = curl_easy_init();
    if (curl) {
        if (this->parameterService->is(this->parameterService, SYM_PARAMETER_HTTPS_VERIFIED_SERVERS, 1)) {
            curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0);
        }
        if (this->parameterService->is(this->parameterService, SYM_PARAMETER_HTTPS_ALLOW_SELF_SIGNED_CERTS, 1)) {
            curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0);
        }
        curl_easy_setopt(curl, CURLOPT_URL, url);
        curl_easy_setopt(curl, CURLOPT_POST, 1);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, postData);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, strlen(postData));
        CURLcode rc = curl_easy_perform(curl);
        if (rc != CURLE_OK) {
        	SymLog_error("Error %d, cannot retrieve %s", rc, url);
        	SymLog_error("%s", curl_easy_strerror(rc));
        } else {
            curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpResponseCode);
        }
        curl_easy_cleanup(curl);
    } else {
        SymLog_error("Cannot initialize curl.");
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
        httpResponseCode = sendMessage(this, url, ackData);
        sb->destroy(sb);
        free(ackData);
        free(url);
    }
    return httpResponseCode;
}

SymMap * SymHttpTransportManager_getParametersFromQueryUrl(char *parameterString) {
    SymMap *parameters = SymMap_new(NULL, 100);
    SymStringArray *tokens = SymStringArray_split(parameterString, "&");
    CURL *curl = curl_easy_init();
    int i;
    for (i = 0; i < tokens->size; i++) {
        char * token = tokens->get(tokens, i);
        if (!token) {
            continue;
        }
        SymStringArray *nameValuePair = SymStringArray_split(tokens->get(tokens, i), "=");
        if (nameValuePair->size == 2) {
            char *value = curl_easy_unescape(curl, nameValuePair->get(nameValuePair, 1), 0, NULL);
            SymStringUtils_replaceChar(value, '+', ' ');
            parameters->put(parameters, nameValuePair->get(nameValuePair, 0), value);
        }
    }
    curl_easy_cleanup(curl);
    return parameters;
}

static char * SymHttpTransportManager_getParam(SymMap *parameters, char *name, char *batchId) {
    char *key = SymStringUtils_format("%s%s", name, batchId);
    char *value = parameters->get(parameters, key);
    free(key);
    return value;
}

static int SymHttpTransportManager_getParamAsLong(SymMap *parameters, char *name, char *batchId) {
    long value = 0;
    char *str = SymHttpTransportManager_getParam(parameters, name, batchId);
    if (str) {
        value = atol(str);
    }
    return value;
}

SymBatchAck * SymHttpTransportManager_getBatchInfo(SymMap *parameters, char *batchId) {
    SymBatchAck *batchAck = SymBatchAck_new(NULL);
    batchAck->batchId = atol(batchId);
    char *nodeId = SymHttpTransportManager_getParam(parameters, SYM_WEB_CONSTANTS_ACK_NODE_ID, batchId);
    if (SymStringUtils_isBlank(nodeId)) {
        nodeId = parameters->get(parameters, SYM_WEB_CONSTANTS_NODE_ID);
    }
    batchAck->nodeId = nodeId;
    batchAck->networkMillis = SymHttpTransportManager_getParamAsLong(parameters, SYM_WEB_CONSTANTS_ACK_NETWORK_MILLIS, batchId);
    batchAck->filterMillis = SymHttpTransportManager_getParamAsLong(parameters, SYM_WEB_CONSTANTS_ACK_FILTER_MILLIS, batchId);
    batchAck->databaseMillis = SymHttpTransportManager_getParamAsLong(parameters, SYM_WEB_CONSTANTS_ACK_DATABASE_MILLIS, batchId);
    batchAck->byteCount = SymHttpTransportManager_getParamAsLong(parameters, SYM_WEB_CONSTANTS_ACK_BYTE_COUNT, batchId);
    batchAck->ignored = (unsigned short) SymHttpTransportManager_getParamAsLong(parameters, SYM_WEB_CONSTANTS_ACK_IGNORE_COUNT, batchId);
    char *status = SymHttpTransportManager_getParam(parameters, SYM_WEB_CONSTANTS_ACK_BATCH_NAME, batchId);
    batchAck->isOk = SymStringUtils_equalsIgnoreCase(status, SYM_WEB_CONSTANTS_ACK_BATCH_OK);

    if (!batchAck->isOk) {
        batchAck->errorLine = atol(status);
        batchAck->sqlState = SymHttpTransportManager_getParam(parameters, SYM_WEB_CONSTANTS_ACK_SQL_STATE, batchId);
        batchAck->sqlCode = (int) SymHttpTransportManager_getParamAsLong(parameters, SYM_WEB_CONSTANTS_ACK_SQL_CODE, batchId);
        batchAck->sqlMessage = SymHttpTransportManager_getParam(parameters, SYM_WEB_CONSTANTS_ACK_SQL_MESSAGE, batchId);
    }
    return batchAck;
}

SymList * SymHttpTransportManager_readAcknowledgement(SymHttpTransportManager *this, char *parameterString1, char *parameterString2) {
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

SymHttpIncomingTransport * SymHttpTransportManager_getPullTransport(SymHttpTransportManager *this, SymNode *remote, SymNode *local, char *securityToken, SymProperties *requestProperties, char *registrationUrl) {
    return SymHttpIncomingTransport_new(NULL, buildUrl("pull", remote, local, securityToken, registrationUrl), this->parameterService);
}

SymHttpOutgoingTransport * SymHttpTransportManager_getPushTransport(SymHttpTransportManager *this, SymNode *remote, SymNode *local, char *securityToken, char *registrationUrl) {
    return SymHttpOutgoingTransport_new(NULL, buildUrl("push", remote, local, securityToken, registrationUrl), this->parameterService);
}

SymHttpIncomingTransport * SymHttpTransportManager_getRegisterTransport(SymHttpTransportManager *this, SymNode *local, char *registrationUrl) {
    return SymHttpIncomingTransport_new(NULL, buildRegistrationUrl(local, registrationUrl), this->parameterService);
}

char * SymHttpTransportManager_strerror(long rc) {
    if (rc == SYM_TRANSPORT_OK) {
        return "OK";
    } else if (rc == SYM_TRANSPORT_REGISTRATION_NOT_OPEN) {
        return "Registration Not Open";
    } else if (rc == SYM_TRANSPORT_REGISTRATION_REQUIRED) {
        return "Registration Required";
    } else if (rc == SYM_TRANSPORT_SYNC_DISABLED) {
        return "Sync Disabled";
    } else if (rc == SYM_TRANSPORT_SC_SERVICE_UNAVAILABLE) {
        return "Service Unavailable";
    } else if (rc == SYM_TRANSPORT_SC_FORBIDDEN) {
        return "Forbidden, Authentication Required";
    } else if (rc == SYM_TRANSPORT_SC_ACCESS_DENIED) {
        return "Access Denied";
    } else if (rc == SYM_TRANSPORT_SC_SERVICE_BUSY) {
        return "Service Busy";
    }
    return "Unknown Error";
}

void SymHttpTransportManager_destroy(SymTransportManager *this) {
    free(this);
}

void SymHttpTransportManager_handleCurlRc(int curlRc, long httpCode, char* url, SymRemoteNodeStatus* status) {
    if (status == NULL) {
        status = SymRemoteNodeStatus_new(NULL, NULL, NULL);
    }

    if (curlRc != CURLE_OK) {
        SymLog_error("Error %d from curl, cannot retrieve %s", curlRc, url);
        SymLog_error("%s", curl_easy_strerror(curlRc));

        status->failed = 1;
        status->failureMessage = SymStringUtils_format("%s", curl_easy_strerror(curlRc));
        status->status = SYM_REMOTE_NODE_STATUS_OFFLINE;
    } else {
        status->status = httpCode;
        if (httpCode != SYM_TRANSPORT_OK) {
            status->failed = 1;
            status->failureMessage = SymHttpTransportManager_strerror(httpCode);
            if (httpCode != SYM_TRANSPORT_OK) {
                SymLog_error("HTTP response code of %ld, %s. URL: %s", httpCode, status->failureMessage, url);
            }
        }
    }
}

SymHttpTransportManager * SymHttpTransportManager_new(SymHttpTransportManager *this, SymParameterService *parameterService) {
    if (this == NULL) {
        this = (SymHttpTransportManager *) calloc(1, sizeof(SymHttpTransportManager));
    }
    this->parameterService = parameterService;
    SymTransportManager *super = &this->super;
    super->sendAcknowledgement = (void *) &SymHttpTransportManager_sendAcknowledgement;
    super->getPullTransport = (void *) &SymHttpTransportManager_getPullTransport;
    super->getPushTransport = (void *) &SymHttpTransportManager_getPushTransport;
    super->getRegisterTransport = (void *) &SymHttpTransportManager_getRegisterTransport;
    super->readAcknowledgement = (void *) SymHttpTransportManager_readAcknowledgement;
    super->destroy = (void *) &SymHttpTransportManager_destroy;
    return this;
}
