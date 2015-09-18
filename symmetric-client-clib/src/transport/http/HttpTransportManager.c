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
    if (strstr(sb->to_string(sb), SYM_WEB_CONSTANTS_QUERY) == NULL) {
        sb->append(sb, SYM_WEB_CONSTANTS_QUERY);
    } else {
        sb->append(sb, SYM_WEB_CONSTANTS_AND);
    }
    sb->append(sb, name);
    sb->append(sb, SYM_WEB_CONSTANTS_EQUALS);
    sb->append(sb, value);
}

static char * build_url(char *action, SymNode *remote, SymNode *local, char *securityToken, char *registrationUrl) {
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
    //append(sb, SYM_WEB_CONSTANTS_HOST_NAME, "todo-host");
    //append(sb, SYM_WEB_CONSTANTS_IP_ADDRESS, "todo-ipapddr");
    return sb->destroy_and_return(sb);
}

static char * build_registration_url(SymNode *local, char *registrationUrl) {
    SymStringBuilder *sb = SymStringBuilder_new_with_string(registrationUrl);
    sb->append(sb, "/registration");
    append(sb, SYM_WEB_CONSTANTS_NODE_GROUP_ID, local->nodeGroupId);
    append(sb, SYM_WEB_CONSTANTS_EXTERNAL_ID, local->externalId);
    append(sb, SYM_WEB_CONSTANTS_SYNC_URL, local->syncUrl);
    append(sb, SYM_WEB_CONSTANTS_SCHEMA_VERSION, local->schemaVersion);
    append(sb, SYM_WEB_CONSTANTS_DATABASE_TYPE, local->databaseType);
    append(sb, SYM_WEB_CONSTANTS_DATABASE_VERSION, local->databaseVersion);
    append(sb, SYM_WEB_CONSTANTS_SYMMETRIC_VERSION, local->symmetricVersion);
    //append(sb, SYM_WEB_CONSTANTS_HOST_NAME, "todo-host");
    //append(sb, SYM_WEB_CONSTANTS_IP_ADDRESS, "todo-ipaddr");
    return sb->destroy_and_return(sb);
}

static char * get_acknowledgement_data(SymIncomingBatch **batches) {
    SymStringBuilder *sb = SymStringBuilder_new();
    int i;
    for (i = 0; batches[i] != NULL; i++) {
        sb->append(sb, SYM_WEB_CONSTANTS_ACK_BATCH_NAME);
        sb->appendf(sb, "%l", batches[i]->batchId);
        sb->append(sb, SYM_WEB_CONSTANTS_EQUALS);

        if (strcmp(batches[i]->status, SYM_INCOMING_BATCH_STATUS_OK) == 0 ||
                strcmp(batches[i]->status, SYM_INCOMING_BATCH_STATUS_IGNORED) == 0) {
            sb->append(sb, SYM_WEB_CONSTANTS_ACK_BATCH_OK);
        } else {
            sb->appendf(sb, "%l", batches[i]->failedRowNumber);
        }
    }
    return sb->destroy_and_return(sb);
}

static int send_message(char *url, char *postData) {
    long httpResponseCode = -1;
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

int SymHttpTransportManager_send_acknowledgement(SymHttpTransportManager *this, SymNode *remote, SymIncomingBatch **batches, SymNode *local, char *securityToken, char *registrationUrl) {
    long httpResponseCode = 0;
    if (batches[0] != NULL) {
        char *url = build_url("ack", remote, local, securityToken, registrationUrl);
        char *ackData = get_acknowledgement_data(batches);
        SymStringBuilder *sb = SymStringBuilder_new_with_string(url);
        sb->append(sb, ackData);
        send_message(url, ackData);
        sb->destroy(sb);
        free(ackData);
        free(url);
    }
    return httpResponseCode;
}

SymBatchAck * SymHttpTransportManager_read_acknowledgement(SymHttpTransportManager *this, char *parameterString1, char *parameterString2) {
    return NULL;
}

SymHttpIncomingTransport * SymHttpTransportManager_get_pull_transport(SymHttpTransportManager *this, SymNode *remote, SymNode *local, char *securityToken, SymProperties *requestProperties, char *registrationUrl) {
    return SymHttpIncomingTransport_new(NULL, build_url("pull", remote, local, securityToken, registrationUrl));
}

SymHttpOutgoingTransport * SymHttpTransportManager_get_push_transport(SymHttpTransportManager *this, SymNode *remote, SymNode *local, char *securityToken, char *registrationUrl) {
    return SymHttpOutgoingTransport_new(NULL, build_url("push", remote, local, securityToken, registrationUrl));
}

SymHttpIncomingTransport * SymHttpTransportManager_get_register_transport(SymHttpTransportManager *this, SymNode *local, char *registrationUrl) {
    return SymHttpIncomingTransport_new(NULL, build_registration_url(local, registrationUrl));
}

void SymHttpTransportManager_destroy(SymTransportManager *this) {
    free(this);
}

SymHttpTransportManager * SymHttpTransportManager_new(SymHttpTransportManager *this, SymParameterService *parameterService) {
    if (this == NULL) {
        this = (SymHttpTransportManager *) calloc(1, sizeof(SymHttpTransportManager));
    }
    SymTransportManager *super = &this->super;
    super->send_acknowledgement = (void *) &SymHttpTransportManager_send_acknowledgement;
    super->read_acknowledgement = (void *) &SymHttpTransportManager_read_acknowledgement;
    super->get_pull_transport = (void *) &SymHttpTransportManager_get_pull_transport;
    super->get_push_transport = (void *) &SymHttpTransportManager_get_push_transport;
    super->get_register_transport = (void *) &SymHttpTransportManager_get_register_transport;
    super->destroy = (void *) &SymHttpTransportManager_destroy;
    return this;
}
