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
#include "service/DataLoaderService.h"

static void send_ack(SymDataLoaderService *this, SymNode *remote, SymNode *local, SymNodeSecurity *localSecurity,
        SymIncomingBatch **incomingBatches) {
    int sendAck = -1;
    int numberOfStatusSendRetries = this->parameterService->get_int(this->parameterService,
            SYM_PARAMETER_DATA_LOADER_NUM_OF_ACK_RETRIES, 5);
    char *registrationUrl = this->parameterService->get_registration_url(this->parameterService);
    int sleepSeconds = this->parameterService->get_int(this->parameterService, SYM_PARAMETER_DATA_LOADER_TIME_BETWEEN_ACK_RETRIES, 5);

    int i = 0;
    for (; i < numberOfStatusSendRetries && sendAck != HTTP_OK; i++) {
        sendAck = this->transportManager->send_acknowledgement(this->transportManager,
                    remote, incomingBatches, local, localSecurity->nodePassword, registrationUrl);
        if (sendAck != 0) {
            printf("Ack was not sent successfully on try number %d.\n", i + 1);
            if (i < numberOfStatusSendRetries - 1) {
                sleep(sleepSeconds);
            }
        }
    }
}

static SymIncomingBatch ** load_data_from_transport(SymDataLoaderService *this, SymNode *remote, SymIncomingTransport *transport, int *error) {
    // TODO:
    SymDataWriter *writer = (SymDataWriter *) SymDefaultDatabaseWriter_new(NULL);
    SymDataReader *reader = (SymDataReader *) SymProtocolDataReader_new(NULL, remote->nodeId, writer);

    long rc = transport->process(transport, reader);

    reader->destroy(reader);
    writer->destroy(writer);
    return NULL;
}

void SymDataLoaderService_load_data_from_registration(SymDataLoaderService *this, SymRemoteNodeStatus *status) {
    SymNode *local = SymNode_new(NULL);
    local->nodeGroupId = this->parameterService->get_node_group_id(this->parameterService);
    local->externalId = this->parameterService->get_external_id(this->parameterService);
    local->databaseType = this->platform->name;
    local->databaseVersion = this->platform->version;
    local->syncUrl = this->parameterService->get_sync_url(this->parameterService);
    local->schemaVersion = this->parameterService->get_string(this->parameterService, SYM_PARAMETER_SCHEMA_VERSION, "");

    char *registrationUrl = this->parameterService->get_registration_url(this->parameterService);
    SymIncomingTransport *transport = this->transportManager->get_register_transport(this->transportManager, local, registrationUrl);
    printf("Using registration URL of %s\n", transport->get_url(transport));

    SymNode *remote = SymNode_new(NULL);
    remote->syncUrl = registrationUrl;

    int error = 0;
    SymIncomingBatch **incomingBatches = load_data_from_transport(this, remote, transport, &error);
    if (incomingBatches != NULL) {
        status->update_incoming_status(status, incomingBatches);
        local->destroy(local);
        local = this->nodeService->find_identity(this->nodeService);
        if (local != NULL) {
            SymNodeSecurity *localSecurity = this->nodeService->find_node_security(this->nodeService, local->nodeId);
            send_ack(this, remote, local, localSecurity, incomingBatches);
            localSecurity->destroy(localSecurity);
        }
        incomingBatches[0]->destroy_all((void **) incomingBatches);
    }

    local->destroy(local);
    remote->destroy(remote);
    transport->destroy(transport);
}

void SymDataLoaderService_load_data_from_pull(SymDataLoaderService *this, SymNode *remote, SymRemoteNodeStatus *status) {
    SymNode *local = this->nodeService->find_identity(this->nodeService);
    if (local == NULL) {
        this->load_data_from_registration(this, status);
    } else {
        SymNodeSecurity *localSecurity = this->nodeService->find_node_security(this->nodeService, local->nodeId);
        char *registrationUrl = this->parameterService->get_registration_url(this->parameterService);
        SymIncomingTransport *transport = this->transportManager->get_pull_transport(this->transportManager, remote, local,
                    localSecurity->nodePassword, NULL, registrationUrl);

        int error = 0;
        SymIncomingBatch **incomingBatches = load_data_from_transport(this, remote, transport, &error);
        if (incomingBatches != NULL) {
            status->update_incoming_status(status, incomingBatches);
            send_ack(this, remote, local, localSecurity, incomingBatches);
            incomingBatches[0]->destroy_all((void **) incomingBatches);
        }
        if (error == 1) {
            printf("Node information missing on the server.  Attempting to re-register.\n");
            this->load_data_from_registration(this, status);
        }

        transport->destroy(transport);
        localSecurity->destroy(localSecurity);
        local->destroy(local);
    }
}

void SymDataLoaderService_destroy(SymDataLoaderService *this) {
    free(this);
}

SymDataLoaderService * SymDataLoaderService_new(SymDataLoaderService *this, SymParameterService *parameterService,
        SymNodeService *nodeService, SymTransportManager *transportManager, SymDatabasePlatform *platform) {
    if (this == NULL) {
        this = (SymDataLoaderService *) calloc(1, sizeof(SymDataLoaderService));
    }
    this->parameterService = parameterService;
    this->nodeService = nodeService;
    this->transportManager = transportManager;
    this->platform = platform;

    this->load_data_from_pull = (void *) &SymDataLoaderService_load_data_from_pull;
    this->load_data_from_registration = (void *) &SymDataLoaderService_load_data_from_registration;
    this->destroy = (void *) &SymDataLoaderService_destroy;
    return this;
}
