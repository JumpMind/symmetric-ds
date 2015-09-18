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
#include "service/RegistrationService.h"

void SymRegistrationService_register_with_server(SymRegistrationService *this) {
    unsigned short isRegistered = this->is_registered_with_server(this);

    if (!this->is_registered_with_server(this)) {

        int maxNumberAttempts = this->parameterService->get_int(this->parameterService, SYM_PARAMETER_REGISTRATION_NUMBER_OF_ATTEMPTS, -1);
        SymRemoteNodeStatus *status = SymRemoteNodeStatus_new(NULL);

        while (!isRegistered && (maxNumberAttempts < 0 || maxNumberAttempts > 0)) {

            this->dataLoaderService->load_data_from_registration(this->dataLoaderService, status);
            maxNumberAttempts--;

            if (isRegistered) {
                SymNode *node = this->nodeService->find_identity(this->nodeService);
                if (node != NULL) {
                    printf("Successfully registered node [id=%s]\n", node->nodeId);
                    // TODO: this->dataService->heartbeat(this->dataService);
                    node->destroy(node);
                } else {
                    printf("Node identity is missing after registration.  The registration server may be misconfigured or have an error\n");
                    isRegistered = 0;
                }
            }

            if (!isRegistered && maxNumberAttempts != 0) {
                unsigned int sleepTimeInSec = 5;
                printf("Could not register.  Sleeping before attempting again.\n");
                printf("Sleeping for %d seconds\n", sleepTimeInSec);
                sleep(sleepTimeInSec);
            }
        }

        if (!isRegistered) {
            int maxNumberAttempts = this->parameterService->get_int(this->parameterService, SYM_PARAMETER_REGISTRATION_NUMBER_OF_ATTEMPTS, -1);
            printf("Failed after trying to register %d times.", maxNumberAttempts);
        }

        status->destroy(status);
    }
}

unsigned short SymRegistrationService_is_registered_with_server(SymRegistrationService *this) {
    SymNode *identity = this->nodeService->find_identity(this->nodeService);
    unsigned short isRegistered = identity != NULL;
    if (identity != NULL) {
        identity->destroy(identity);
    }
    return isRegistered;
}

void SymRegistrationService_destroy(SymRegistrationService *this) {
    free(this);
}

SymRegistrationService * SymRegistrationService_new(SymRegistrationService *this, SymNodeService *nodeService, SymDataLoaderService *dataLoaderService,
        SymParameterService *parameterService) {
    if (this == NULL) {
        this = (SymRegistrationService *) calloc(1, sizeof(SymRegistrationService));
    }
    this->nodeService = nodeService;
    this->dataLoaderService = dataLoaderService;
    this->parameterService = parameterService;
    this->is_registered_with_server = (void *) &SymRegistrationService_is_registered_with_server;
    this->register_with_server = (void *) &SymRegistrationService_register_with_server;
    this->destroy = (void *) &SymRegistrationService_destroy;
    return this;
}
