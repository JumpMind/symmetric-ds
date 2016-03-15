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
#include "common/Log.h"

void SymRegistrationService_registerWithServer(SymRegistrationService *this) {
    unsigned short isRegistered = this->isRegisteredWithServer(this);

    if (!this->isRegisteredWithServer(this)) {

        int maxNumberAttempts = this->parameterService->getInt(this->parameterService, SYM_PARAMETER_REGISTRATION_NUMBER_OF_ATTEMPTS, -1);
        SymMap *channels = this->configurationService->getChannels(this->configurationService, 0);
        SymRemoteNodeStatus *status = SymRemoteNodeStatus_new(NULL, NULL, channels);

        while (!isRegistered && (maxNumberAttempts < 0 || maxNumberAttempts > 0)) {

            this->dataLoaderService->loadDataFromRegistration(this->dataLoaderService, status);
            isRegistered = status->status == SYM_REMOTE_NODE_STATUS_DATA_PROCESSED;
            maxNumberAttempts--;

            if (isRegistered) {
                SymNode *node = this->nodeService->findIdentity(this->nodeService);
                if (node != NULL) {
                	SymLog_info("Successfully registered node [id=%s]", node->nodeId);
                    this->dataService->heartbeat(this->dataService, 1);
                } else {
                    SymLog_error("Node identity is missing after registration.  The registration server may be misconfigured or have an error");
                    isRegistered = 0;
                }
            }

            if (!isRegistered && maxNumberAttempts != 0) {
                unsigned int sleepTimeInSec = 5;
                SymLog_debug("Could not register.  Sleeping before attempting again.");
                SymLog_debug("Sleeping for %d seconds", sleepTimeInSec);
                sleep(sleepTimeInSec);
            }
        }

        if (!isRegistered) {
            int maxNumberAttempts = this->parameterService->getInt(this->parameterService, SYM_PARAMETER_REGISTRATION_NUMBER_OF_ATTEMPTS, -1);
            SymLog_error("Failed after trying to register %d times.", maxNumberAttempts);
        }

        channels->destroy(channels);
        status->destroy(status);
    }
}

unsigned short SymRegistrationService_isRegisteredWithServer(SymRegistrationService *this) {
    return this->nodeService->findIdentity(this->nodeService) != NULL;
}

void SymRegistrationService_destroy(SymRegistrationService *this) {
    free(this);
}

SymRegistrationService * SymRegistrationService_new(SymRegistrationService *this, SymNodeService *nodeService,
        SymDataLoaderService *dataLoaderService, SymParameterService *parameterService,
        SymConfigurationService *configurationService, SymDataService *dataService) {
    if (this == NULL) {
        this = (SymRegistrationService *) calloc(1, sizeof(SymRegistrationService));
    }
    this->nodeService = nodeService;
    this->dataLoaderService = dataLoaderService;
    this->parameterService = parameterService;
    this->configurationService = configurationService;
    this->dataService = dataService;
    this->isRegisteredWithServer = (void *) &SymRegistrationService_isRegisteredWithServer;
    this->registerWithServer = (void *) &SymRegistrationService_registerWithServer;
    this->destroy = (void *) &SymRegistrationService_destroy;
    return this;
}
