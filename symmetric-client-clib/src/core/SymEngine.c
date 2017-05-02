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
#include "core/SymEngine.h"

static unsigned short SymEngine_isConfigured(SymEngine *this) {
    unsigned short isConfigured = 1;

    if (this->parameterService->getRegistrationUrl(this->parameterService) == NULL) {
        SymLog_warn("Please set the %s for the node", SYM_PARAMETER_REGISTRATION_URL);
        isConfigured = 0;
    }
    if (this->parameterService->getNodeGroupId(this->parameterService) == NULL) {
    	SymLog_warn("Please set the %s for the node", SYM_PARAMETER_GROUP_ID);
        isConfigured = 0;
    }
    if (this->parameterService->getExternalId(this->parameterService) == NULL) {
    	SymLog_warn("Please set the %s for the node", SYM_PARAMETER_EXTERNAL_ID);
        isConfigured = 0;
    }

    return isConfigured;
}

unsigned short SymEngine_start(SymEngine *this) {
    unsigned short isStarted = 0;

    SymLog_configure(this->properties);

    SymLog_info("About to start SymmetricDS");

    this->dialect->initTablesAndDatabaseObjects(this->dialect);
    this->sequenceService->init(this->sequenceService);

    if (SymEngine_isConfigured(this)) {
        SymNode *node = this->nodeService->findIdentity(this->nodeService);

        if (node != NULL) {

            if (strcmp(node->externalId, this->parameterService->getExternalId(this->parameterService)) != 0 ||
                    strcmp(node->nodeGroupId, this->parameterService->getNodeGroupId(this->parameterService)) != 0) {
            	SymLog_error("The configured state does not match recorded database state.  The recorded external id is %s while the configured external id is %s. The recorded node group id is %s while the configured node group id is %s",
                        node->externalId, this->parameterService->getExternalId(this->parameterService),
                        node->nodeGroupId, this->parameterService->getNodeGroupId(this->parameterService));
            } else {
            	SymLog_info("Starting registered node [group=%s, id=%s, externalId=%s]", node->nodeGroupId, node->nodeId, node->externalId);

                if (this->parameterService->is(this->parameterService, SYM_PARAMETER_AUTO_SYNC_TRIGGERS_AT_STARTUP, 1)) {
                	this->triggerRouterService->syncTriggers(this->triggerRouterService, 0);
                }
                else {
                	SymLog_info("%s is turned off.", SYM_PARAMETER_AUTO_SYNC_TRIGGERS_AT_STARTUP);
                }

                // TODO: if HEARTBEAT_SYNC_ON_STARTUP
            }
        } else {
        	SymLog_info("Starting unregistered node [group=%s, externalId=%s]", this->parameterService->getNodeGroupId(this->parameterService),
                    this->parameterService->getExternalId(this->parameterService));
        }

        SymLog_info("Started SymmetricDS");
        isStarted = 1;
    } else {
    	SymLog_error("Did not start SymmetricDS.  It has not been configured properly");
    }

    SymLog_info("SymmetricDS: type=%s, name=, version=%s, groupId=%s, externalId=%s, databaseName=%s, databaseVersion=%s, driverName=, driverVersion=",
            SYM_DEPLOYMENT_TYPE, SYM_VERSION, this->parameterService->getNodeGroupId(this->parameterService),
                this->parameterService->getExternalId(this->parameterService),
                this->platform->name, this->platform->version);
	return isStarted;
}

unsigned short SymEngine_stop(SymEngine *this) {
	SymLog_info("Stopping SymmetricDS externalId=%s version=%s database=%s",
            this->parameterService->getExternalId(this->parameterService), SYM_VERSION, this->platform->name);

	return 0;
}

void SymEngine_syncTriggers(SymEngine *this) {
    this->triggerRouterService->syncTriggers(this->triggerRouterService, 0);
}

SymRemoteNodeStatuses * SymEngine_push(SymEngine *this) {
    return this->pushService->pushData(this->pushService);
}

SymRemoteNodeStatuses * SymEngine_pull(SymEngine *this) {
    return this->pullService->pullData(this->pullService);
}

void SymEngine_route(SymEngine *this) {
    this->routerService->routeData(this->routerService);
}

void SymEngine_purge(SymEngine *this) {
    this->purgeService->purgeOutgoing(this->purgeService);
    this->purgeService->purgeIncoming(this->purgeService);
}

void SymEngine_heartbeat(SymEngine *this, unsigned short force) {
    this->dataService->heartbeat(this->dataService, force);
}

unsigned short SymEngine_uninstall(SymEngine *this) {
	SymLog_warn("Un-installing");
	return 0;
}

void SymEngine_destroy(SymEngine *this) {
    this->triggerRouterService->destroy(this->triggerRouterService);
    this->dialect->destroy(this->dialect);
    this->purgeService->destroy(this->purgeService);
    this->pushService->destroy(this->pushService);
    this->pullService->destroy(this->pullService);
    this->offlinePushService->destroy(this->offlinePushService);
    this->offlinePullService->destroy(this->offlinePullService);
    this->routerService->destroy(this->routerService);
    this->registrationService->destroy(this->registrationService);
    this->configurationService->destroy(this->configurationService);
    this->platform->destroy(this->platform);
    this->dataLoaderService->destroy(this->dataLoaderService);
    this->transportManager->destroy(this->transportManager);
    this->offlineTransportManager->destroy(this->offlineTransportManager);
    this->incomingBatchService->destroy(this->incomingBatchService);
    this->outgoingBatchService->destroy(this->outgoingBatchService);
    this->acknowledgeService->destroy(this->acknowledgeService);
    this->dataExtractorService->destroy(this->dataExtractorService);
    this->dataService->destroy(this->dataService);
    this->nodeService->destroy(this->nodeService);
    this->parameterService->destroy(this->parameterService);
    this->sequenceService->destroy(this->sequenceService);
    free(this);
}

SymEngine * SymEngine_new( SymEngine *this, SymProperties *properties) {
    if (this == NULL) {
        this = (SymEngine *) calloc(1, sizeof(SymEngine));
    }
    this->start = (void *) &SymEngine_start;
    this->stop = (void *) &SymEngine_stop;
    this->uninstall = (void *) &SymEngine_uninstall;
    this->syncTriggers = (void *) &SymEngine_syncTriggers;
    this->push = (void *) &SymEngine_push;
    this->pull = (void *) &SymEngine_pull;
    this->route = (void *) &SymEngine_route;
    this->purge = (void *) &SymEngine_purge;
    this->heartbeat = (void *) &SymEngine_heartbeat;
    this->destroy = (void *) &SymEngine_destroy;

    this->properties = properties;
    this->platform = SymDatabasePlatformFactory_create(properties);
    this->dialect = SymDialectFactory_create(this->platform);

    this->sequenceService = SymSequenceService_new(NULL, this->platform);

    this->parameterService = SymParameterService_new(NULL, properties, this->platform);
    this->configurationService = SymConfigurationService_new(NULL, this->parameterService, this->platform);

    this->triggerRouterService = SymTriggerRouterService_new(NULL, this->configurationService,
            this->sequenceService, this->parameterService, this->platform, this->dialect);
    this->transportManager = SymTransportManagerFactory_create(SYM_PROTOCOL_HTTP, this->parameterService);
    this->offlineTransportManager = SymTransportManagerFactory_create(SYM_PROTOCOL_FILE, this->parameterService);
    this->nodeService = SymNodeService_new(NULL, this->platform);
    this->nodeService->lastRestartTime = SymDate_new(NULL);
    this->nodeCommunicationService = SymNodeCommunicationService_new(NULL,  this->nodeService, this->parameterService);
    this->incomingBatchService = SymIncomingBatchService_new(NULL, this->platform, this->parameterService);
    this->outgoingBatchService = SymOutgoingBatchService_new(NULL, this->platform, this->parameterService, this->sequenceService);
    this->acknowledgeService = SymAcknowledgeService_new(NULL, this->outgoingBatchService, this->platform);
    this->dataLoaderService = SymDataLoaderService_new(NULL, this->parameterService, this->nodeService, this->triggerRouterService,
            this->transportManager, this->offlineTransportManager, this->platform, this->dialect, this->incomingBatchService);
    this->dataService = SymDataService_new(NULL, this->platform, this->triggerRouterService, this->nodeService, this->dialect, this->outgoingBatchService, this->parameterService);
    this->routerService = SymRouterService_new(NULL, this->outgoingBatchService, this->sequenceService, this->dataService, this->nodeService, this->configurationService,
            this->parameterService, this->triggerRouterService, this->platform);
    this->dataExtractorService = SymDataExtractorService_new(NULL, this->nodeService, this->outgoingBatchService, this->dataService,
            this->triggerRouterService, this->parameterService, this->configurationService, this->platform);
    this->registrationService = SymRegistrationService_new(NULL, this->nodeService, this->dataLoaderService, this->parameterService,
            this->configurationService, this->dataService);
    this->pullService = SymPullService_new(NULL, this->nodeService, this->dataLoaderService, this->registrationService,
            this->configurationService, this->nodeCommunicationService);
    this->pushService = SymPushService_new(NULL, this->nodeService, this->dataExtractorService, this->transportManager, this->parameterService,
            this->configurationService, this->acknowledgeService, this->nodeCommunicationService);
    this->offlinePullService = SymOfflinePullService_new(NULL, this->nodeService,
            this->dataLoaderService, this->registrationService, this->configurationService, this->nodeCommunicationService);
    this->offlinePushService = SymOfflinePushService_new(NULL, this->nodeService,
            this->dataExtractorService, this->offlineTransportManager, this->parameterService,
            this->configurationService, this->acknowledgeService, this->nodeCommunicationService);
    this->purgeService = SymPurgeService_new(NULL, this->parameterService, this->dialect, this->platform);
    this->fileSyncService = SymFileSyncService_new(NULL, this);

    return this;
}
