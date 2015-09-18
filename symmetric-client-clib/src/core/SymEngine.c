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

static unsigned short is_configured(SymEngine *this) {
    unsigned short isConfigured = 1;

    if (this->parameterService->get_registration_url(this->parameterService) == NULL) {
        printf("Please set the %s for the node\n", SYM_PARAMETER_REGISTRATION_URL);
        isConfigured = 0;
    }
    if (this->parameterService->get_node_group_id(this->parameterService) == NULL) {
        printf("Please set the %s for the node\n", SYM_PARAMETER_GROUP_ID);
        isConfigured = 0;
    }
    if (this->parameterService->get_external_id(this->parameterService) == NULL) {
        printf("Please set the %s for the node\n", SYM_PARAMETER_EXTERNAL_ID);
        isConfigured = 0;
    }

    return isConfigured;
}

unsigned short SymEngine_start(SymEngine *this) {
    unsigned short isStarted = 0;
	printf("About to start SymmetricDS\n");
    this->dialect->init_tables(this->dialect);

    if (is_configured(this)) {
        SymNode *node = this->nodeService->find_identity(this->nodeService);

        if (node != NULL) {
            if (strcmp(node->externalId, this->parameterService->get_external_id(this->parameterService)) != 0 ||
                    strcmp(node->nodeGroupId, this->parameterService->get_node_group_id(this->parameterService)) != 0) {
                printf("The configured state does not match recorded database state.  The recorded external id is %s while the configured external id is %s. The recorded node group id is %s while the configured node group id is %s",
                        node->externalId, this->parameterService->get_external_id(this->parameterService),
                        node->nodeGroupId, this->parameterService->get_node_group_id(this->parameterService));
            } else {
                printf("Starting registered node [group=%s, id=%s, externalId=%s]\n", node->nodeGroupId, node->nodeId, node->externalId);

                // TODO: if AUTO_SYNC_TRIGGERS_AT_STARTUP

                // TODO: if HEARTBEAT_SYNC_ON_STARTUP
            }
            node->destroy(node);
        } else {
            printf("Starting unregistered node [group=%s, externalId=%s]\n", this->parameterService->get_node_group_id(this->parameterService),
                    this->parameterService->get_external_id(this->parameterService));
        }

        printf("Started SymmetricDS\n");
        isStarted = 1;
    } else {
        printf("Did not start SymmetricDS.  It has not been configured properly\n");
    }

    printf("SymmetricDS: type=%s, name=, version=%s, groupId=%s, externalId=%s, databaseName=%s, databaseVersion=%s, driverName=, driverVersion=\n",
            SYM_DEPLOYMENT_TYPE, SYM_VERSION, this->parameterService->get_node_group_id(this->parameterService),
                this->parameterService->get_external_id(this->parameterService),
                this->platform->name, this->platform->version);
	return isStarted;
}

unsigned short SymEngine_stop(SymEngine *this) {
	printf("Stopping\n");
    printf("Stopping SymmetricDS externalId=%s version=%s database=%s",
            this->parameterService->get_external_id(this->parameterService), SYM_VERSION, this->platform->name);

	return 0;
}

unsigned short SymEngine_sync_triggers(SymEngine *this) {
    return this->triggerRouterService->sync_triggers(this->triggerRouterService->sync_triggers);
}

unsigned short SymEngine_uninstall(SymEngine *this) {
	printf("Un-installing\n");
	return 0;
}

void SymEngine_destroy(SymEngine *this) {
    this->triggerRouterService->destroy(this->triggerRouterService);
    this->dialect->destroy(this->dialect);
    this->platform->destroy(this->platform);
    if (this->dialect) {
        this->dialect->destroy(this->dialect);
    }
    free(this);
}

SymEngine * SymEngine_new(SymEngine *this, SymProperties *properties) {
    if (this == NULL) {
        this = (SymEngine *) calloc(1, sizeof(SymEngine));
    }
    this->start = (void *) &SymEngine_start;
    this->stop = (void *) &SymEngine_stop;
    this->uninstall = (void *) &SymEngine_uninstall;
    this->sync_triggers = (void *) &SymEngine_sync_triggers;
    this->destroy = (void *) &SymEngine_destroy;

    this->properties = properties;
    this->platform = SymDatabasePlatformFactory_create(properties);
    this->dialect = SymDialectFactory_create(this->platform);

    this->parameterService = SymParameterService_new(NULL, properties);
    this->triggerRouterService = SymTriggerRouterService_new(NULL);
    this->transportManager = SymTransportManagerFactory_create(SYM_PROTOCOL_HTTP, this->parameterService);
    this->nodeService = SymNodeService_new(NULL);
    this->dataLoaderService = SymDataLoaderService_new(NULL, this->parameterService, this->nodeService, this->transportManager, this->platform);
    this->registrationService = SymRegistrationService_new(NULL, this->nodeService, this->dataLoaderService, this->parameterService);
    this->pullService = SymPullService_new(NULL, this->nodeService, this->dataLoaderService, this->registrationService);
    this->pushService = SymPushService_new(NULL);

    return this;
}
