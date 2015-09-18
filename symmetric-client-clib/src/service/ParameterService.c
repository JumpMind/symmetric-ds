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
#include "service/ParameterService.h"

static void get_database_parameters(SymParameterService *this, char *externalId, char *nodeGroupId) {
}

static void reread_database_parameters(SymParameterService *this) {
    if (this->parameters != NULL) {
        this->parameters->destroy(this->parameters);
    }
    this->parameters = SymProperties_new(NULL);
    this->parameters->put_all(this->parameters, this->properties);
    get_database_parameters(this, SYM_PARAMETER_ALL, SYM_PARAMETER_ALL);
    get_database_parameters(this, SYM_PARAMETER_ALL, this->properties->get(this->properties, SYM_PARAMETER_GROUP_ID, NULL));
    get_database_parameters(this, this->properties->get(this->properties, SYM_PARAMETER_EXTERNAL_ID, NULL),
            this->properties->get(this->properties, SYM_PARAMETER_GROUP_ID, NULL));
}

static SymProperties * get_parameters(SymParameterService *this) {
    time_t now = time(NULL);
    if (this->parameters == NULL || (this->cacheTimeoutInMs > 0 && this->lastTimeParameterWereCached < (now - (this->cacheTimeoutInMs / 1000)))) {
        reread_database_parameters(this);
        this->lastTimeParameterWereCached = now;
        this->cacheTimeoutInMs = this->get_long(this, SYM_PARAMETER_PARAMETER_REFRESH_PERIOD_IN_MS, 60000);
    }
    return this->parameters;
}

char* SymParameterService_get_registration_url(SymParameterService *this) {
    return this->get_string(this, SYM_PARAMETER_REGISTRATION_URL, NULL);
}

char* SymParameterService_get_sync_url(SymParameterService *this) {
    return this->get_string(this, SYM_PARAMETER_SYNC_URL, NULL);
}

char * SymParameterService_get_external_id(SymParameterService *this) {
    return this->get_string(this, SYM_PARAMETER_EXTERNAL_ID, NULL);
}

char * SymParameterService_get_node_group_id(SymParameterService *this) {
    return this->get_string(this, SYM_PARAMETER_GROUP_ID, NULL);
}

char * SymParameterService_get_string(SymParameterService *this, char *name, char *defaultValue) {
    SymProperties *prop = get_parameters(this);
    char *value = prop->get(prop, name, NULL);
    if (value == NULL) {
        value = defaultValue;
    }
    return value;
}

long SymParameterService_get_long(SymParameterService *this, char *name, long defaultValue) {
    long value = defaultValue;
    char *stringValue = this->get_string(this, name, NULL);
    if (stringValue != NULL) {
        value = atol(stringValue);
    }
    return value;
}

int SymParameterService_get_int(SymParameterService *this, char *name, int defaultValue) {
    int value = defaultValue;
    char *stringValue = this->get_string(this, name, NULL);
    if (stringValue != NULL) {
        value = atoi(stringValue);
    }
    return value;
}

void SymParameterService_save_parameter(SymParameterService *this, char *externalId, char *nodeGroupId,
        char *name, char *value, char *lastUpdatedBy) {
}

void SymParameterService_delete_parameter(SymParameterService *this, char *externalId, char *nodeGroupId, char *name) {
}

void SymParameterService_destroy(SymParameterService *this) {
    this->parameters->destroy(this->parameters);
    free(this);
}

SymParameterService * SymParameterService_new(SymParameterService *this, SymProperties *properties) {
    if (this == NULL) {
        this = (SymParameterService *) calloc(1, sizeof(SymParameterService));
    }
    this->properties = properties;
    this->get_registration_url = (void *) &SymParameterService_get_registration_url;
    this->get_sync_url = (void *) &SymParameterService_get_sync_url;
    this->get_external_id = (void *) &SymParameterService_get_external_id;
    this->get_node_group_id = (void *) &SymParameterService_get_node_group_id;
    this->get_string = (void *) &SymParameterService_get_string;
    this->get_long = (void *) &SymParameterService_get_long;
    this->get_int = (void *) &SymParameterService_get_int;
    this->save_parameter = (void *) &SymParameterService_save_parameter;
    this->delete_parameter = (void *) &SymParameterService_delete_parameter;
    this->destroy = (void *) &SymParameterService_destroy;
    return this;
}
