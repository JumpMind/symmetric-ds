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

static void getDatabaseParameters(SymParameterService *this, char *externalId, char *nodeGroupId) {
}

static void rereadDatabaseParameters(SymParameterService *this) {
    if (this->parameters != NULL) {
        this->parameters->destroy(this->parameters);
    }
    this->parameters = SymProperties_new(NULL);
    this->parameters->putAll(this->parameters, this->properties);
    getDatabaseParameters(this, SYM_PARAMETER_ALL, SYM_PARAMETER_ALL);
    getDatabaseParameters(this, SYM_PARAMETER_ALL, this->properties->get(this->properties, SYM_PARAMETER_GROUP_ID, NULL));
    getDatabaseParameters(this, this->properties->get(this->properties, SYM_PARAMETER_EXTERNAL_ID, NULL),
            this->properties->get(this->properties, SYM_PARAMETER_GROUP_ID, NULL));
}

static SymProperties * getParameters(SymParameterService *this) {
    time_t now = time(NULL);
    if (this->parameters == NULL || (this->cacheTimeoutInMs > 0 && this->lastTimeParameterWereCached < (now - (this->cacheTimeoutInMs / 1000)))) {
        rereadDatabaseParameters(this);
        this->lastTimeParameterWereCached = now;
        this->cacheTimeoutInMs = this->getLong(this, SYM_PARAMETER_PARAMETER_REFRESH_PERIOD_IN_MS, 60000);
    }
    return this->parameters;
}

char* SymParameterService_getRegistrationUrl(SymParameterService *this) {
    return this->getString(this, SYM_PARAMETER_REGISTRATION_URL, NULL);
}

char* SymParameterService_getSyncUrl(SymParameterService *this) {
    return this->getString(this, SYM_PARAMETER_SYNC_URL, NULL);
}

char * SymParameterService_getExternalId(SymParameterService *this) {
    return this->getString(this, SYM_PARAMETER_EXTERNAL_ID, NULL);
}

char * SymParameterService_getNodeGroupId(SymParameterService *this) {
    return this->getString(this, SYM_PARAMETER_GROUP_ID, NULL);
}

char * SymParameterService_getString(SymParameterService *this, char *name, char *defaultValue) {
    SymProperties *prop = getParameters(this);
    char *value = prop->get(prop, name, NULL);
    if (value == NULL) {
        value = defaultValue;
    }
    return value;
}

long SymParameterService_getLong(SymParameterService *this, char *name, long defaultValue) {
    long value = defaultValue;
    char *stringValue = this->getString(this, name, NULL);
    if (stringValue != NULL) {
        value = atol(stringValue);
    }
    return value;
}

int SymParameterService_getInt(SymParameterService *this, char *name, int defaultValue) {
    int value = defaultValue;
    char *stringValue = this->getString(this, name, NULL);
    if (stringValue != NULL) {
        value = atoi(stringValue);
    }
    return value;
}

unsigned short SymParameterService_is(SymParameterService *this, char *name, unsigned short defaultValue) {
    unsigned short value = defaultValue;
    char *stringValue = this->getString(this, name, NULL);
    if (stringValue != NULL) {
        if (strcmp("true", stringValue) == 0) {
            value = 1;
        } else {
            value = atoi(stringValue);
        }
    }
    return value;
}

void SymParameterService_saveParameter(SymParameterService *this, char *externalId, char *nodeGroupId,
        char *name, char *value, char *lastUpdatedBy) {
}

void SymParameterService_deleteParameter(SymParameterService *this, char *externalId, char *nodeGroupId, char *name) {
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
    this->getRegistrationUrl = (void *) &SymParameterService_getRegistrationUrl;
    this->getSyncUrl = (void *) &SymParameterService_getSyncUrl;
    this->getExternalId = (void *) &SymParameterService_getExternalId;
    this->getNodeGroupId = (void *) &SymParameterService_getNodeGroupId;
    this->getString = (void *) &SymParameterService_getString;
    this->getLong = (void *) &SymParameterService_getLong;
    this->getInt = (void *) &SymParameterService_getInt;
    this->is = (void *) &SymParameterService_is;
    this->saveParameter = (void *) &SymParameterService_saveParameter;
    this->deleteParameter = (void *) &SymParameterService_deleteParameter;
    this->destroy = (void *) &SymParameterService_destroy;
    return this;
}
