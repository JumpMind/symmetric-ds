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

static SymDatabaseParameter * databaseParameterMapper(SymRow *row) {
    SymDatabaseParameter *param = SymDatabaseParameter_new(NULL);
    param->key = row->getStringNew(row, "param_key");
    param->value = row->getStringNew(row, "param_value");
    param->externalId = row->getStringNew(row, "external_id");
    param->nodeGroupId = row->getStringNew(row, "node_group_id");
    return param;
}

void * parameterMapper(SymRow *row, SymParameterService *this) {
    if (row->getString(row, "param_value")) {
        char* name = row->getStringNew(row, "param_key");
        this->parameters->put(this->parameters, name, row->getStringNew(row, "param_value"));
    }
    return NULL;
}

static void getDatabaseParameters(SymParameterService *this, char *externalId, char *nodeGroupId) {
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    SymStringArray *args = SymStringArray_new(NULL);
    args->add(args, externalId)->add(args, nodeGroupId);
    int error;
    SymList *list = sqlTemplate->queryWithUserData(sqlTemplate, SYM_SQL_SELECT_PARAMETERS, args, NULL, &error, (void *) &parameterMapper, this);
    args->destroy(args);
    list->destroy(list);
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

static void rereadParameters(SymParameterService *this) {
    this->lastTimeParameterWereCached = 0;
    getParameters(this);
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
            value = 0;
        }
    }
    return value;
}

void SymParameterService_saveParameter(SymParameterService *this, char *externalId, char *nodeGroupId,
        char *key, char *paramValue, char *lastUpdatedBy) {
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    SymStringArray *args = SymStringArray_new(NULL);
    args->add(args, paramValue)->add(args, lastUpdatedBy)->add(args, externalId);
    args->add(args, nodeGroupId)->add(args, key);
    int error;
    int count = sqlTemplate->update(sqlTemplate, SYM_SQL_UPDATE_PARAMETER, args, NULL, &error);
    args->reset(args);

    if (count == 0) {
        args->add(args, externalId)->add(args, nodeGroupId)->add(args, key);
        args->add(args, paramValue)->add(args, lastUpdatedBy);
        sqlTemplate->update(sqlTemplate, SYM_SQL_INSERT_PARAMETER, args, NULL, &error);
    }

    rereadParameters(this);
}

void SymParameterService_deleteParameter(SymParameterService *this, char *externalId, char *nodeGroupId, char *key) {
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    SymStringArray *args = SymStringArray_new(NULL);
    args->add(args, externalId)->add(args, nodeGroupId)->add(args, key);
    int error;
    sqlTemplate->update(sqlTemplate, SYM_SQL_DELETE_PARAMETER, args, NULL, &error);
    rereadParameters(this);
}

SymList * SymParameterService_getDatabaseParametersFor(SymParameterService *this, char *paramKey) {
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    SymStringArray *args = SymStringArray_new(NULL);
    args->add(args, paramKey);
    int error;
    return sqlTemplate->query(sqlTemplate, SYM_SELECT_PARAMETERS_BY_KEY, args, NULL, &error, (void *) &databaseParameterMapper);
}

void SymParameterService_destroy(SymParameterService *this) {
    this->parameters->destroy(this->parameters);
    free(this);
}

SymParameterService * SymParameterService_new(SymParameterService *this, SymProperties *properties, SymDatabasePlatform *platform) {
    if (this == NULL) {
        this = (SymParameterService *) calloc(1, sizeof(SymParameterService));
    }
    this->properties = properties;
    this->platform = platform;
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
    this->getDatabaseParametersFor = (void *) &SymParameterService_getDatabaseParametersFor;
    this->destroy = (void *) &SymParameterService_destroy;
    return this;
}
