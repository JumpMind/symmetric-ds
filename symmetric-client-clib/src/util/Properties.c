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
#include "util/Properties.h"

int SymProperties_indexOf(SymProperties *this, char *key) {
    int i;
    for (i = 0; i < this->index; i++) {
        if (strcmp(this->propArray[i].key, key) == 0) {
            return i;
        }
    }
    return SYM_NOT_FOUND;
}

char * SymProperties_get(SymProperties *this, char *key, char *defaultValue) {
    int indexOf = SymProperties_indexOf(this, key);
    if (indexOf != SYM_NOT_FOUND) {
        return this->propArray[indexOf].value;
    } else {
        return defaultValue;
    }
}

void SymProperties_put(SymProperties *this, char *key, char *value) {
    int indexOf = SymProperties_indexOf(this, key);
    if (indexOf != SYM_NOT_FOUND) {
        this->propArray[indexOf].key = key;
        this->propArray[indexOf].value = value;
    } else {
        this->propArray[this->index].key = key;
        this->propArray[this->index].value = value;
        this->index++;
    }
}

void SymProperties_putAll(SymProperties *this, SymProperties *properties) {
    int i;
    for (i = 0; i < properties->index; i++) {
        this->put(this, properties->propArray[i].key, properties->propArray[i].value);
    }
}

char * SymProperties_toString(struct SymProperties *this) {
    SymStringBuilder *buff = SymStringBuilder_new(NULL);

    int i;
    for (i = 0; i < this->index; i++) {
        buff->append(buff, this->propArray[i].key);
        buff->append(buff, "=");
        buff->append(buff, this->propArray[i].value);
        if (i < this->index) {
            buff->append(buff, ", ");
        }
    }

    return buff->destroyAndReturn(buff);
}

void SymProperties_destroy(SymProperties *this) {
    free(this->propArray);
    free(this);
}

SymProperties * SymProperties_new(SymProperties *this) {
    if (this == NULL) {
        this = (SymProperties *) calloc(1, sizeof(SymProperties));
    }
    this->propArray = (SymProperty *) calloc(255, sizeof(SymProperty));
    this->get = (void *) &SymProperties_get;
    this->put = (void *) &SymProperties_put;
    this->putAll = (void *) &SymProperties_putAll;
    this->toString = (void *) &SymProperties_toString;
    this->destroy = (void *) &SymProperties_destroy;
    return this;
}

SymProperties * SymProperties_newWithString(SymProperties *this, char *propertiesFileContents) {
    if (this == NULL) {
        this = SymProperties_new(this);
    }

    SymStringArray *propertyLines = SymStringArray_split(propertiesFileContents, "\n");
    int i;
    for (i = 0; i < propertyLines->size; ++i) {
        char *propertyLine = propertyLines->get(propertyLines, i);
        if (SymStringUtils_isBlank(propertyLine)) {
            continue;
        }

        char *trimmed = SymStringUtils_ltrim(propertyLine);
        if (trimmed[0] == '#' || trimmed[0] == '!' ) {
            free(trimmed);
            continue;
        }

        int delimiterIndex = 0;
        int length = strlen(trimmed);
        int i;
        for (i = 0; i < length; ++i) {
            if (trimmed[i] == '=' || trimmed[i] == ':') {
                delimiterIndex = i;
                break;
            }
        }

        if (delimiterIndex > 0) {
            SymStringBuilder *buff = SymStringBuilder_newWithString(trimmed);
            char *propertyName = buff->substring(buff, 0, delimiterIndex);
            char *propertyValue = buff->substring(buff, delimiterIndex+1, length);
            this->put(this, propertyName, propertyValue);
            buff->destroy(buff);
        }

        free(trimmed);
    }

    propertyLines->destroy(propertyLines);

    return this;
}

SymProperties * SymProperties_newWithFile(SymProperties *this, char *argPath) {
    FILE *file;
    int BUFFER_SIZE = 1024;
    char inputBuffer[BUFFER_SIZE];

    file = fopen(argPath,"r");
    if (!file) {
        return NULL;
    }

    SymStringBuilder *buff = SymStringBuilder_newWithSize(4096);

    while (fgets(inputBuffer, BUFFER_SIZE, file) != NULL) {
        buff->append(buff, inputBuffer);
    }

    char *fileContentsRaw = buff->destroyAndReturn(buff);
    this = SymProperties_newWithString(NULL, fileContentsRaw);

    fclose(file);

    free(fileContentsRaw);

    return this;
}
