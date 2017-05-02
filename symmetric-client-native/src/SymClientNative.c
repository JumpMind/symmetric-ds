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
#include "SymClientNative.h"

SymStringArray * SymNativeClient_getPropertiesNames(int argCount, char **argValues) {
    SymStringArray *propertiesNames = SymStringArray_new(NULL);

    if (argCount > 1) {
        propertiesNames->add(propertiesNames, argValues[1]);
    }
    else {
        char *location = SymStringUtils_format("%s/symmetric.properties", getenv("HOME"));
        propertiesNames->add(propertiesNames, location);
        free(location);
        propertiesNames->add(propertiesNames, "./symmetric.properties");
    }
    return propertiesNames;
}

int SymNativeClient_runSymmetricEngine(SymProperties *properties) {

    SymEngine *engine = SymEngine_new(NULL, properties);

    SymJobManager *jobManager = SymJobManager_new(NULL, engine);

    jobManager->startJobs(jobManager);

    jobManager->destroy(jobManager);
    engine->destroy(engine);
    return 0;
}

int main(int argCount, char **argValues) {
    SymStringArray * propertiesFiles = SymNativeClient_getPropertiesNames(argCount, argValues);
    SymProperties *properties = NULL;

    int i;
    for (i = 0; i < propertiesFiles->size; ++i) {
        char *fileName = propertiesFiles->get(propertiesFiles, i);

        printf("Checking for %s... ", fileName);
        if (access( fileName, F_OK ) != -1 ) {
            printf("Found %s.\n", fileName);
            properties = SymProperties_newWithFile(NULL, fileName);
            break;
        }
        else {
            printf("Not found.\n");
        }
    }

    if (properties == NULL) {
        fprintf(stderr, "No properties file found.  Usage: sym [properties-file-location]\n");
        return 1;
    }

    SymLog_configure(properties);

    SymNativeClient_runSymmetricEngine(properties);

    if (properties) {
        properties->destroy(properties);
    }
    propertiesFiles->destroy(propertiesFiles);
    return 0;
}
