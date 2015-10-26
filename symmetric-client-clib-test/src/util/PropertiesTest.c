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
#include "symclient_test.h"

#define NOT_FOUND "<not found>"

void SymPropertiesTest_test1() {
    SymProperties *prop = SymProperties_new(NULL);
    char *dbUrl = "jdbc:sqlite:filename";
    char *dbUser = "admin";
    char *dbPassword = "secret";

    prop->put(prop, SYM_PARAMETER_DB_URL, dbUrl);
    prop->put(prop, SYM_PARAMETER_DB_USER, dbUser);
    prop->put(prop, SYM_PARAMETER_DB_PASSWORD, dbPassword);

    CU_ASSERT(strcmp(prop->get(prop, SYM_PARAMETER_DB_URL, NOT_FOUND), dbUrl) == 0);
    CU_ASSERT(strcmp(prop->get(prop, SYM_PARAMETER_DB_USER, NOT_FOUND), dbUser) == 0);
    CU_ASSERT(strcmp(prop->get(prop, SYM_PARAMETER_DB_PASSWORD, NOT_FOUND), dbPassword) == 0);
    prop->destroy(prop);
}

void SymPropertiesTest_test2() {
    SymProperties *prop = SymProperties_new(NULL);
    char *regUrl = "http://localhost:31415/server";

    prop->put(prop, SYM_PARAMETER_REGISTRATION_URL, regUrl);

    CU_ASSERT(strcmp(prop->get(prop, SYM_PARAMETER_REGISTRATION_URL, NOT_FOUND), regUrl) == 0);
    prop->destroy(prop);
}

void SymPropertiesTest_test3() {
    SymProperties *prop = SymProperties_new(NULL);
    char *dbUser = "administrator";
    char *dbPassword = "secretly";

    prop->put(prop, SYM_PARAMETER_DB_USER, dbUser);
    prop->put(prop, SYM_PARAMETER_DB_PASSWORD, dbPassword);

    SymProperties *prop2 = SymProperties_new(NULL);
    prop2->putAll(prop2, prop);

    CU_ASSERT(strcmp(prop2->get(prop, SYM_PARAMETER_DB_USER, NOT_FOUND), dbUser) == 0);
    CU_ASSERT(strcmp(prop2->get(prop, SYM_PARAMETER_DB_PASSWORD, NOT_FOUND), dbPassword) == 0);
    prop->destroy(prop);
    prop2->destroy(prop2);
}

void SymPropertiesTest_testNewWithString() {
    char *propertiesFileContents = " prop1=value1\n#comment\n \nprop2=value 2 ";

    SymProperties *prop = SymProperties_newWithString(NULL, propertiesFileContents);

    CU_ASSERT(strcmp(prop->get(prop, "prop1", NOT_FOUND), "value1") == 0);
    CU_ASSERT(strcmp(prop->get(prop, "prop2", NOT_FOUND), "value 2 ") == 0);

    prop->destroy(prop);
}

int SymPropertiesTest_CUnit() {
    CU_pSuite suite = CU_add_suite("SymPropertiesTest", NULL, NULL);
    if (suite == NULL) {
        return CUE_NOSUITE;
    }

    if (CU_add_test(suite, "SymPropertiesTest_test1", SymPropertiesTest_test1) == NULL ||
            CU_add_test(suite, "SymPropertiesTest_test2", SymPropertiesTest_test2) == NULL ||
            CU_add_test(suite, "SymPropertiesTest_test3", SymPropertiesTest_test3) == NULL ||
            CU_add_test(suite, "SymPropertiesTest_testNewWithString", SymPropertiesTest_testNewWithString) == NULL ||
            1==0) {
        return CUE_NOTEST;
    }
    return CUE_SUCCESS;
}
