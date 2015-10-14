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
#include "util/StringUtils.h"


void SymStringUtilsTest_testTrim() {
     CU_ASSERT(strcmp(Sym_trim("nothing to trim"), "nothing to trim") == 0);
     CU_ASSERT(strcmp(Sym_trim("  trim the front"), "trim the front") == 0);
     CU_ASSERT(strcmp(Sym_trim("trim the back     "), "trim the back") == 0);
     CU_ASSERT(strcmp(Sym_trim(" trim one char front and back "), "trim one char front and back") == 0);
     CU_ASSERT(strcmp(Sym_trim(" trim one char front"), "trim one char front") == 0);
     CU_ASSERT(strcmp(Sym_trim("trim one char back "), "trim one char back") == 0);
     CU_ASSERT(strcmp(Sym_trim("                   "), "") == 0);
     CU_ASSERT(strcmp(Sym_trim(" "), "") == 0);
     CU_ASSERT(strcmp(Sym_trim("a"), "a") == 0);
     CU_ASSERT(strcmp(Sym_trim(""), "") == 0);
}

void SymStringUtilsTest_test_toUpperCase() {
     CU_ASSERT(strcmp(Sym_toUpperCase("lowercase"), "LOWERCASE") == 0);
     CU_ASSERT(strcmp(Sym_toUpperCase("UPPER"), "UPPER") == 0);
     CU_ASSERT(strcmp(Sym_toUpperCase("MixeD Case"), "MIXED CASE") == 0);
     CU_ASSERT(strcmp(Sym_toUpperCase(" "), " ") == 0);
     CU_ASSERT(strcmp(Sym_toUpperCase("a"), "A") == 0);
     CU_ASSERT(strcmp(Sym_toUpperCase(""), "") == 0);
}

void SymStringUtilsTest_test_toLowerCase() {
     CU_ASSERT(strcmp(Sym_toLowerCase("lowercase"), "lowercase") == 0);
     CU_ASSERT(strcmp(Sym_toLowerCase("UPPER"), "upper") == 0);
     CU_ASSERT(strcmp(Sym_toLowerCase("MixeD Case"), "mixed case") == 0);
     CU_ASSERT(strcmp(Sym_toLowerCase(" "), " ") == 0);
     CU_ASSERT(strcmp(Sym_toLowerCase("a"), "a") == 0);
     CU_ASSERT(strcmp(Sym_toLowerCase(""), "") == 0);
}

int SymStringUtilsTest_CUnit() {
    CU_pSuite suite = CU_add_suite("SymStringUtilsTest", NULL, NULL);
    if (suite == NULL) {
        return CUE_NOSUITE;
    }

    if (CU_add_test(suite, "SymStringUtilsTest_testTrim", SymStringUtilsTest_testTrim) == NULL ||
            CU_add_test(suite, "SymStringUtilsTest_test_toUpperCase", SymStringUtilsTest_test_toUpperCase) == NULL ||
            CU_add_test(suite, "SymStringUtilsTest_test_toLowerCase", SymStringUtilsTest_test_toLowerCase) == NULL) {
        return CUE_NOTEST;
    }
    return CUE_SUCCESS;
}
