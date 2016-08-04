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
     CU_ASSERT(strcmp(SymStringUtils_trim("nothing to trim"), "nothing to trim") == 0);
     CU_ASSERT(strcmp(SymStringUtils_trim("  trim the front"), "trim the front") == 0);
     CU_ASSERT(strcmp(SymStringUtils_trim("trim the back     "), "trim the back") == 0);
     CU_ASSERT(strcmp(SymStringUtils_trim(" trim one char front and back "), "trim one char front and back") == 0);
     CU_ASSERT(strcmp(SymStringUtils_trim(" trim one char front"), "trim one char front") == 0);
     free(SymStringUtils_trim(" trim one char front"));
     CU_ASSERT(strcmp(SymStringUtils_trim("trim one char back "), "trim one char back") == 0);
     CU_ASSERT(strcmp(SymStringUtils_trim("                   "), "") == 0);
     CU_ASSERT(strcmp(SymStringUtils_trim(" "), "") == 0);
     free(SymStringUtils_trim(" "));
     CU_ASSERT(strcmp(SymStringUtils_trim("a"), "a") == 0);
     CU_ASSERT(strcmp(SymStringUtils_trim(""), "") == 0);
     free(SymStringUtils_trim(""));
}

void SymStringUtilsTest_testLTrim() {
     CU_ASSERT(strcmp(SymStringUtils_ltrim("nothing to trim"), "nothing to trim") == 0);
     CU_ASSERT(strcmp(SymStringUtils_ltrim("  trim the front"), "trim the front") == 0);
     CU_ASSERT(strcmp(SymStringUtils_ltrim("trim the back     "), "trim the back     ") == 0);
     CU_ASSERT(strcmp(SymStringUtils_ltrim(" trim one char front and back "), "trim one char front and back ") == 0);
     CU_ASSERT(strcmp(SymStringUtils_ltrim(" trim one char front"), "trim one char front") == 0);
     CU_ASSERT(strcmp(SymStringUtils_ltrim("trim one char back "), "trim one char back ") == 0);
     free(SymStringUtils_ltrim("trim one char back "));
     CU_ASSERT(strcmp(SymStringUtils_ltrim("                   "), "") == 0);
     CU_ASSERT(strcmp(SymStringUtils_ltrim(" "), "") == 0);
     CU_ASSERT(strcmp(SymStringUtils_ltrim("a"), "a") == 0);
     CU_ASSERT(strcmp(SymStringUtils_ltrim(""), "") == 0);
     free(SymStringUtils_ltrim(""));
}

void SymStringUtilsTest_testRTrim() {
     CU_ASSERT(strcmp(SymStringUtils_rtrim("nothing to trim"), "nothing to trim") == 0);
     CU_ASSERT(strcmp(SymStringUtils_rtrim("  trim the front"), "  trim the front") == 0);
     CU_ASSERT(strcmp(SymStringUtils_rtrim("trim the back     "), "trim the back") == 0);
     CU_ASSERT(strcmp(SymStringUtils_rtrim(" trim one char front and back "), " trim one char front and back") == 0);
     CU_ASSERT(strcmp(SymStringUtils_rtrim(" trim one char front"), " trim one char front") == 0);
     CU_ASSERT(strcmp(SymStringUtils_rtrim("trim one char back "), "trim one char back") == 0);
     free(SymStringUtils_rtrim("trim one char back "));
     CU_ASSERT(strcmp(SymStringUtils_rtrim("                   "), "") == 0);
     CU_ASSERT(strcmp(SymStringUtils_rtrim(" "), "") == 0);
     free(SymStringUtils_rtrim(" "));
     CU_ASSERT(strcmp(SymStringUtils_rtrim("a"), "a") == 0);
     CU_ASSERT(strcmp(SymStringUtils_rtrim(""), "") == 0);
     free(SymStringUtils_rtrim(""));
}

void SymStringUtilsTest_test_toUpperCase() {
     CU_ASSERT(strcmp(SymStringUtils_toUpperCase("lowercase"), "LOWERCASE") == 0);
     CU_ASSERT(strcmp(SymStringUtils_toUpperCase("UPPER"), "UPPER") == 0);
     CU_ASSERT(strcmp(SymStringUtils_toUpperCase("MixeD Case"), "MIXED CASE") == 0);
     CU_ASSERT(strcmp(SymStringUtils_toUpperCase(" "), " ") == 0);
     CU_ASSERT(strcmp(SymStringUtils_toUpperCase("a"), "A") == 0);
     CU_ASSERT(strcmp(SymStringUtils_toUpperCase(""), "") == 0);
}

void SymStringUtilsTest_test_toLowerCase() {
     CU_ASSERT(strcmp(SymStringUtils_toLowerCase("lowercase"), "lowercase") == 0);
     CU_ASSERT(strcmp(SymStringUtils_toLowerCase("UPPER"), "upper") == 0);
     CU_ASSERT(strcmp(SymStringUtils_toLowerCase("MixeD Case"), "mixed case") == 0);
     CU_ASSERT(strcmp(SymStringUtils_toLowerCase(" "), " ") == 0);
     CU_ASSERT(strcmp(SymStringUtils_toLowerCase("a"), "a") == 0);
     CU_ASSERT(strcmp(SymStringUtils_toLowerCase(""), "") == 0);
}

void SymStringUtilsTest_test_isBlank() {
    CU_ASSERT(SymStringUtils_isBlank("t") == 0);
    CU_ASSERT(SymStringUtils_isBlank("       t") == 0);
    CU_ASSERT(SymStringUtils_isBlank("some string") == 0);
    CU_ASSERT(SymStringUtils_isBlank("") == 1);
    CU_ASSERT(SymStringUtils_isBlank(" ") == 1);
    CU_ASSERT(SymStringUtils_isBlank("  ") == 1);
    CU_ASSERT(SymStringUtils_isBlank("\t") == 1);
}

void SymStringUtilsTest_test_isNotBlank() {
    CU_ASSERT(SymStringUtils_isNotBlank("t") == 1);
    CU_ASSERT(SymStringUtils_isNotBlank("       t") == 1);
    CU_ASSERT(SymStringUtils_isNotBlank("some string") == 1);
    CU_ASSERT(SymStringUtils_isNotBlank("") == 0);
    CU_ASSERT(SymStringUtils_isNotBlank(" ") == 0);
    CU_ASSERT(SymStringUtils_isNotBlank("  ") == 0);
    CU_ASSERT(SymStringUtils_isNotBlank("\t") == 0);
}

void SymStringUtilsTest_test_format() {
    CU_ASSERT(strcmp(SymStringUtils_format("Hello"), "Hello") == 0);
    CU_ASSERT(strcmp(SymStringUtils_format("%s", "2"), "2") == 0);
    CU_ASSERT(strcmp(SymStringUtils_format("%s", ""), "") == 0);
    CU_ASSERT(strcmp(SymStringUtils_format("%s%s", "1", ""), "1") == 0);
    CU_ASSERT(strcmp(SymStringUtils_format("%s%s", "1", "2"), "12") == 0);
    CU_ASSERT(strcmp(SymStringUtils_format("%s%s", "2", "2"), "22") == 0);
    CU_ASSERT(strcmp(SymStringUtils_format("%s%s%s%s%s%s%s%s%s%s", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"), "12345678910") == 0);
}

void SymStringUtilsTest_test_substring() {
    CU_ASSERT(strcmp(SymStringUtils_substring("Testing", 0, 1), "T") == 0);
    CU_ASSERT(strcmp(SymStringUtils_substring("Testing", 1, 2), "e") == 0);
    CU_ASSERT(strcmp(SymStringUtils_substring("", 0, 0), "") == 0);
    CU_ASSERT(strcmp(SymStringUtils_substring("T", 0, 1), "T") == 0);
    CU_ASSERT(strcmp(SymStringUtils_substring("Testing", 0, 8), "Testing") == 0);
}

void SymStringUtilsTest_test_replace() {
    CU_ASSERT(strcmp(SymStringUtils_replaceWithLength("", strlen(""), "H", "Tr"), "") == 0);
    CU_ASSERT(strcmp(SymStringUtils_replaceWithLength("Hello", strlen("Hello"), "H", "Tr"), "Trello") == 0);
    CU_ASSERT(strcmp(SymStringUtils_replaceWithLength("\\\"Hi\\\"", strlen("\\\"Hi\\\""), "\\\"", "\"\""),
            "\"\"Hi\"\"") == 0);
    CU_ASSERT(strcmp(SymStringUtils_replaceWithLength(",", strlen(","), ",", "||"), "||") == 0);
    CU_ASSERT(strcmp(SymStringUtils_replaceWithLength(" spaces ", strlen(" spaces "), " ", "|longer|"), "|longer|spaces|longer|") == 0);

    CU_ASSERT(strcmp(SymStringUtils_replace("", "H", "Tr"), "") == 0);
    CU_ASSERT(strcmp(SymStringUtils_replace("Hello", "H", "Tr"), "Trello") == 0);
    CU_ASSERT(strcmp(SymStringUtils_replace("\\\"Hi\\\"", "\\\"", "\"\""), "\"\"Hi\"\"") == 0);
    CU_ASSERT(strcmp(SymStringUtils_replace(",", ",", "||"), "||") == 0);
    CU_ASSERT(strcmp(SymStringUtils_replace(" spaces ", " ", "|longer|"), "|longer|spaces|longer|") == 0);

}

void SymStringUtilsTest_test_startsWith() {
    CU_ASSERT(SymStringUtils_startsWith("Testing","Tes") == 1);
    CU_ASSERT(SymStringUtils_startsWith("Testing","Ttt") == 0);
    CU_ASSERT(SymStringUtils_startsWith("Testing","T") == 1);
    CU_ASSERT(SymStringUtils_startsWith("","") == 1);
    CU_ASSERT(SymStringUtils_startsWith("1","1") == 1);
    CU_ASSERT(SymStringUtils_startsWith("1"," ") == 0);
}

void SymStringUtilsTest_test_endsWith() {
    CU_ASSERT(SymStringUtils_endsWith("Testing","ing") == 1);
    CU_ASSERT(SymStringUtils_endsWith("Testing","ed") == 0);
    CU_ASSERT(SymStringUtils_endsWith("","") == 1);
    CU_ASSERT(SymStringUtils_endsWith("1","1") == 1);
    CU_ASSERT(SymStringUtils_endsWith("1"," ") == 0);
}


int SymStringUtilsTest_CUnit() {
    CU_pSuite suite = CU_add_suite("SymStringUtilsTest", NULL, NULL);
    if (suite == NULL) {
        return CUE_NOSUITE;
    }

    if (CU_add_test(suite, "SymStringUtilsTest_testTrim", SymStringUtilsTest_testTrim) == NULL ||
            CU_add_test(suite, "SymStringUtilsTest_test_toUpperCase", SymStringUtilsTest_test_toUpperCase) == NULL ||
            CU_add_test(suite, "SymStringUtilsTest_test_toLowerCase", SymStringUtilsTest_test_toLowerCase) == NULL ||
            CU_add_test(suite, "SymStringUtilsTest_test_isBlank", SymStringUtilsTest_test_isBlank) == NULL ||
            CU_add_test(suite, "SymStringUtilsTest_test_isNotBlank", SymStringUtilsTest_test_isNotBlank) == NULL ||
            CU_add_test(suite, "SymStringUtilsTest_test_format", SymStringUtilsTest_test_format) == NULL ||
            CU_add_test(suite, "SymStringUtilsTest_test_substring", SymStringUtilsTest_test_substring) == NULL ||
            CU_add_test(suite, "SymStringUtilsTest_testLTrim", SymStringUtilsTest_testLTrim) == NULL ||
            CU_add_test(suite, "SymStringUtilsTest_testRTrim", SymStringUtilsTest_testRTrim) == NULL ||
            CU_add_test(suite, "SymStringUtilsTest_test_startsWith", SymStringUtilsTest_test_startsWith) == NULL ||
            CU_add_test(suite, "SymStringUtilsTest_test_endsWith", SymStringUtilsTest_test_endsWith) == NULL ||
            CU_add_test(suite, "SymStringUtilsTest_test_replace", SymStringUtilsTest_test_replace) == NULL ||

            1==0) {
        return CUE_NOTEST;
    }
    return CUE_SUCCESS;
}
