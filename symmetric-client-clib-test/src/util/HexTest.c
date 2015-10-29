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
#include <string.h>
#include "util/Hex.h"

void SymHexTest_testDecode1() {
    int outSize;
    unsigned char *result = SymHex_decode("656667", &outSize);
    CU_ASSERT(outSize == 3);
    CU_ASSERT(strcmp((char *) result, "efg") == 0);
}

void SymHexTest_testEncode1() {
    char *result = SymHex_encode((unsigned char * ) "efg", 3);
    CU_ASSERT(strcmp(result, "656667") == 0);
}

int SymHexTest_CUnit() {
    CU_pSuite suite = CU_add_suite("SymHexTest", NULL, NULL);
    if (suite == NULL) {
        return CUE_NOSUITE;
    }

    if (CU_add_test(suite, "SymHexTest_testDecode1", SymHexTest_testDecode1) == NULL ||
            CU_add_test(suite, "SymHexTest_testEncode1", SymHexTest_testEncode1) == NULL) {
        return CUE_NOTEST;
    }
    return CUE_SUCCESS;
}
