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

int main() {
    if (CU_initialize_registry() != CUE_SUCCESS) {
        return CU_get_error();
    }

    if (
            //SymEngineTest_CUnit() != CUE_SUCCESS ||
            //SymMapTest_CUnit() != CUE_SUCCESS ||
            //SymListTest_CUnit() != CUE_SUCCESS ||
            //SymStringBuilderTest_CUnit() != CUE_SUCCESS ||
            //SymStringArrayTest_CUnit() != CUE_SUCCESS ||
            //SymPropertiesTest_CUnit() != CUE_SUCCESS ||
               SymStringUtilsTest_CUnit() != CUE_SUCCESS ||
            //SymHexTest_CUnit() != CUE_SUCCESS ||
            //SymBase64Test_CUnit() != CUE_SUCCESS ||
            1==0) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    CU_basic_set_mode(CU_BRM_VERBOSE);
    CU_basic_run_tests();
    CU_cleanup_registry();
    return CU_get_error();
}
