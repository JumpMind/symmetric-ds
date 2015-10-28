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

void SymStringArrayTest_test1() {
    SymStringArray *array = SymStringArray_new(NULL);
    array->add(array, "one");
    array->add(array, "two");
    array->add(array, "three");

    CU_ASSERT(array->size == 3);
    CU_ASSERT(array->contains(array, "one"));
    CU_ASSERT(array->contains(array, "two"));
    CU_ASSERT(array->contains(array, "three"));
    CU_ASSERT(!array->contains(array, "twoo"));

    CU_ASSERT(strcmp(array->get(array, 0), "one") == 0);
    CU_ASSERT(strcmp(array->get(array, 1), "two") == 0);
    CU_ASSERT(strcmp(array->get(array, 2), "three") == 0);

    array->destroy(array);
}

void SymStringArrayTest_test2() {
    SymStringArray *array = SymStringArray_split("one,two,three", ",");

    CU_ASSERT(array->size == 3);
    CU_ASSERT(array->contains(array, "one"));
    CU_ASSERT(array->contains(array, "two"));
    CU_ASSERT(array->contains(array, "three"));

    if (array->size == 3) {
        CU_ASSERT(strcmp(array->get(array, 0), "one") == 0);
        CU_ASSERT(strcmp(array->get(array, 1), "two") == 0);
        CU_ASSERT(strcmp(array->get(array, 2), "three") == 0);
    }

    array->destroy(array);
}

void SymStringArrayTest_test3() {
    SymStringArray *array = SymStringArray_split("one", ",");

    CU_ASSERT(array->size == 1);
    CU_ASSERT(array->contains(array, "one"));

    if (array->size == 1) {
        CU_ASSERT(strcmp(array->get(array, 0), "one") == 0);
    }

    array->destroy(array);
}

void SymStringArrayTest_test4() {
    SymStringArray *array = SymStringArray_split(",,", ",");

    CU_ASSERT(array->size == 3);

    if (array->size == 3) {
        CU_ASSERT(array->get(array, 0) == NULL);
        CU_ASSERT(array->get(array, 1) == NULL);
        CU_ASSERT(array->get(array, 2) == NULL);
    }

    array->destroy(array);
}

int SymStringArrayTest_CUnit() {
    CU_pSuite suite = CU_add_suite("SymStringArrayTest", NULL, NULL);
    if (suite == NULL) {
        return CUE_NOSUITE;
    }

    if (CU_add_test(suite, "SymStringArrayTest_test1", SymStringArrayTest_test1) == NULL ||
            CU_add_test(suite, "SymStringArrayTest_test2", SymStringArrayTest_test2) == NULL ||
            CU_add_test(suite, "SymStringArrayTest_test3", SymStringArrayTest_test3) == NULL ||
            CU_add_test(suite, "SymStringArrayTest_test4", SymStringArrayTest_test4) == NULL) {
        return CUE_NOTEST;
    }
    return CUE_SUCCESS;
}
