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
void SymStringArrayTest_testSplitLong() {
    // This long string was triggering a bug because it splits longer than the initial
    // length of the array.
    SymStringArray *array = SymStringArray_split("batch-11=ok&nodeId-11=000&network-11=0&filter-11=0&database-11=2&byteCount-11=203&batch-12=ok&nodeId-12=000&network-12=0&filter-12=0&database-12=0&byteCount-12=8&batch-14=ok&nodeId-14=000&network-14=0&filter-14=0&database-14=0&byteCount-14=8&batch-16=ok&nodeId-16=000&network-16=0&filter-16=0&database-16=0&byteCount-16=8&batch-17=ok&nodeId-17=000&network-17=0&filter-17=0&database-17=0&byteCount-17=8&batch-18=ok&nodeId-18=000&network-18=0&filter-18=0&database-18=0&byteCount-18=8&batch-19=ok&nodeId-19=000&network-19=0&filter-19=0&database-19=0&byteCount-19=8&batch-21=ok&nodeId-21=000&network-21=0&filter-21=0&database-21=0&byteCount-21=8&batch-22=ok&nodeId-22=000&network-22=0&filter-22=0&database-22=0&byteCount-22=8&batch-23=ok&nodeId-23=000&network-23=0&filter-23=0&database-23=0&byteCount-23=8&batch-24=ok&nodeId-24=000&network-24=0&filter-24=0&database-24=0&byteCount-24=8&batch-25=ok&nodeId-25=000&network-25=0&filter-25=0&database-25=0&byteCount-25=8&batch-26=ok&nodeId-26=000&network-26=0&filter-26=0&database-26=0&byteCount-26=8&batch-27=ok&nodeId-27=000&network-27=0&filter-27=0&database-27=0&byteCount-27=8&batch-28=ok&nodeId-28=000&network-28=0&filter-28=0&database-28=0&byteCount-28=8&batch-29=ok&nodeId-29=000&network-29=0&filter-29=0&database-29=0&byteCount-29=8&batch-30=ok&nodeId-30=000&network-30=0&filter-30=0&database-30=0&byteCount-30=8&batch-31=ok&nodeId-31=000&network-31=0&filter-31=0&database-31=0&byteCount-31=8&batch-32=ok&nodeId-32=000&network-32=0&filter-32=0&database-32=0&byteCount-32=8&batch-33=ok&nodeId-33=000&network-33=0&filter-33=0&database-33=0&byteCount-33=8", "&");

    CU_ASSERT(array->size == 120);

    CU_ASSERT(strcmp(array->get(array, 0), "batch-11=ok") == 0);
    CU_ASSERT(strcmp(array->get(array, 1), "nodeId-11=000") == 0);
    CU_ASSERT(strcmp(array->get(array, 2), "network-11=0") == 0);

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
            CU_add_test(suite, "SymStringArrayTest_test4", SymStringArrayTest_test4) == NULL ||
            CU_add_test(suite, "SymStringArrayTest_testSplitLong", SymStringArrayTest_testSplitLong) == NULL ||
            1==0
            ) {
        return CUE_NOTEST;
    }
    return CUE_SUCCESS;
}
