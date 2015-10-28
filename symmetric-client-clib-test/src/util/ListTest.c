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

typedef struct {
    int i;
} myobj;

/*
void SymListTest_test1() {
    SymList *list = SymList_new(NULL);
    list->add(list, "hola");
    list->add(list, "");
    list->add(list, NULL);
    list->add(list, "bonjour");

    CU_ASSERT(strcmp(list->get(list, 0), "hola") == 0);
    CU_ASSERT(strcmp(list->get(list, 1), "") == 0);
    CU_ASSERT(list->get(list, 2) == NULL);
    CU_ASSERT(strcmp(list->get(list, 3), "bonjour") == 0);
    CU_ASSERT(list->size == 4);

    char **array = (char **) list->to_array(list);
    CU_ASSERT(strcmp(array[0], "hola") == 0);
    CU_ASSERT(array[1] != NULL);
    CU_ASSERT(strcmp(array[1], "") == 0);
    CU_ASSERT(array[2] == NULL);
    CU_ASSERT(strcmp(array[3], "bonjour") == 0);
    CU_ASSERT(array[4] == NULL);

    int i;
    for (i = 0; array[i] != NULL; i++) {
        printf("%d is %s\n", i, array[i]);
    }


    list->destroy(list);
}

void SymListTest_test2() {
    myobj myobjs[3];
    myobjs[0].i = 0;
    myobjs[1].i = 1;
    myobjs[2].i = 2;

    SymList *list = SymList_new(NULL);
    list->add(list, &myobjs[0]);
    list->add(list, NULL);
    list->add(list, &myobjs[2]);
    CU_ASSERT(((myobj *) list->get(list, 0))->i == 0);
    CU_ASSERT(((myobj *) list->get(list, 1)) == NULL);
    CU_ASSERT(((myobj *) list->get(list, 2))->i == 2);

    myobj **array = (myobj **) list->to_array(list);
    CU_ASSERT(array[0]->i == 0);
    CU_ASSERT(array[1] == NULL);
    CU_ASSERT(array[2]->i == 2);

    int i;
    for (i = 0; array[i] != NULL; i++) {
        printf("%d is %d\n", i, array[i]->i);
    }


    list->destroy(list);
}

void SymListTest_test3() {
    SymList *list = SymList_new(NULL);
    list->add(list, "one string");
    list->add(list, "two thing");
    list->add(list, "red ping");
    list->add(list, "blue ring");

    char **array = (char **) list->to_array_slice(list, 1, 2);
    CU_ASSERT(strcmp(array[0], "two thing") == 0);

    int i;
    for (i = 0; array[i] != NULL; i++) {
        printf("%d is %s\n", i, array[i]);
    }


    list->destroy(list);
}

*/

int SymListTest_CUnit() {
    CU_pSuite suite = CU_add_suite("SymListTest", NULL, NULL);
    if (suite == NULL) {
        return CUE_NOSUITE;
    }

    /*
    if (CU_add_test(suite, "SymListTest_test1", SymListTest_test1) == NULL ||
            CU_add_test(suite, "SymListTest_test2", SymListTest_test2) == NULL ||
            CU_add_test(suite, "SymListTest_test3", SymListTest_test3) == NULL) {
        return CUE_NOTEST;
    }
    */
    return CUE_SUCCESS;
}
