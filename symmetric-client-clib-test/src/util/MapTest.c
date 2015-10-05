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

static void SymMapTest_test_stringmap(int keySize, char **keys, char **values, int mapSize) {
    SymMap *map = SymMap_new(NULL, mapSize);
    int i;
    for (i = 0; i < keySize; i++) {
        map->put(map, keys[i], values[i], strlen(values[i]));
    }

    for (i = 0; i < keySize; i++) {
        char *out = (char *) map->get(map, keys[i]);
        CU_ASSERT(out != NULL);
        if (out != NULL) {
            CU_ASSERT(strcmp(out, values[i]) == 0);
        }
    }
    map->destroy(map);
}

void SymMapTest_test1() {
    char *keys[] = { "Hello", "Hi", "Welcome", "Greetings" };
    char *values[] = { "Goodbye", "Hey", "Thank you", "Later" };

    SymMapTest_test_stringmap(4, keys, values, 1);
    SymMapTest_test_stringmap(4, keys, values, 4);
    SymMapTest_test_stringmap(4, keys, values, 8);
}

void SymMapTest_test2() {
    char *keys[] = { "1", "2", "Three" };
    char *values[] = { "One", "", "3" };

    SymMapTest_test_stringmap(3, keys, values, 0);
    SymMapTest_test_stringmap(3, keys, values, 1);
    SymMapTest_test_stringmap(3, keys, values, 2);
    SymMapTest_test_stringmap(3, keys, values, 3);
}

void SymMapTest_test3() {
    char *columnNames[] = { "id", "name", "description", "create_time" };
    SymTable *table = SymTable_newWithFullname(NULL, "mydb", "dbo", "table1");
    table->columns = SymList_new(NULL);
    int i;
    for (i = 0; i < 4; i++) {
        table->columns->add(table->columns, SymColumn_new(NULL, columnNames[i], 0));
    }

    SymMap *map = SymMap_new(NULL, 100);
    map->put(map, table->name, table, sizeof(SymTable));
    SymTable *out = map->get(map, table->name);

    CU_ASSERT(out != NULL);
    if (out) {
        CU_ASSERT(out->columns != NULL);
        if (out->columns) {
            SymIterator *iter = out->columns->iterator(out->columns);
            while (iter->hasNext(iter)) {
                SymColumn *column = (SymColumn *) iter->next(iter);
                CU_ASSERT(column->name != NULL);
                if (column->name) {
                    CU_ASSERT(strcmp(column->name, columnNames[iter->index]) == 0);
                }
            }
            iter->destroy(iter);
        }
    }

    table->destroy(table);
    map->destroy(map);
}

int SymMapTest_CUnit() {
    CU_pSuite suite = CU_add_suite("SymMapTest", NULL, NULL);
    if (suite == NULL) {
        return CUE_NOSUITE;
    }

    if (CU_add_test(suite, "SymMapTest_test1", SymMapTest_test1) == NULL ||
            CU_add_test(suite, "SymMapTest_test2", SymMapTest_test2) == NULL ||
            CU_add_test(suite, "SymMapTest_test3", SymMapTest_test3) == NULL) {
        return CUE_NOTEST;
    }
    return CUE_SUCCESS;
}
