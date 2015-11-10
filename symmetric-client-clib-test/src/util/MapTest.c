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

static SymMap * SymMapTest_test_createStringMap(int keySize, char **keys, char **values, int mapSize) {
    SymMap *map = SymMap_new(NULL, mapSize);
    int i;
    for (i = 0; i < keySize; i++) {
        map->put(map, keys[i], values[i]);
    }
    return map;
}

static void SymMapTest_test_stringmap(int keySize, char **keys, char **values, int mapSize) {
    int i;
    SymMap *map = SymMapTest_test_createStringMap(keySize, keys, values, mapSize);

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
    map->put(map, table->name, table);
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

unsigned short SymMapTest_stringListContains(SymList * list, char * string) {
	int i;
	for (i = 0; i < list->size; i++) {
		if (strcmp(list->get(list, i), string) == 0) {
			return 1;
		}
	}

	return 0;
}

unsigned short SymMapTest_entryListContainsKey(SymList * entryList, char * string) {
	int i;
	for (i = 0; i < entryList->size; i++) {
		if (strcmp(((SymMapEntry*)entryList->get(entryList, i))->key, string) == 0) {
			return 1;
		}
	}

	return 0;
}

unsigned short SymMapTest_entryListContainsValue(SymList * entryList, char * string) {
	int i;
	for (i = 0; i < entryList->size; i++) {
		if (strcmp(((SymMapEntry*)entryList->get(entryList, i))->value, string) == 0) {
			return 1;
		}
	}

	return 0;
}

void SymMapTest_testValuesEmpty() {
    SymMap *map = SymMap_new(NULL, 100);
    SymList *valuesList = map->values(map);

    CU_ASSERT(valuesList->size == 0);

    map->destroy(map);
}

void SymMapTest_testValuesSingleValue() {
    SymMap *map = SymMap_new(NULL, 100);
    map->put(map, "key1", "value1");
    SymList *valuesList = map->values(map);

    CU_ASSERT(strcmp(valuesList->get(valuesList, 0), "value1") == 0);
    map->destroy(map);
}


void SymMapTest_testValuesMultiValue() {

	int KEY_COUNT = 5;

    char *keys[] = { "key3", "key2", "key1", "key5", "key4" };
    char *values[] = { "value3", "value2", "value1", "value5", "value4" };

    SymMap *maps[] = {
    		SymMapTest_test_createStringMap(KEY_COUNT, keys, values, 1),
			SymMapTest_test_createStringMap(KEY_COUNT, keys, values, 4),
			SymMapTest_test_createStringMap(KEY_COUNT, keys, values, 8),
			SymMapTest_test_createStringMap(KEY_COUNT, keys, values, 100)
    };

    int i;
    for (i = 0; i < 4; i++) {
    	SymMap *map = maps[i];
	    SymList *valueList = map->values(map);

	    CU_ASSERT(valueList->size == KEY_COUNT);

	    CU_ASSERT(SymMapTest_stringListContains(valueList, "NOT FOUND VALUE") == 0);
	    CU_ASSERT(SymMapTest_stringListContains(valueList, "value1"));
	    CU_ASSERT(SymMapTest_stringListContains(valueList, "value2"));
	    CU_ASSERT(SymMapTest_stringListContains(valueList, "value3"));
	    CU_ASSERT(SymMapTest_stringListContains(valueList, "value4"));
	    CU_ASSERT(SymMapTest_stringListContains(valueList, "value5"));

	    map->destroy(map);
    }
}

void SymMapTest_testEntriesEmpty() {
    SymMap *map = SymMap_new(NULL, 100);
    SymList *entryList = map->entries(map);

    CU_ASSERT(entryList->size == 0);

    map->destroy(map);
}

void SymMapTest_testEntriesSingleValue() {
    SymMap *map = SymMap_new(NULL, 100);
    map->put(map, "key1", "value1");
    SymList *entryList = map->entries(map);

    CU_ASSERT(strcmp(((SymMapEntry*)entryList->get(entryList, 0))->key, "key1") == 0);
    CU_ASSERT(strcmp(((SymMapEntry*)entryList->get(entryList, 0))->value, "value1") == 0);
    map->destroy(map);
}


void SymMapTest_testEntriesMultiValue() {

	int KEY_COUNT = 5;

    char *keys[] = { "key3", "key2", "key1", "key5", "key4" };
    char *values[] = { "value3", "value2", "value1", "value5", "value4" };

    SymMap *maps[] = {
    		SymMapTest_test_createStringMap(KEY_COUNT, keys, values, 1),
			SymMapTest_test_createStringMap(KEY_COUNT, keys, values, 4),
			SymMapTest_test_createStringMap(KEY_COUNT, keys, values, 8),
			SymMapTest_test_createStringMap(KEY_COUNT, keys, values, 100)
    };

    int i;
    for (i = 0; i < 4; i++) {
    	SymMap *map = maps[i];
    	SymList *entryList = map->entries(map);

	    CU_ASSERT(entryList->size == KEY_COUNT);

	    CU_ASSERT(SymMapTest_entryListContainsKey(entryList, "NOT FOUND KEY") == 0);
	    CU_ASSERT(SymMapTest_entryListContainsValue(entryList, "NOT FOUND VALUE") == 0);
	    CU_ASSERT(SymMapTest_entryListContainsKey(entryList, "key1"));
	    CU_ASSERT(SymMapTest_entryListContainsValue(entryList, "value1"));
	    CU_ASSERT(SymMapTest_entryListContainsKey(entryList, "key2"));
	    CU_ASSERT(SymMapTest_entryListContainsValue(entryList, "value2"));
	    CU_ASSERT(SymMapTest_entryListContainsKey(entryList, "key3"));
	    CU_ASSERT(SymMapTest_entryListContainsValue(entryList, "value3"));
	    CU_ASSERT(SymMapTest_entryListContainsKey(entryList, "key4"));
	    CU_ASSERT(SymMapTest_entryListContainsValue(entryList, "value4"));
	    CU_ASSERT(SymMapTest_entryListContainsKey(entryList, "key5"));
	    CU_ASSERT(SymMapTest_entryListContainsValue(entryList, "value5"));

	    map->destroy(map);
    }
}


void SymMapTest_testKeysEmpty() {
    SymMap *map = SymMap_new(NULL, 100);
    SymStringArray *keyList = map->keys(map);

    CU_ASSERT(keyList->size == 0);

    map->destroy(map);
}

void SymMapTest_testKeysSingleValue() {
    SymMap *map = SymMap_new(NULL, 100);
    map->put(map, "key1", "value1");
    SymStringArray *keyList = map->keys(map);

    CU_ASSERT(strcmp(keyList->get(keyList, 0), "key1") == 0);
    map->destroy(map);
}

void SymMapTest_testKeysMultiValue() {

	int KEY_COUNT = 5;

    char *keys[] = { "key3", "key2", "key1", "key5", "key4" };
    char *values[] = { "value3", "value2", "value1", "value5", "value4" };

    SymMap *maps[] = {
    		SymMapTest_test_createStringMap(KEY_COUNT, keys, values, 1),
			SymMapTest_test_createStringMap(KEY_COUNT, keys, values, 4),
			SymMapTest_test_createStringMap(KEY_COUNT, keys, values, 8),
			SymMapTest_test_createStringMap(KEY_COUNT, keys, values, 100)
    };

    int i;
    for (i = 0; i < 4; i++) {
    	SymMap *map = maps[i];
    	SymStringArray *keyList = map->keys(map);

	    CU_ASSERT(keyList->size == KEY_COUNT);

	    CU_ASSERT(keyList->contains(keyList, "NOT FOUND VALUE") == 0);
	    CU_ASSERT(keyList->contains(keyList, "key1"));
	    CU_ASSERT(keyList->contains(keyList, "key2"));
	    CU_ASSERT(keyList->contains(keyList, "key3"));
	    CU_ASSERT(keyList->contains(keyList, "key4"));
	    CU_ASSERT(keyList->contains(keyList, "key5"));

	    map->destroy(map);
    }
}

void SymMapTest_testRemove() {
    int mapSize = 16;
    SymMap *map = SymMap_new(NULL, mapSize);
    int i;
    for (i = 0; i < mapSize; i++) {
        map->put(map, SymStringUtils_format("%d", i), SymStringUtils_format("value %d", i));
    }
    for (i = 0; i < mapSize; i++) {
        map->remove(map, SymStringUtils_format("%d", i));
    }

    map->remove(map, "1");
}

void SymMapTest_testResetRemove() {
    int mapSize = 16;
    SymMap *map = SymMap_new(NULL, mapSize);
    int i;
    for (i = 0; i < mapSize; i++) {
        map->put(map, SymStringUtils_format("%d", i), SymStringUtils_format("value %d", i));
    }
    CU_ASSERT(SymStringUtils_equals(map->get(map, "1"), "value 1"));
    map->reset(map);
    CU_ASSERT(map->get(map, "1") == NULL);
    map->remove(map, "1");
    CU_ASSERT(map->get(map, "1") == NULL);

    map->put(map, "1", "value 1");
    CU_ASSERT(SymStringUtils_equals(map->get(map, "1"), "value 1"));
    map->remove(map, "1");
    CU_ASSERT(map->get(map, "1") == NULL);
}

int SymMapTest_CUnit() {
    CU_pSuite suite = CU_add_suite("SymMapTest", NULL, NULL);
    if (suite == NULL) {
        return CUE_NOSUITE;
    }

    if (CU_add_test(suite, "SymMapTest_test1", SymMapTest_test1) == NULL ||
            CU_add_test(suite, "SymMapTest_test2", SymMapTest_test2) == NULL ||
            CU_add_test(suite, "SymMapTest_test3", SymMapTest_test3) == NULL ||
			CU_add_test(suite, "SymMapTest_testValuesEmpty", SymMapTest_testValuesEmpty) == NULL ||
			CU_add_test(suite, "SymMapTest_testValuesSingleValue", SymMapTest_testValuesSingleValue) == NULL ||
			CU_add_test(suite, "SymMapTest_testValuesMultiValue", SymMapTest_testValuesMultiValue) == NULL ||
			CU_add_test(suite, "SymMapTest_testEntriesEmpty", SymMapTest_testEntriesEmpty) == NULL ||
			CU_add_test(suite, "SymMapTest_testEntriesSingleValue", SymMapTest_testEntriesSingleValue) == NULL ||
			CU_add_test(suite, "SymMapTest_testEntriesMultiValue", SymMapTest_testEntriesMultiValue) == NULL ||
			CU_add_test(suite, "SymMapTest_testKeysEmpty", SymMapTest_testKeysEmpty) == NULL ||
			CU_add_test(suite, "SymMapTest_testKeysSingleValue", SymMapTest_testKeysSingleValue) == NULL ||
			CU_add_test(suite, "SymMapTest_testKeysMultiValue", SymMapTest_testKeysMultiValue) == NULL ||
			CU_add_test(suite, "SymMapTest_testRemove", SymMapTest_testRemove) == NULL ||
			CU_add_test(suite, "SymMapTest_testResetRemove", SymMapTest_testResetRemove) == NULL ||
			1==0

			) {
        return CUE_NOTEST;
    }
    return CUE_SUCCESS;
}
