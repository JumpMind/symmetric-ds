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

void SymStringBuilderTest_test1() {
    SymStringBuilder *sb = SymStringBuilder_newWithString("hello");
    sb->append(sb, ",hola");
    sb->append(sb, ",salute");
    sb->append(sb, ",bonjour");
    sb->append(sb, ",guten tag");
    sb->append(sb, ",ciao");
    CU_ASSERT(strcmp(sb->str, "hello,hola,salute,bonjour,guten tag,ciao") == 0);
    sb->destroy(sb);
}

void SymStringBuilderTest_test2() {
    SymStringBuilder *sb = SymStringBuilder_newWithSize(1);
    sb->append(sb, "como ");
    sb->append(sb, "t'allez-vous");
    CU_ASSERT(strcmp(sb->str, "como t'allez-vous") == 0);
    sb->destroy(sb);
}

void SymStringBuilderTest_test3() {
    SymStringBuilder *sb = SymStringBuilder_new();
    sb->append(sb, "1");
    sb->append(sb, "2");
    sb->append(sb, "3");
    CU_ASSERT(strcmp(sb->str, "123") == 0);
    sb->destroy(sb);
}

void SymStringBuilderTest_test4() {
    SymStringBuilder *sb = SymStringBuilder_new();
    sb->append(sb, "1");
    sb->append(sb, "2");
    sb->append(sb, "3");
    char *value = sb->destroyAndReturn(sb);
    CU_ASSERT(strcmp(value, "123") == 0);
    free(value);
}

void SymStringBuilderTest_test_long() {
    long batchId = -9999;
    SymStringBuilder *sb = SymStringBuilder_new();
    sb->append(sb, "batch-");
    sb->appendf(sb, "%ld", batchId);
    sb->append(sb, "=OK");
    char *value = sb->destroyAndReturn(sb);
    CU_ASSERT(strcmp(value, "batch--9999=OK") == 0)
    free(value);
}

/**
 * Provide separate appendfv "exec" testing function here because it
 * appears unsafe to construct va_list by hand.
 */
SymStringBuilder * SymStringBuilderTest_exec_appendfv(const char *fmt, ...) {
	SymStringBuilder *sb = SymStringBuilder_new();
    va_list arglist;
    va_start(arglist, fmt);
    sb->appendfv(sb, fmt, arglist);
    va_end(arglist);
    return sb;
}

void SymStringBuilderTest_test_appendfv() {
	{
	    SymStringBuilder *sb = SymStringBuilderTest_exec_appendfv("test with no args.");
	    char *value = sb->destroyAndReturn(sb);
	    CU_ASSERT(strcmp(value, "test with no args.") == 0)
	    free(value);
	}
	{
	    long batchId = -9999;
	    SymStringBuilder *sb = SymStringBuilderTest_exec_appendfv("long test %ld", batchId);
	    char *value = sb->destroyAndReturn(sb);
	    CU_ASSERT(strcmp(value, "long test -9999") == 0)
	    free(value);
	}
	{
	    SymStringBuilder *sb = SymStringBuilderTest_exec_appendfv("2 string test (%s) (%s)", "string 1", "string 2");
	    char *value = sb->destroyAndReturn(sb);
	    CU_ASSERT(strcmp(value, "2 string test (string 1) (string 2)") == 0)
	    free(value);
	}
}

int SymStringBuilderTest_CUnit() {
    CU_pSuite suite = CU_add_suite("SymStringBuilderTest", NULL, NULL);
    if (suite == NULL) {
        return CUE_NOSUITE;
    }

    if (CU_add_test(suite, "SymStringBuilderTest_test1", SymStringBuilderTest_test1) == NULL ||
            CU_add_test(suite, "SymStringBuilderTest_test2", SymStringBuilderTest_test2) == NULL ||
            CU_add_test(suite, "SymStringBuilderTest_test3", SymStringBuilderTest_test3) == NULL ||
            CU_add_test(suite, "SymStringBuilderTest_test4", SymStringBuilderTest_test4) == NULL ||
            CU_add_test(suite, "SymStringBuilderTest_test_long", SymStringBuilderTest_test_long) == NULL ||
			CU_add_test(suite, "SymStringBuilderTest_test_appendfv", SymStringBuilderTest_test_appendfv) == NULL) {
        return CUE_NOTEST;
    }
    return CUE_SUCCESS;
}
