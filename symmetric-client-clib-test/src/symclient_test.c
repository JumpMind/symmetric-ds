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
#include "file/FileTriggerTracker.h"
#include "file/DirectorySnapshot.h"

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
            // SymStringUtilsTest_CUnit() != CUE_SUCCESS ||
            //SymHexTest_CUnit() != CUE_SUCCESS ||
            //SymBase64Test_CUnit() != CUE_SUCCESS ||
            1==1) {

        //        SymFileTriggerTracker *tracker =
        //                SymFileTriggerTracker_new(NULL);
        //        printf("Running tracker...");
        //        tracker->trackChanges(tracker);

        printf("manuals\n");
        SymList *list1 =
                SymFileUtils_listFilesRecursive("./tmp/manuals");
        printf("manuals2\n");
        SymList *list2 =
                SymFileUtils_listFilesRecursive("./tmp/manuals2");
        SymDirectorySnapshot *snapshot1 = SymDirectorySnapshot_newWithFileList(NULL, list1);
        SymDirectorySnapshot *snapshot2 = SymDirectorySnapshot_newWithFileList(NULL, list2);
        SymDirectorySnapshot *changes = snapshot1->diff(snapshot1, snapshot2);
        if (changes->fileSnapshots->size == 0) {
            SymLog_info("No Changes detected");
        } else {
            int i;
            for (i = 0; i < changes->fileSnapshots->size; ++i) {
                SymFileSnapshot *change = changes->fileSnapshots->get(changes->fileSnapshots, i);
                SymLog_info("Change: %s (%s)", change->fileName, change->lastEventType);

            }
        }


        CU_cleanup_registry();
        return CU_get_error();
    }

    CU_basic_set_mode(CU_BRM_VERBOSE);
    CU_basic_run_tests();
    CU_cleanup_registry();
    return CU_get_error();
}
