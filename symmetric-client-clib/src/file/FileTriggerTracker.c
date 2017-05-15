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
#include "file/FileTriggerTracker.h"
#include "core/SymEngine.h"

void SymFileTriggerTracker_destroy(SymFileTriggerTracker *this) {
    free(this);
}

SymDirectorySnapshot * SymFileTriggerTracker_trackChanges(SymFileTriggerTracker *this) {
    time_t startTime = time(NULL);

    SymDirectorySnapshot *currentSnapshot = this->takeSnapshot(this);
    SymDirectorySnapshot *changes = this->lastSnapshot->diff(this->lastSnapshot, currentSnapshot);
    this->lastSnapshot = currentSnapshot;

    time_t runTime = (time(NULL)-startTime);

    SymLog_info("Tracked %d files in %d seconds.  Found %d files changed.",
            this->lastSnapshot->fileSnapshots->size, runTime, changes->fileSnapshots->size);
    return changes;
}

SymDirectorySnapshot * SymFileTriggerTracker_takeSnapshot(SymFileTriggerTracker *this) {
    if (!SymFileUtils_exists(this->fileTriggerRouter->fileTrigger->baseDir)) {
        SymLog_warn("File Sync baseDir does not exist %s", this->fileTriggerRouter->fileTrigger->baseDir);
    }

    SymList *fileList =
            SymFileUtils_listFilesRecursive(this->fileTriggerRouter->fileTrigger->baseDir);

    SymDirectorySnapshot *snapshot = SymDirectorySnapshot_newWithFileList(NULL, fileList);
    return snapshot;
}

SymFileTriggerTracker * SymFileTriggerTracker_new(SymFileTriggerTracker *this) {
    if (this == NULL) {
        this = (SymFileTriggerTracker *) calloc(1, sizeof(SymFileTriggerTracker));
    }
    this->trackChanges = (void *) &SymFileTriggerTracker_trackChanges;
    this->takeSnapshot = (void *) &SymFileTriggerTracker_takeSnapshot;
    this->destroy = (void *) &SymFileTriggerTracker_destroy;
    return this;
}

