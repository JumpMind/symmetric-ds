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
#include "file/DirectorySnapshot.h"


void SymDirectorySnapshot_destroy(SymDirectorySnapshot *this) {
    this->fileSnapshots->destroy(this->fileSnapshots);
    free(this);
}

SymDirectorySnapshot *SymDirectorySnapshot_diff(SymDirectorySnapshot *this, SymDirectorySnapshot *currentSnapshot) {
    SymDirectorySnapshot *changes = SymDirectorySnapshot_new(NULL);
    SymFileSnapshot *currentFileSnapshot = NULL;
    SymFileSnapshot *previousFileSnapshot = NULL;
    SymFileSnapshot *fileSnapshot = NULL;

    int i;
    for (i = 0; i < currentSnapshot->fileSnapshots->size; ++i) {
        currentFileSnapshot = currentSnapshot->fileSnapshots->get(currentSnapshot->fileSnapshots, i);
        previousFileSnapshot = NULL;
        int j;
        for (j = 0; j < this->fileSnapshots->size; ++j) {
            fileSnapshot = this->fileSnapshots->get(this->fileSnapshots, j);

            if (SymStringUtils_equals(currentFileSnapshot->relativeDir, fileSnapshot->relativeDir)
                    && SymStringUtils_equals(currentFileSnapshot->fileName, fileSnapshot->fileName)) {
                previousFileSnapshot = fileSnapshot;
                break;
            }
        }
        currentFileSnapshot->triggerId = this->fileTriggerRouter->triggerId;
        currentFileSnapshot->routerId = this->fileTriggerRouter->routerId;
        currentFileSnapshot->channelId = this->fileTriggerRouter->fileTrigger->channelId;
        currentFileSnapshot->reloadChannelId = this->fileTriggerRouter->fileTrigger->reloadChannelId;

        if (previousFileSnapshot == NULL) {
            currentFileSnapshot->lastEventType = "C";
            changes->fileSnapshots->add(changes->fileSnapshots, currentFileSnapshot);
        } else {
            if (currentFileSnapshot->fileModifiedTime != previousFileSnapshot->fileModifiedTime
                    || currentFileSnapshot->fileSize != previousFileSnapshot->fileSize) {
                currentFileSnapshot->lastEventType = "M";
                changes->fileSnapshots->add(changes->fileSnapshots, currentFileSnapshot);
            }
        }
    }

    // Look for deletes.
    for (i = 0; i < this->fileSnapshots->size; ++i) {
        SymFileSnapshot *previousFileSnapshot = this->fileSnapshots->get(this->fileSnapshots, i);
        int j;
        SymFileSnapshot *currentFileSnapshot = NULL;
        for (j = 0; j < currentSnapshot->fileSnapshots->size; ++j) {
            SymFileSnapshot *fileSnapshot = currentSnapshot->fileSnapshots->get(currentSnapshot->fileSnapshots, j);
            if (SymStringUtils_equals(previousFileSnapshot->fileName, fileSnapshot->fileName)) {
                currentFileSnapshot = fileSnapshot;
                break;
            }
        }
        if (currentFileSnapshot == NULL) {
            previousFileSnapshot->lastEventType = "D";
            previousFileSnapshot->triggerId = this->fileTriggerRouter->triggerId;
            previousFileSnapshot->routerId = this->fileTriggerRouter->routerId;
            previousFileSnapshot->channelId = this->fileTriggerRouter->fileTrigger->channelId;
            previousFileSnapshot->reloadChannelId = this->fileTriggerRouter->fileTrigger->reloadChannelId;
            changes->fileSnapshots->add(changes->fileSnapshots, previousFileSnapshot);
        }
    }

    return changes;
}

SymDirectorySnapshot * SymDirectorySnapshot_newWithFileList(SymDirectorySnapshot *this, SymList *fileList) {
    this = SymDirectorySnapshot_new(this);

    this->fileSnapshots = SymList_new(NULL);

    int i;
    for (i = 0; i < fileList->size; ++i) {
        SymFileEntry *entry = fileList->get(fileList, i);
        SymFileSnapshot *fileSnapshot = SymFileSnapshot_new(NULL);
        fileSnapshot->relativeDir = entry->directory;
        fileSnapshot->fileName = entry->fileName;
        if (entry->fileModificationTime != NULL) {
            fileSnapshot->fileModifiedTime = entry->fileModificationTime->time;
        }
        fileSnapshot->fileSize = entry->fileSize;
        this->fileSnapshots->add(this->fileSnapshots, fileSnapshot);
    }

    return this;
}

SymDirectorySnapshot * SymDirectorySnapshot_new(SymDirectorySnapshot *this) {
    if (this == NULL) {
        this = (SymDirectorySnapshot *) calloc(1, sizeof(SymDirectorySnapshot));
    }
    this->fileSnapshots = SymList_new(NULL);
    this->diff = (void *) &SymDirectorySnapshot_diff;
    this->destroy = (void *) &SymDirectorySnapshot_destroy;
    return this;
}
