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
#ifndef SYM_FILETRIGGERTRACKER_H_
#define SYM_FILETRIGGERTRACKER_H_

#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <dirent.h>
#include <time.h>
#include "file/DirectorySnapshot.h"
#include "model/FileTriggerRouter.h"
#include "util/FileUtils.h"

struct SymEngine;

typedef struct SymFileTriggerTracker {
    SymFileTriggerRouter *fileTriggerRouter;
    SymDirectorySnapshot *lastSnapshot;
    SymDirectorySnapshot *changesSinceLastSnapshot;
    struct SymEngine *engine;
    unsigned short useCrc;
    SymDirectorySnapshot * (*trackChanges)(struct SymFileTriggerTracker *this);
    SymDirectorySnapshot * (*takeSnapshot)(struct SymFileTriggerTracker *this);
    void (*destroy)(struct SymFileTriggerTracker *this);
} SymFileTriggerTracker;

SymFileTriggerTracker * SymFileTriggerTracker_new(SymFileTriggerTracker *this);

#endif
