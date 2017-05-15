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
#ifndef SYM_FILETRIGGER_H_
#define SYM_FILETRIGGER_H_

#include <stdlib.h>
#include "common/Constants.h"
#include "util/Date.h"
#include "model/FileSnapshot.h"
#include "util/StringUtils.h"

#define SYM_FILE_CTL_EXTENSION ".ctl"


typedef struct SymFileTrigger {
    char *triggerId;
    char *channelId;
    char *reloadChannelId;
    char *baseDir;
    unsigned short recurse;
    char *includesFiles;
    char *excludesFiles;
    unsigned short syncOnCreate;
    unsigned short syncOnModified;
    unsigned short syncOnDelete;
    unsigned short syncOnCtlFile;
    unsigned short deleteAfterSync;
    char *beforeCopyScript;
    char *afterCopyScript;
    SymDate *createTime;
    char *lastUpdateBy;
    SymDate *lastUpdateTime;

    char* (*getPath)(struct SymFileTrigger *this, SymFileSnapshot *snapshot);
    void (*destroy)(struct SymFileTrigger *this);
} SymFileTrigger;

SymFileTrigger * SymFileTrigger_new(SymFileTrigger *this);

#endif
