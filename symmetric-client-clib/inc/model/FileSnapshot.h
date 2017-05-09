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
#ifndef SYM_FILESNAPSHOT_H_
#define SYM_FILESNAPSHOT_H_

#include <stdlib.h>
#include "util/Date.h"


typedef struct SymFileSnapshot {
    char *triggerId;
    char *routerId;
    char *relativeDir;
    char *fileName;
    char *lastEventType;
    char *channelId;
    char *reloadChannelId;
    long crc32Checksum;
    long oldCrc32Checksum;
    long fileSize;
    long fileModifiedTime;
    SymDate *createTime;
    char *lastUpdateBy;
    SymDate *lastUpdateTime;
    void (*destroy)(struct SymFileSnapshot *this);
} SymFileSnapshot;

SymFileSnapshot * SymFileSnapshot_new(SymFileSnapshot *this);

#endif /* SYM_FILESNAPSHOT_H_ */
