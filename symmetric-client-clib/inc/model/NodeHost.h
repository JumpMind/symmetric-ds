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
#ifndef SYM_NODEHOST_H
#define SYM_NODEHOST_H

#include <stdlib.h>

#include "util/Date.h"
#include "util/AppUtils.h"
#include "model/Node.h"

typedef struct SymNodeHost {
    char *nodeId;
    char *hostName;
    char *ipAddress;
    char *osUser;
    char *osName;
    char *osArch;
    char *osVersion;
    int availableProcessors;
    long freeMemoryBytes;
    long totalMemoryBytes;
    long maxMemoryBytes;
    char *javaVersion;
    char *javaVendor;
    char *jdbcVersion;
    char *symmetricVersion;
    char *timezoneOffset;
    SymDate *heartbeatTime;
    SymDate *lastRestartTime;
    SymDate *createTime;

    void (*refresh)(struct SymNodeHost *this);
    void (*destroy)(struct SymNodeHost *this);
} SymNodeHost;

SymNodeHost * SymNodeHost_new(SymNodeHost *this);

#endif
