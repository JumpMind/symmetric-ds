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
#ifndef SYM_OFFLINEFILESYNCOUTGOINGTRANSPORT_H_
#define SYM_OFFLINEFILESYNCOUTGOINGTRANSPORT_H_

#include <stdlib.h>
#include <errno.h>
#include "common/Log.h"
#include "model/Node.h"
#include "service/ParameterService.h"
#include "transport/OutgoingTransport.h"
#include "util/List.h"
#include "util/StringBuilder.h"
#include "util/StringUtils.h"
#include "util/StringArray.h"
#include "util/FileUtils.h"
#include "transport/TransportManager.h"
#include "web/WebConstants.h"

typedef struct SymOfflineFileSyncOutgoingTransport {
    SymOutgoingTransport super;
    SymNode *remoteNode;
    SymNode *localNode;
    char *offlineOutgoingDir;
} SymOfflineFileSyncOutgoingTransport;

SymOfflineFileSyncOutgoingTransport * SymOfflineFileSyncOutgoingTransport_new(SymOfflineFileSyncOutgoingTransport *this,
        SymNode *remoteNode, SymNode *localNode, char *offlineOutgoingDir);

#endif /* SYM_OFFLINEFILESYNCOUTGOINGTRANSPORT_H_ */
