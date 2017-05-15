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
#ifndef SYM_OFFLINEFILESYNCINCOMINGTRANSPORT_H_
#define SYM_OFFLINEFILESYNCINCOMINGTRANSPORT_H_

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <curl/curl.h>
#include <util/Properties.h>
#include <dirent.h>
#include "model/Node.h"
#include "transport/http/HttpTransportManager.h"
#include "transport/IncomingTransport.h"
#include "transport/file/FileIncomingTransport.h"
#include "common/Log.h"
#include "util/StringUtils.h"
#include "util/StringArray.h"
#include "util/FileUtils.h"

typedef struct SymOfflineFileSyncIncomingTransport {
    SymIncomingTransport super;
    SymNode *remoteNode;
    SymNode *localNode;
    char *offlineIncomingDir;
    char *offlineArchiveDir;
    char *offlineErrorDir;
} SymOfflineFileSyncIncomingTransport;

SymOfflineFileSyncIncomingTransport * SymOfflineFileSyncIncomingTransport_new(SymOfflineFileSyncIncomingTransport *this, SymNode *remoteNode, SymNode *localNode,
        char *offlineIncomingDir, char *offlineArchiveDir, char *offlineErrorDir);

#endif /* SYM_OFFLINEFILESYNCINCOMINGTRANSPORT_H_ */
