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
#include "transport/file/OfflineFileSyncOutgoingTransport.h"

char *SymOfflineFileSyncOutgoingTransport_getFileName(SymOfflineFileSyncOutgoingTransport *this) {
    long currentTimeMillis;
    time(&currentTimeMillis);
    currentTimeMillis *= 1000;

    return SymStringUtils_format("%s/%s-%s_to_%s-%s_%ld", this->offlineOutgoingDir,
            this->localNode->nodeGroupId, this->localNode->nodeId,  this->remoteNode->nodeGroupId, this->remoteNode->nodeId, currentTimeMillis);
}

long SymOfflineFileSyncOutgoingTransport_process(SymOfflineFileSyncOutgoingTransport *this, SymDataProcessor *processor) {
    SymFileUtils_mkdir(this->offlineOutgoingDir);

    char* fileNameBase = SymOfflineFileSyncOutgoingTransport_getFileName(this);
    char *tmpFileName = "./tmp/staging/filesync_outgoing/filesync.zip";
    char *zipFileName = SymStringUtils_format("%s%s", fileNameBase, ".zip");

    SymLog_debug("Writing file %s", tmpFileName);

    long result = SYM_TRANSPORT_SC_SERVICE_UNAVAILABLE;

    SymLog_debug("Rename '%s' to '%s'", tmpFileName, zipFileName);
    int renameResult = rename(tmpFileName, zipFileName);
    if (renameResult == 0) {
        result = SYM_OFFLINE_TRANSPORT_OK;
    } else {
        SymLog_warn("Failed to rename '%s' to '%s' %s", tmpFileName, zipFileName, strerror(errno));
        result = SYM_TRANSPORT_SC_SERVICE_UNAVAILABLE;
    }

    free(zipFileName);
    free(fileNameBase);

    return result;
}

void SymOfflineFileSyncOutgoingTransport_destroy(SymOfflineFileSyncOutgoingTransport *this) {
    free(this);
}

SymOfflineFileSyncOutgoingTransport * SymOfflineFileSyncOutgoingTransport_new(SymOfflineFileSyncOutgoingTransport *this, SymNode *remoteNode, SymNode *localNode,
        char *offlineOutgoingDir) {
    if (this == NULL) {
        this = (SymOfflineFileSyncOutgoingTransport *) calloc(1, sizeof(SymOfflineFileSyncOutgoingTransport));
    }
    SymOutgoingTransport *super = (SymOutgoingTransport *)&this->super;
    this->remoteNode = remoteNode;
    this->localNode = localNode;
    this->offlineOutgoingDir = offlineOutgoingDir;

    super->process = (void *) &SymOfflineFileSyncOutgoingTransport_process;
    super->destroy = (void *) &SymOfflineFileSyncOutgoingTransport_destroy;
    return this;
}
