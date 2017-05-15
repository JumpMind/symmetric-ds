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
#include "transport/file/OfflineFileSyncIncomingTransport.h"

long SymOfflineFileSyncIncomingTransport_process(SymOfflineFileSyncIncomingTransport *this, SymDataProcessor *processor, SymRemoteNodeStatus *status) {
    char* fileName = SymFileIncomingTransport_getIncomingFile(this, ".zip");
    if (!fileName || SymStringUtils_isBlank(fileName)) {
        SymLog_debug("No incoming files found at '%s'", this->offlineIncomingDir);
        return SYM_TRANSPORT_SC_SERVICE_UNAVAILABLE;
    }
    char *path = SymStringUtils_format("%s/%s", this->offlineIncomingDir, fileName);

    unsigned short success = 1;

    char *tmpFileName = "./tmp/staging/filesync_incoming/filesync.zip";

    char* cmd = SymStringUtils_format("/bin/cp -fp \"%s\" \"%s\"", path, tmpFileName);
    SymLog_debug("Exec cmd %s", cmd);
    system(cmd);
    free(cmd);
    cmd = NULL;

    if (success) {
        if (SymStringUtils_isNotBlank(this->offlineArchiveDir)) {
            SymFileUtils_mkdir(this->offlineArchiveDir);
            char *archivePath = SymStringUtils_format("%s/%s", this->offlineArchiveDir, fileName);
            int result = rename(path, archivePath);
            if (result != 0) {
                SymLog_warn("Failed to archive '%s' to '%s' %s", path, archivePath, strerror(errno));
            }
        } else {
            int result = remove(path);
            if (result != 0) {
                SymLog_warn("Failed to delete '%s' %s", path, strerror(errno));
            }
        }
    } else if (SymStringUtils_isNotBlank(this->offlineErrorDir)) {
        SymFileUtils_mkdir(this->offlineErrorDir);
        char *errorPath = SymStringUtils_format("%s/%s", this->offlineErrorDir, fileName);
          int result = rename(path, errorPath);
          if (result != 0) {
              SymLog_warn("Failed to archive '%s' to '%s' %s", path, errorPath, strerror(errno));
          }
    }

    return SYM_TRANSPORT_OK;
}


void SymOfflineFileSyncIncomingTransport_destroy(SymOfflineFileSyncIncomingTransport *this) {
    free(this);
}

SymOfflineFileSyncIncomingTransport * SymOfflineFileSyncIncomingTransport_new(SymOfflineFileSyncIncomingTransport *this, SymNode *remoteNode, SymNode *localNode,
        char *offlineIncomingDir, char *offlineArchiveDir, char *offlineErrorDir) {
    if (this == NULL) {
        this = (SymOfflineFileSyncIncomingTransport *) calloc(1, sizeof(SymOfflineFileSyncIncomingTransport));
    }
    SymIncomingTransport *super = &this->super;
    super->process = (void *) &SymOfflineFileSyncIncomingTransport_process;
    super->destroy = (void *) &SymOfflineFileSyncIncomingTransport_destroy;

    this->remoteNode = remoteNode;
    this->localNode = localNode;
    this->offlineIncomingDir = offlineIncomingDir;
    this->offlineArchiveDir = offlineArchiveDir;
    this->offlineErrorDir = offlineErrorDir;
    return this;
}
