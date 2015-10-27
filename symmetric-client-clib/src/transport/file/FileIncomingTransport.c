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
#include "transport/file/FileIncomingTransport.h"

char* SymFileIncomingTransport_getIncomingFile(SymFileIncomingTransport *this, char *extension) {
    DIR *dir;
    struct dirent *dirEntries;
    dir = opendir(this->offlineIncomingDir);

    if (dir) {
        while ((dirEntries = readdir(dir)) != NULL) {
            char *entry = dirEntries->d_name;
            if (SymStringUtils_endsWith(entry, extension)) {
                return entry;
            }
        }
        closedir(dir);
    }

    return NULL;
}

long SymFileIncomingTransport_process(SymFileIncomingTransport *this, SymDataProcessor *processor) {
    FILE *file;
    int BUFFER_SIZE = 1024;
    char inputBuffer[BUFFER_SIZE];

    char *fileName = SymFileIncomingTransport_getIncomingFile(this, ".csv");

    file = fopen(fileName,"r");

    if (!file) {
        SymLog_warn("Failed to load file %s", this->offlineIncomingDir);
        return CURLE_HTTP_NOT_FOUND;
    }

    int counter = 0;
    while (fgets(inputBuffer, BUFFER_SIZE, file) != NULL) {
        processor->process(processor, inputBuffer, strlen(inputBuffer), counter++);

    }

    return CURLE_OK;
}

void SymFileIncomingTransport_destroy(SymFileIncomingTransport *this) {
    free(this->remoteNode);
    free(this);
}

SymFileIncomingTransport * SymFileIncomingTransport_new(SymFileIncomingTransport *this, SymNode *remoteNode, SymNode *localNode,
        char *offlineIncomingDir, char *offlineArchiveDir, char *offlineErrorDir) {
    if (this == NULL) {
        this = (SymFileIncomingTransport *) calloc(1, sizeof(SymFileIncomingTransport));
    }
    SymIncomingTransport *super = &this->super;
    super->process = (void *) &SymFileIncomingTransport_process;
    super->destroy = (void *) &SymFileIncomingTransport_destroy;

    this->remoteNode = remoteNode;
    this->localNode = localNode;
    this->offlineIncomingDir = offlineIncomingDir;
    this->offlineArchiveDir = offlineArchiveDir;
    this->offlineErrorDir = offlineErrorDir;
    return this;
}
