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

    SymStringArray *files = SymStringArray_new(NULL);

    char *startFilter = SymStringUtils_format("%s-%s", this->remoteNode->nodeGroupId, this->remoteNode->nodeId);

    if (dir) {
        while ((dirEntries = readdir(dir)) != NULL) {
            char *entry = dirEntries->d_name;
            if (SymStringUtils_startsWith(entry, startFilter) && SymStringUtils_endsWith(entry, extension)) {
                files->add(files, entry);
            }
        }
        closedir(dir);
    }
    else {
        SymLog_error("Failed to open incoming directory '%s'", this->offlineIncomingDir);
    }

    char *firstFile = NULL;

    if (files->size > 0) {
        files->sort(files);
        firstFile = files->get(files, 0);
    }

    free(startFilter);
    files->destroy(files);

    if (firstFile) {
        char *path = SymStringUtils_format("%s/%s", this->offlineIncomingDir, firstFile);
        return path;
    } else {
        return firstFile;
    }
}

long SymFileIncomingTransport_process(SymFileIncomingTransport *this, SymDataProcessor *processor) {
    FILE *file;
    int BUFFER_SIZE = 2048;
    char inputBuffer[BUFFER_SIZE];

    char *fileName = SymFileIncomingTransport_getIncomingFile(this, ".csv");

    file = fopen(fileName,"r");

    if (!file) {
        SymLog_warn("Failed to load file %s", this->offlineIncomingDir);
        return SYM_TRANSPORT_SC_SERVICE_UNAVAILABLE;
    }

    processor->open(processor);

    int count;
    while ((count = fread(inputBuffer, sizeof(char), BUFFER_SIZE, file)) > 0) {
        processor->process(processor, inputBuffer, sizeof(char), count);
    }

    processor->close(processor);

    return SYM_TRANSPORT_OK;
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
