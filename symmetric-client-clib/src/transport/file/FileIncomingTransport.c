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

    SymFileUtils_mkdir(this->offlineIncomingDir);

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

    // seems we have stack memory here for firstFile.
    // This memory gets overwritten after it's returned.
    if (firstFile) {
        firstFile = SymStringUtils_format("%s", firstFile);
    }

    free(startFilter);
    files->destroy(files);

    return firstFile;
}

long SymFileIncomingTransport_process(SymFileIncomingTransport *this, SymDataProcessor *processor, SymRemoteNodeStatus *status) {
    FILE *file;
    int BUFFER_SIZE = 2048;
    char inputBuffer[BUFFER_SIZE];

    char *fileName = SymFileIncomingTransport_getIncomingFile(this, ".csv");
    if (!fileName || SymStringUtils_isBlank(fileName)) {
        SymLog_info("No incoming files found at '%s'", this->offlineIncomingDir);
        return SYM_TRANSPORT_SC_SERVICE_UNAVAILABLE;
    }
    char *path = SymStringUtils_format("%s/%s", this->offlineIncomingDir, fileName);

    file = fopen(path,"r");

    if (!file) {
        SymLog_warn("Failed to load file '%s'", path);
        return SYM_TRANSPORT_SC_SERVICE_UNAVAILABLE;
    }

    processor->open(processor);

    unsigned short success = 1;

    int count;
    while ((count = fread(inputBuffer, sizeof(char), BUFFER_SIZE, file)) > 0) {
        int size = processor->process(processor, inputBuffer, sizeof(char), count);
        if (size == 0) {
            SymLog_warn("Failed to process file %s", this->offlineIncomingDir);
            success = 0;
            break;
        }
    }

    processor->close(processor);
    fclose(file);
    file = NULL;

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

void SymFileIncomingTransport_destroy(SymFileIncomingTransport *this) {
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
