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
#include "transport/file/FileOutgoingTransport.h"

char *SymFileOutgoingTransport_getFileName(SymFileOutgoingTransport *this) {

    long currentTimeMillis;
    time(&currentTimeMillis);
    currentTimeMillis *= 1000;

    return SymStringUtils_format("%s/%s-%s_to_%s-%s_%ld.tmp", this->offlineOutgoingDir,
            this->localNode->nodeGroupId, this->localNode->nodeId,  this->remoteNode->nodeGroupId, this->remoteNode->nodeId, currentTimeMillis);
}

long SymFileOutgoingTransport_process(SymFileOutgoingTransport *this, SymDataProcessor *processor) {

    processor->open(processor);

    int BUFFER_SIZE = 2048;
    char inputBuffer[BUFFER_SIZE];
    char* fileName = SymFileOutgoingTransport_getFileName(this);
    SymLog_debug("Writing file %s", fileName);

    long result = SYM_TRANSPORT_SC_SERVICE_UNAVAILABLE;

    FILE *file = fopen(SymFileOutgoingTransport_getFileName(this), "w");

    if (file) {
        int size;
        while ((size = processor->process(processor, inputBuffer, 1, BUFFER_SIZE)) > 0) {
            fprintf(file, "%.*s", size, inputBuffer);
        }
        fclose(file);
        result = SYM_TRANSPORT_OK;
    } else {
        SymLog_error("Failed to open file for writing. %s", fileName);
        result = SYM_TRANSPORT_SC_SERVICE_UNAVAILABLE;
    }
    free(fileName);

    // TODO NEED list of batch id's written.

    SymList *batchIds = processor->getBatchesProcessed(processor);
    SymStringBuilder *buff = SymStringBuilder_new(NULL);
    int i;
    for (i = 0; i < batchIds->size; ++i) {
        char* batchId = batchIds->get(batchIds, i);
        buff->append(buff, SYM_WEB_CONSTANTS_ACK_BATCH_NAME);
        buff->append(buff, batchId);
        buff->append(buff, "=");
        buff->append(buff, SYM_WEB_CONSTANTS_ACK_BATCH_OK);
        buff->append(buff, "&");
    }

    this->super.ackString = buff->destroyAndReturn(buff);

    return result;
}


void SymFileOutgoingTransport_destroy(SymFileOutgoingTransport *this) {
    free(this);
}

SymFileOutgoingTransport * SymFileOutgoingTransport_new(SymFileOutgoingTransport *this, SymNode *remoteNode, SymNode *localNode,
        char *offlineOutgoingDir) {
    if (this == NULL) {
        this = (SymFileOutgoingTransport *) calloc(1, sizeof(SymFileOutgoingTransport));
    }
    SymOutgoingTransport *super = (SymOutgoingTransport *)&this->super;
    this->remoteNode = remoteNode;
    this->localNode = localNode;
    this->offlineOutgoingDir = offlineOutgoingDir;

    super->process = (void *) &SymFileOutgoingTransport_process;
    super->destroy = (void *) &SymFileOutgoingTransport_destroy;
    return this;
}
