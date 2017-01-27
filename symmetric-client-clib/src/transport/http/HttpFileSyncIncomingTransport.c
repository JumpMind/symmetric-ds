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
#include "transport/http/HttpFileSyncIncomingTransport.h"

static size_t SymHttpFileSyncIncomingTransport_writeCallback(void *data, size_t size, size_t count, FILE *file) {
    size_t written = fwrite(data, size, count, file);
    return written;
}
long SymHttpFileSyncIncomingTransport_process(SymHttpFileSyncIncomingTransport *this, SymDataProcessor *processor, SymRemoteNodeStatus *status) {
    long httpCode = 0;
    FILE *file;
    CURLcode rc = CURLE_FAILED_INIT;
    CURL *curl = curl_easy_init();
    char *zipFileName = this->getZipFile(this);
    if (curl) {
        file = fopen(zipFileName, "wb");
        if (!file) {
            SymLog_error("Could not open zip file for writing %s", zipFileName);
            return 500;
        }

        SymCurlConfig_configure(curl, this->parameterService);
        curl_easy_setopt(curl, CURLOPT_URL, this->url);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, SymHttpFileSyncIncomingTransport_writeCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, file);
        rc = curl_easy_perform(curl);
        curl_easy_cleanup(curl);
        fclose(file);
    } else {
        SymLog_error("Error cannot initialize curl");
    }
    if (rc == CURLE_OK) {
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);
    }

    SymHttpTransportManager_handleCurlRc(rc, httpCode, this->url, status);
    if (rc != CURLE_OK || httpCode != SYM_TRANSPORT_OK) {
        remove(zipFileName);
    }

    return httpCode;
}

char * SymHttpFileSyncIncomingTransport_getUrl(SymHttpFileSyncIncomingTransport *this) {
    return this->url;
}

char * SymHttpFileSyncIncomingTransport_getZipFile(SymHttpFileSyncIncomingTransport *this) {
    remove("tmp/staging/filesync_incoming/filesync.zip");
    SymFileUtils_mkdir("tmp/staging/filesync_incoming/");
    return "tmp/staging/filesync_incoming/filesync.zip";
}

void SymHttpFileSyncIncomingTransport_destroy(SymHttpFileSyncIncomingTransport *this) {
    free(this->url);
    free(this);
}

SymHttpFileSyncIncomingTransport * SymHttpFileSyncIncomingTransport_new(SymHttpFileSyncIncomingTransport *this, char *url, SymParameterService *parameterService) {
    if (this == NULL) {
        this = (SymHttpFileSyncIncomingTransport *) calloc(1, sizeof(SymHttpFileSyncIncomingTransport));
    }
    this->parameterService = parameterService;
    SymIncomingTransport *super = &this->super;
    super->getUrl = (void *) &SymHttpFileSyncIncomingTransport_getUrl;
    super->process = (void *) &SymHttpFileSyncIncomingTransport_process;
    super->destroy = (void *) &SymHttpFileSyncIncomingTransport_destroy;

    this->getZipFile = (void *) &SymHttpFileSyncIncomingTransport_getZipFile;
    this->url = url;
    return this;
}
