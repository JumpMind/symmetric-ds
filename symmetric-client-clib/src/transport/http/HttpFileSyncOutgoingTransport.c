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
#include "transport/http/HttpFileSyncOutgoingTransport.h"

static size_t SymHttpFileSyncOutgoingTransport_saveResponse(char *data, size_t size, size_t nmemb, SymHttpFileSyncOutgoingTransport *this) {
    int numBytes = size * nmemb;
    this->response->appendn(this->response, data, nmemb);
    return numBytes;
}

static void SymHttpFileSyncOutgoingTransport_parseResponse(SymHttpFileSyncOutgoingTransport *this) {
    if (SymStringUtils_isBlank(this->response->str)) {
        SymLog_error("Did not receive an acknowledgment for the batches sent.  Received '%s'", this->response->str);
    } else {
        SymOutgoingTransport *super = &this->super;
        SymStringArray *lines = SymStringArray_split(this->response->str, "\n");
        super->ackString = SymStringBuilder_copy(lines->get(lines, 0));
        super->ackExtendedString = SymStringBuilder_copy(lines->get(lines, 1));
        int i;
        for (i = 2; i < lines->size; i++) {
            SymLog_info("Read an unexpected line %s", lines->get(lines, i));
        }
        lines->destroy(lines);
    }
}

long SymHttpFileSyncOutgoingTransport_process(SymHttpFileSyncOutgoingTransport *this, SymDataProcessor *processor, SymRemoteNodeStatus *status) {
    long httpCode = -1;
    CURLcode rc = CURLE_FAILED_INIT;
    CURL *curl = curl_easy_init();
    if (curl) {
        struct curl_httppost *formpost=NULL;
        struct curl_httppost *lastptr=NULL;

        curl_formadd(&formpost,
                &lastptr,
                CURLFORM_COPYNAME, "sendfile",
                CURLFORM_FILE, "./tmp/staging/filesync_outgoing/filesync.zip",
                CURLFORM_END);
        curl_formadd(&formpost,
                &lastptr,
                CURLFORM_COPYNAME, "filename",
                CURLFORM_COPYCONTENTS, "filesync.zip",
                CURLFORM_END);

        SymCurlConfig_configure(curl, this->parameterService);

        struct curl_slist *headers = NULL;
        headers = curl_slist_append(headers, "Transfer-Encoding: chunked");
        curl_easy_setopt(curl, CURLOPT_URL, this->url);
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(curl, CURLOPT_HTTPPOST, formpost);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, SymHttpFileSyncOutgoingTransport_saveResponse);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, this);
        rc = curl_easy_perform(curl);
        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);
        SymHttpFileSyncOutgoingTransport_parseResponse(this);
    } else {
        SymLog_error("Error cannot initialize curl");
    }
    if (rc == CURLE_OK) {
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);
    }

    SymHttpTransportManager_handleCurlRc(rc, httpCode, this->url, status);

    return httpCode;
}

void SymHttpFileSyncOutgoingTransport_destroy(SymHttpFileSyncOutgoingTransport *this) {
    this->response->destroy(this->response);
    free(this->url);
    free(this);
}

SymHttpFileSyncOutgoingTransport * SymHttpFileSyncOutgoingTransport_new(SymHttpFileSyncOutgoingTransport *this, char *url, SymParameterService *parameterService) {
    if (this == NULL) {
        this = (SymHttpFileSyncOutgoingTransport *) calloc(1, sizeof(SymHttpFileSyncOutgoingTransport));
    }
    this->parameterService = parameterService;
    SymOutgoingTransport *super = &this->super;
    this->url = url;
    this->response = SymStringBuilder_new(NULL);
    super->process = (void *) &SymHttpFileSyncOutgoingTransport_process;
    super->destroy = (void *) &SymHttpFileSyncOutgoingTransport_destroy;
    return this;
}
