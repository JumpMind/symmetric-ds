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
#include "transport/http/HttpOutgoingTransport.h"

static size_t SymHttpOutgoingTransport_saveResponse(char *data, size_t size, size_t nmemb, SymHttpOutgoingTransport *this) {
    int numBytes = size * nmemb;
    this->response->appendn(this->response, data, nmemb);
    return numBytes;
}

static void SymHttpOutgoingTransport_parseResponse(SymHttpOutgoingTransport *this) {
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

static size_t SymHttpOutgoingTransport_readCallback(char *data, size_t size, size_t count, SymDataProcessor *processor) {
    return processor->process(processor, data, size, count);
}

long SymHttpOutgoingTransport_process(SymHttpOutgoingTransport *this, SymDataProcessor *processor, SymRemoteNodeStatus *status) {
    long httpCode = -1;
    CURLcode rc = CURLE_FAILED_INIT;
    CURL *curl = curl_easy_init();
    if (curl) {
        if (this->parameterService->is(this->parameterService, SYM_PARAMETER_HTTPS_VERIFIED_SERVERS, 1)) {
            curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0);
        }
        if (this->parameterService->is(this->parameterService, SYM_PARAMETER_HTTPS_ALLOW_SELF_SIGNED_CERTS, 1)) {
            curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0);
        }
        struct curl_slist *headers = NULL;
        headers = curl_slist_append(headers, "Transfer-Encoding: chunked");
        curl_easy_setopt(curl, CURLOPT_URL, this->url);
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
        curl_easy_setopt(curl, CURLOPT_READFUNCTION, SymHttpOutgoingTransport_readCallback);
        curl_easy_setopt(curl, CURLOPT_READDATA, processor);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, SymHttpOutgoingTransport_saveResponse);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, this);
        processor->open(processor);
        rc = curl_easy_perform(curl);
        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);
        SymHttpOutgoingTransport_parseResponse(this);
        processor->close(processor);
    } else {
        SymLog_error("Error cannot initialize curl");
    }
    if (rc == CURLE_OK) {
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);
    }

    SymHttpTransportManager_handleCurlRc(rc, httpCode, this->url, status);

    return httpCode;
}

void SymHttpOutgoingTransport_destroy(SymHttpOutgoingTransport *this) {
    this->response->destroy(this->response);
    free(this->url);
    free(this);
}

SymHttpOutgoingTransport * SymHttpOutgoingTransport_new(SymHttpOutgoingTransport *this, char *url, SymParameterService *parameterService) {
    if (this == NULL) {
        this = (SymHttpOutgoingTransport *) calloc(1, sizeof(SymHttpOutgoingTransport));
    }
    this->parameterService = parameterService;
    SymOutgoingTransport *super = &this->super;
    this->url = url;
    this->response = SymStringBuilder_new(NULL);
    super->process = (void *) &SymHttpOutgoingTransport_process;
    super->destroy = (void *) &SymHttpOutgoingTransport_destroy;
    return this;
}
