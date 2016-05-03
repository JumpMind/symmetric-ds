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
#include "transport/http/HttpIncomingTransport.h"

static size_t SymHttpIncomingTransport_writeCallback(char *data, size_t size, size_t count, SymDataProcessor *processor) {
    return processor->process(processor, data, size, count);
}

long SymHttpIncomingTransport_process(SymHttpIncomingTransport *this, SymDataProcessor *processor, SymRemoteNodeStatus *status) {
    long httpCode = 0;
    CURLcode rc = CURLE_FAILED_INIT;
    CURL *curl = curl_easy_init();
    if (curl) {
        if (this->parameterService->is(this->parameterService, SYM_PARAMETER_HTTPS_VERIFIED_SERVERS, 1)) {
            curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0);
        }
        if (this->parameterService->is(this->parameterService, SYM_PARAMETER_HTTPS_ALLOW_SELF_SIGNED_CERTS, 1)) {
            curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0);
        }
        curl_easy_setopt(curl, CURLOPT_URL, this->url);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, SymHttpIncomingTransport_writeCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, processor);
        processor->open(processor);
        rc = curl_easy_perform(curl);
        curl_easy_cleanup(curl);
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

char * SymHttpIncomingTransport_getUrl(SymHttpIncomingTransport *this) {
    return this->url;
}

void SymHttpIncomingTransport_destroy(SymHttpIncomingTransport *this) {
    free(this->url);
    free(this);
}

SymHttpIncomingTransport * SymHttpIncomingTransport_new(SymHttpIncomingTransport *this, char *url, SymParameterService *parameterService) {
    if (this == NULL) {
        this = (SymHttpIncomingTransport *) calloc(1, sizeof(SymHttpIncomingTransport));
    }
    this->parameterService = parameterService;
    SymIncomingTransport *super = &this->super;
    super->getUrl = (void *) &SymHttpIncomingTransport_getUrl;
    super->process = (void *) &SymHttpIncomingTransport_process;
    super->destroy = (void *) &SymHttpIncomingTransport_destroy;

    this->url = url;
    return this;
}
