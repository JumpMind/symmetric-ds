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

SymList * SymHttpOutgoingTransport_readAcks(SymHttpOutgoingTransport *this) {
    return NULL;
}

static size_t SymHttpOutgoingTransport_readCallback(char *data, size_t size, size_t count, SymDataProcessor *processor) {
    return processor->process(processor, data, size, count);
}

long SymHttpOutgoingTransport_process(SymHttpOutgoingTransport *this, SymDataProcessor *processor) {
    long httpCode = 0;
    CURLcode rc = CURLE_FAILED_INIT;
    CURL *curl = curl_easy_init();
    if (curl) {
        struct curl_slist *headers = NULL;
        headers = curl_slist_append(headers, "Transfer-Encoding: chunked");
        curl_easy_setopt(curl, CURLOPT_URL, this->url);
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
        curl_easy_setopt(curl, CURLOPT_READFUNCTION, SymHttpOutgoingTransport_readCallback);
        curl_easy_setopt(curl, CURLOPT_READDATA, processor);
        processor->open(processor);
        rc = curl_easy_perform(curl);
        if (rc != CURLE_OK) {
            SymLog_error("Error %d from curl, cannot retrieve %s", rc, this->url);
            SymLog_error("%s", curl_easy_strerror(rc));
        }
        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);
        processor->close(processor);
    } else {
        SymLog_error("Error cannot initialize curl");
    }
    if (rc == CURLE_OK) {
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);
        if (httpCode != SYM_TRANSPORT_OK) {
            SymLog_error("HTTP response code of %ld, %s", httpCode, SymHttpTransportManager_strerror(httpCode));
        }
    }
    return httpCode;
}

void SymHttpOutgoingTransport_destroy(SymHttpOutgoingTransport *this) {
    free(this->url);
    free(this);
}

SymHttpOutgoingTransport * SymHttpOutgoingTransport_new(SymHttpOutgoingTransport *this, char *url) {
    if (this == NULL) {
        this = (SymHttpOutgoingTransport *) calloc(1, sizeof(SymHttpOutgoingTransport));
    }
    SymOutgoingTransport *super = &this->super;
    this->url = url;
    super->process = (void *) &SymHttpOutgoingTransport_process;
    super->destroy = (void *) &SymHttpOutgoingTransport_destroy;
    return this;
}
