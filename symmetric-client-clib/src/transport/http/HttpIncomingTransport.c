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

static char * strerror_http(long rc) {
    if (rc == SYM_TRANSPORT_OK) {
        return "OK";
    } else if (rc == SYM_TRANSPORT_REGISTRATION_NOT_OPEN) {
        return "Registration Not Open";
    } else if (rc == SYM_TRANSPORT_REGISTRATION_REQUIRED) {
        return "Registration Required";
    } else if (rc == SYM_TRANSPORT_SYNC_DISABLED) {
        return "Sync Disabled";
    } else if (rc == SYM_TRANSPORT_SC_SERVICE_UNAVAILABLE) {
        return "Service Unavailable";
    } else if (rc == SYM_TRANSPORT_SC_FORBIDDEN) {
        return "Forbidden, Authentication Required";
    }
    return "Unknown Error";
}

static size_t write_callback(char *data, size_t size, size_t count, SymDataReader *reader) {
    return reader->process(reader, data, size, count);
}

long SymHttpIncomingTransport_process(SymHttpIncomingTransport *this, SymDataReader *reader) {
    long httpCode = 0;
    CURLcode rc = CURLE_FAILED_INIT;
    CURL *curl = curl_easy_init();
    if (curl) {
        curl_easy_setopt(curl, CURLOPT_URL, this->url);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, reader);
        reader->open(reader);
        rc = curl_easy_perform(curl);
        if (rc != CURLE_OK) {
            fprintf(stderr, "Error %d from curl, cannot retrieve %s\n", rc, this->url);
            fprintf(stderr, "%s\n", curl_easy_strerror(rc));
        }
        curl_easy_cleanup(curl);
        reader->close(reader);
    } else {
        fprintf(stderr, "Error cannot initialize curl\n");
    }
    if (rc == CURLE_OK) {
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);
        if (httpCode != SYM_TRANSPORT_OK) {
            fprintf(stderr, "HTTP response code of %ld, %s\n", httpCode, strerror_http(httpCode));
        }
    }
    return httpCode;
}

char * SymHttpIncomingTransport_get_url(SymHttpIncomingTransport *this) {
    return this->url;
}

void SymHttpIncomingTransport_destroy(SymHttpIncomingTransport *this) {
    free(this->url);
    free(this);
}

SymHttpIncomingTransport * SymHttpIncomingTransport_new(SymHttpIncomingTransport *this, char *url) {
    if (this == NULL) {
        this = (SymHttpIncomingTransport *) calloc(1, sizeof(SymHttpIncomingTransport));
    }
    SymIncomingTransport *super = &this->super;
    super->get_url = (void *) &SymHttpIncomingTransport_get_url;
    super->process = (void *) &SymHttpIncomingTransport_process;
    super->destroy = (void *) &SymHttpIncomingTransport_destroy;

    this->url = url;
    return this;
}
