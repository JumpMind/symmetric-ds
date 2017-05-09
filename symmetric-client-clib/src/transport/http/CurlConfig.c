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
#include "transport/http/CurlConfig.h"

void SymCurlConfig_configure(CURL *curl, SymParameterService *parameterService) {

    long readTimeoutMillis = parameterService->getLong(parameterService,
            SYM_PARAMETER_TRANSPORT_HTTP_TIMEOUT, 90000);
    long readTimeoutSeconds = readTimeoutMillis/1000;

    curl_easy_setopt(curl, CURLOPT_LOW_SPEED_LIMIT, 1);
    curl_easy_setopt(curl, CURLOPT_LOW_SPEED_TIME, readTimeoutSeconds);

    if (parameterService->is(parameterService, SYM_PARAMETER_HTTPS_VERIFIED_SERVERS, 1)) {
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0);
    }
    if (parameterService->is(parameterService, SYM_PARAMETER_HTTPS_ALLOW_SELF_SIGNED_CERTS, 1)) {
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0);
    }
}
