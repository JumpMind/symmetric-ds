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
#ifndef SYM_HTTP_TRANSPORT_MANAGER_H
#define SYM_HTTP_TRANSPORT_MANAGER_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <curl/curl.h>
#include "service/ParameterService.h"
#include "transport/TransportManager.h"
#include "transport/http/HttpIncomingTransport.h"
#include "transport/http/HttpOutgoingTransport.h"
#include "util/StringBuilder.h"
#include "util/List.h"
#include "util/AppUtils.h"
#include "util/Map.h"
#include "util/StringUtils.h"
#include "web/WebConstants.h"
#include "model/RemoteNodeStatus.h"

typedef struct SymHttpTransportManager {
    SymTransportManager super;
    SymParameterService *parameterService;
} SymHttpTransportManager;

SymHttpTransportManager * SymHttpTransportManager_new(SymHttpTransportManager *this, SymParameterService *parameterService);

char * SymHttpTransportManager_strerror(long rc);

SymMap * SymHttpTransportManager_getParametersFromQueryUrl(char *parameterString);
SymBatchAck * SymHttpTransportManager_getBatchInfo(SymMap *parameters, char *batchId);
void  SymHttpTransportManager_handleCurlRc(int curlRc, long httpCode, char* url, SymRemoteNodeStatus* status);

#endif
