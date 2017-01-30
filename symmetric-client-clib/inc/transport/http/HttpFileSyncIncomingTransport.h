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
#ifndef SYM_HTTPFILESYNCINCOMINGTRANSPORT_H_
#define SYM_HTTPFILESYNCINCOMINGTRANSPORT_H_

#include <stdio.h>
#include <stdlib.h>
#include <curl/curl.h>
#include "service/ParameterService.h"
#include "model/Node.h"
#include "transport/http/HttpTransportManager.h"
#include "transport/IncomingTransport.h"
#include "util/Properties.h"
#include "common/ParameterConstants.h"
#include "common/Log.h"
#include "model/RemoteNodeStatus.h"
#include "transport/http/CurlConfig.h"

typedef struct SymHttpFileSyncIncomingTransport {
    SymIncomingTransport super;
    char *url;
    SymParameterService *parameterService;
    char* (*getZipFile)(struct SymHttpFileSyncIncomingTransport *transport);
} SymHttpFileSyncIncomingTransport;

SymHttpFileSyncIncomingTransport * SymHttpFileSyncIncomingTransport_new(SymHttpFileSyncIncomingTransport *this, char *url, SymParameterService *parameterService);

#endif /* SYM_HTTPFILESYNCINCOMINGTRANSPORT_H_ */
