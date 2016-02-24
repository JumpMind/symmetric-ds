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
#ifndef SYM_APP_UTILS_H
#define SYM_APP_UTILS_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "common/Log.h"
#include "util/StringUtils.h"

#ifdef SYM_WIN32
#include <Windows.h>
#include <Winsock2.h>
#else
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netdb.h>
#include <ifaddrs.h>
#include <sys/utsname.h>
#endif

#define SYM_MAX_HOSTNAME 64
#define SYM_MAX_IP_ADDRESS 64

char * SymAppUtils_getHostName();

char * SymAppUtils_getIpAddress();

char * SymAppUtils_getTimezoneOffset();

char * SymAppUtils_getOsName();

char * SymAppUtils_getOsVersion();

char * SymAppUtils_getOsArch();

char * SymAppUtils_getOsUser();

#endif
