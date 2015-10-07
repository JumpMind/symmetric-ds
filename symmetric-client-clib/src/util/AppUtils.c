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
#include "util/AppUtils.h"

char * SymAppUtils_getHostName() {
    char *name = (char *) calloc(SYM_MAX_HOSTNAME, sizeof(char));
    gethostname(name, SYM_MAX_HOSTNAME);
    return name;
}

char * SymAppUtils_getIpAddress() {
    struct ifaddrs *ifaddr, *ifa;
    int rc;
    char *ipaddr = (char *) calloc(SYM_MAX_IP_ADDRESS, sizeof(char));

    if (getifaddrs(&ifaddr) == 0) {
        for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
            if (ifa->ifa_addr != NULL && ifa->ifa_addr->sa_family == AF_INET &&
                    (strcmp(ifa->ifa_name, "wlan0") == 0 || strcmp(ifa->ifa_name, "eth0") == 0)) {
                if ((rc = getnameinfo(ifa->ifa_addr, sizeof(struct sockaddr_in), ipaddr, NI_MAXHOST, NULL, 0, NI_NUMERICHOST)) == 0) {
                    break;
                } else {
                    printf("getnameinfo() failed: %s\n", gai_strerror(rc));
                }
            }
        }
        freeifaddrs(ifaddr);
    }
    return ipaddr;
}
