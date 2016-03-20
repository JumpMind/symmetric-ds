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
    int numBytes = SYM_MAX_HOSTNAME * sizeof(char);
    char *name = (char *) malloc(numBytes);
    gethostname(name, numBytes);
    return name;
}

#ifdef SYM_WIN32
char * SymAppUtils_getIpAddress() {
	char *hostname = SymAppUtils_getHostName();
    struct hostent *he = gethostbyname(SymAppUtils_getHostName());
    free(hostname);
    if (he != 0) {
    	int i;
		for (i = 0; he->h_addr_list[i] != 0; ++i) {
			struct in_addr addr;
			memcpy(&addr, he->h_addr_list[i], sizeof(struct in_addr));
			if (strcmp(he->h_addr_list[i], "localhost") != 0) {
				return inet_ntoa(addr);
			}
		}
    }
	return "127.0.0.1";
}
#else
char * SymAppUtils_getIpAddress() {
    struct ifaddrs *ifaddr, *ifa;
    int rc;
    char *ipaddr = (char *) calloc(SYM_MAX_IP_ADDRESS, sizeof(char));

    if (getifaddrs(&ifaddr) == 0) {
        for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
            if (ifa->ifa_addr != NULL && ifa->ifa_addr->sa_family == AF_INET &&
                    (strcmp(ifa->ifa_name, "wlan0") == 0
                            || strcmp(ifa->ifa_name, "en0") == 0
                            || strcmp(ifa->ifa_name, "eth0") == 0)) {
                if ((rc = getnameinfo(ifa->ifa_addr, sizeof(struct sockaddr_in), ipaddr, SYM_MAX_IP_ADDRESS, NULL, 0, NI_NUMERICHOST)) == 0) {
                    break;
                } else {
                	SymLog_warn("getnameinfo() failed: %s", gai_strerror(rc));
                }
            }
        }
        freeifaddrs(ifaddr);
    }
    return ipaddr;
}
#endif

#ifdef SYM_WIN32
char * SymAppUtils_getTimezoneOffset() {
	return "-00:00";
}
#else
char * SymAppUtils_getTimezoneOffset() {
    time_t t = time(NULL);
    struct tm lt = {0};
    localtime_r(&t, &lt);

    int MINUTES_PER_HOUR = 60, SECONDS_PER_MINUTE = 60;
    int offsetInMinutes = lt.tm_gmtoff / SECONDS_PER_MINUTE;

    char sign;
    if (offsetInMinutes < 0) {
        sign = '-';
    }
    else {
        sign = '+'; // should be plus or minus sign for UTC?
    }

    int hours =  abs(offsetInMinutes / MINUTES_PER_HOUR);
    int minutes = abs(offsetInMinutes % MINUTES_PER_HOUR);

    char *timezoneOffset = SymStringUtils_format("%c%02d:%02d", sign, hours, minutes);

    return timezoneOffset;
}
#endif

#ifdef SYM_WIN32
char * SymAppUtils_getOsName() {
	return "Windows";
}
#else
char * SymAppUtils_getOsName() {
    struct utsname unameData;
    uname(&unameData);
    return SymStringUtils_format("%s", unameData.sysname);
}
#endif

#ifdef SYM_WIN32
char * SymAppUtils_getOsVersion() {
	DWORD version = GetVersion();
	DWORD major = (DWORD) (LOBYTE(LOWORD(version)));
	DWORD minor = (DWORD) (HIBYTE(LOWORD(version)));
	return SymStringUtils_format("%d.%d", major, minor);
}
#else
char * SymAppUtils_getOsVersion() {
    struct utsname unameData;
    uname(&unameData);
    return SymStringUtils_format("%s", unameData.release);
}
#endif

#ifdef SYM_WIN32
char * SymAppUtils_getOsArch() {
	SYSTEM_INFO sysinfo;
	GetSystemInfo(&sysinfo);
	if (sysinfo.wProcessorArchitecture == PROCESSOR_ARCHITECTURE_AMD64) {
		return "x64";
	} else if (sysinfo.wProcessorArchitecture == PROCESSOR_ARCHITECTURE_ARM) {
		return "ARM";
	} else if (sysinfo.wProcessorArchitecture == PROCESSOR_ARCHITECTURE_IA64) {
		return "IA64";
	} else if (sysinfo.wProcessorArchitecture == PROCESSOR_ARCHITECTURE_INTEL) {
		return "x86";
	}
	return "UNK";
}
#else
char * SymAppUtils_getOsArch() {
    struct utsname unameData;
    uname(&unameData);
    return SymStringUtils_format("%s", unameData.machine);
}
#endif

#ifdef SYM_WIN32
char * SymAppUtils_getOsUser() {
	char username[100];
	DWORD size = 100;
	GetUserName(username, &size);
	return SymStringUtils_rtrim(username);
}
#else
char * SymAppUtils_getOsUser() {
    char *username = getlogin();
    return username;
}
#endif
