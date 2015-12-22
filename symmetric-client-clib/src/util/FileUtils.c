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
#include "util/FileUtils.h"

int SymFileUtils_mkdir(char* dirName) {

    int result = 0;
    struct stat st = {0};

    if (stat(dirName, &st) == -1) {
        char tmp[256];
        char *p = NULL;
        size_t len;
        char SEPERATOR = '/';

        snprintf(tmp, sizeof(tmp),"%s", dirName);
        len = strlen(tmp);
        if (tmp[len - 1] == SEPERATOR) {
            tmp[len - 1] = 0;
        }

        for (p = tmp + 1; *p; p++) {
            if(*p == '/') {
                *p = 0;
                result = mkdir(tmp, S_IRWXU);
                if (result != 0) {
                    SymLog_warn("Failed to create dir '%s' %s", tmp, strerror(errno));
                }
                *p = '/';
            }
        }

        result = mkdir(tmp, S_IRWXU);
        if (result != 0) {
            SymLog_warn("Failed to create dir '%s' %s", tmp, strerror(errno));
        }
    }

    return result;
}

int SymFileUtils_getFileSize(char *filename) {
    struct stat st;

    if (stat(filename, &st) == 0) {
        return st.st_size;
    }

    return -1;
}

unsigned short SymFileUtils_exists(char *filename) {
    if( access( filename, F_OK ) != -1 ) {
        return 1;
    } else {
        return 0;
    }
}
