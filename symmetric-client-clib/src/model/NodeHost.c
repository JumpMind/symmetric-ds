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
#include "model/NodeHost.h"

void SymNodeHost_refresh(SymNodeHost *this) {
   this->hostName = SymAppUtils_getHostName();
   this->ipAddress = SymAppUtils_getIpAddress();
   this->osArch = SymAppUtils_getOsArch();
   this->osName = SymAppUtils_getOsName();
   this->osVersion = SymAppUtils_getOsVersion();
   this->osUser = SymAppUtils_getOsUser();

   // TODO consider mallinfo(); for memory statistics?

   this->symmetricVersion = SYM_VERSION;
   this->timezoneOffset = SymAppUtils_getTimezoneOffset();
   this->heartbeatTime = SymDate_new(NULL);
}

void SymNodeHost_destroy(SymNodeHost *this) {
    if (this->heartbeatTime) {
        this->heartbeatTime->destroy(this->heartbeatTime);
    }

    free(this);
}

SymNodeHost * SymNodeHost_new(SymNodeHost *this) {
    if (this == NULL) {
        this = (SymNodeHost *) calloc(1, sizeof(SymNodeHost));
    }
    this->refresh = (void *) &SymNodeHost_refresh;
    this->destroy = (void *) &SymNodeHost_destroy;
    return this;
}
