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
#include "db/platform/DatabasePlatformFactory.h"
#include "common/Log.h"

SymDatabasePlatform * SymDatabasePlatformFactory_create(SymProperties *properties) {
    SymDatabasePlatform *platform = NULL;
    char *url = properties->get(properties, SYM_PARAMETER_DB_URL, SYM_DATABASE_SQLITE);
    if (strncmp(url, SYM_DATABASE_SQLITE, strlen(SYM_DATABASE_SQLITE)) == 0) {
        platform = (SymDatabasePlatform *) SymSqlitePlatform_new(NULL, properties);
    } else {
        SymLog_error("Could not find platform for database URL '%s'", url);
    }
    return platform;
}
