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
#include "db/DatabasePlatform.h"

int SymDatabasePlatform_table_exists(SymDatabasePlatform *this) {
    return 0;
}

int SymDatabasePlatform_execute_sql(SymDatabasePlatform *this) {
    return 0;
}

void SymDatabasePlatform_free(SymDatabasePlatform *this) {
}

void SymDatabasePlatform_destroy(SymDatabasePlatform *this) {
    free(this);
}

SymDatabasePlatform * SymDatabasePlatform_new(SymDatabasePlatform *this, SymProperties *properties) {
    if (this == NULL) {
        this = (SymDatabasePlatform *) calloc(1, sizeof(SymDatabasePlatform));
    }
    this->name = SYM_DATABASE_UNDEFINED;
    this->properties = properties;
    this->execute_sql = (void *) &SymDatabasePlatform_execute_sql;
    this->table_exists = (void *) &SymDatabasePlatform_table_exists;
    this->free = (void *) &SymDatabasePlatform_free;
    this->destroy = (void *) &SymDatabasePlatform_destroy;
    return this;
}
