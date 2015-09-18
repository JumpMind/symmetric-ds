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
#include "db/SymDialect.h"

int SymDialect_init_tables(SymDialect *this) {
    return 0;
}

int SymDialect_drop_tables(SymDialect *this) {
    return 0;
}

int SymDialect_create_trigger(SymDialect *this) {
    return 0;
}

int SymDialect_remove_trigger(SymDialect *this) {
    return 0;
}

int SymDialect_get_initial_load_sql(SymDialect *this) {
    return 0;
}

void SymDialect_destroy(SymDialect *this) {
    free(this);
}

SymDialect * SymDialect_new(SymDialect *this, SymDatabasePlatform *platform) {
    if (this == NULL) {
        this = (SymDialect *) calloc(1, sizeof(SymDialect));
    }
    this->platform = platform;
    this->init_tables = (void *) &SymDialect_init_tables;
    this->drop_tables = (void *) &SymDialect_drop_tables;
    this->create_trigger = (void *) &SymDialect_create_trigger;
    this->remove_trigger = (void *) &SymDialect_remove_trigger;
    this->get_initial_load_sql = (void *) &SymDialect_get_initial_load_sql;
    this->destroy = (void *) &SymDialect_destroy;
    return this;
}
