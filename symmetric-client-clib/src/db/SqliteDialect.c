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
#include "db/SqliteDialect.h"

static int create_if_missing(SymDialect *super, char *tableName, char *createSql) {
    if (!super->platform->table_exists(super->platform, tableName)) {
        char *errorMessage;
        printf("DDL applied: %s\n", tableName);
        if (super->platform->execute_sql(super->platform, createSql, NULL, NULL, &errorMessage)) {
            fprintf(stderr, "Error creating %s table: %s\n", tableName, errorMessage);
            super->platform->free(errorMessage);
            return 1;
        }
    }
    return 0;
}

int SymSqliteDialect_init_tables(SymDialect *super) {
    printf("Checking if SymmetricDS tables need created or altered\n");
    create_if_missing(super, "sym_channel", CREATE_SYM_CHANNEL);
    create_if_missing(super, "sym_data", CREATE_SYM_DATA);
    create_if_missing(super, "sym_data_event", CREATE_SYM_DATA_EVENT);
    create_if_missing(super, "sym_incoming_batch", CREATE_SYM_INCOMING_BATCH);
    create_if_missing(super, "sym_node", CREATE_SYM_NODE);
    create_if_missing(super, "sym_node_group", CREATE_SYM_NODE_GROUP);
    create_if_missing(super, "sym_node_group_link", CREATE_SYM_NODE_GROUP_LINK);
    create_if_missing(super, "sym_node_host", CREATE_SYM_NODE_HOST);
    create_if_missing(super, "sym_node_identity", CREATE_SYM_NODE_IDENTITY);
    create_if_missing(super, "sym_outgoing_batch", CREATE_SYM_OUTGOING_BATCH);
    create_if_missing(super, "sym_parameter", CREATE_SYM_PARAMETER);
    create_if_missing(super, "sym_router", CREATE_SYM_ROUTER);
    create_if_missing(super, "sym_sequence", CREATE_SYM_SEQUENCE);
    create_if_missing(super, "sym_trigger", CREATE_SYM_TRIGGER);
    create_if_missing(super, "sym_trigger_hist", CREATE_SYM_TRIGGER_HIST);
    create_if_missing(super, "sym_trigger_router", CREATE_SYM_TRIGGER_ROUTER);
    printf("Done with auto update of SymmetricDS tables\n");
    return 0;
}

int SymSqliteDialect_drop_tables(SymDialect *super) {
    return 0;
}

int SymSqliteDialect_create_trigger(SymDialect *super) {
    return 0;
}

int SymSqliteDialect_remove_trigger(SymDialect *super) {
    return 0;
}

int SymSqliteDialect_get_initial_load_sql(SymDialect *super) {
    return 0;
}

void SymSqliteDialect_destroy(SymDialect *super) {
    SymSqliteDialect *this = (SymSqliteDialect *) super;
    free(this);
}

SymSqliteDialect * SymSqliteDialect_new(SymSqliteDialect *this, SymDatabasePlatform *platform) {
    if (this == NULL) {
        this = (SymSqliteDialect *) calloc(1, sizeof(SymSqliteDialect));
    }
    SymDialect_new(&this->super, platform);
    SymDialect *super = &this->super;
    super->init_tables = (void *) &SymSqliteDialect_init_tables;
    super->drop_tables = (void *) &SymSqliteDialect_drop_tables;
    super->create_trigger = (void *) &SymSqliteDialect_create_trigger;
    super->remove_trigger = (void *) &SymSqliteDialect_remove_trigger;
    super->get_initial_load_sql = (void *) &SymSqliteDialect_get_initial_load_sql;
    super->destroy = (void *) &SymSqliteDialect_destroy;

    printf("The DbDialect being used is SymSqliteDialect\n");

    return this;
}
