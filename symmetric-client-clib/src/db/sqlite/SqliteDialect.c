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
#include "db/sqlite/SqliteDialect.h"

static int create_if_missing(SymDialect *super, char *tableName, char *createSql) {
    // TODO: re-implement this using ddl reader
    if (!super->platform->tableExists(super->platform, tableName)) {
        char *errorMessage;
        printf("DDL applied: %s\n", tableName);
        if (super->platform->executeSql(super->platform, createSql, NULL, NULL, &errorMessage)) {
            fprintf(stderr, "Error creating %s table: %s\n", tableName, errorMessage);
            free(errorMessage);
            return 1;
        }
    }
    return 0;
}

int SymSqliteDialect_initTables(SymDialect *super) {
    printf("Checking if SymmetricDS tables need created or altered\n");
    create_if_missing(super, "sym_channel", CREATE_SYM_CHANNEL);
    create_if_missing(super, "sym_data", CREATE_SYM_DATA);
    create_if_missing(super, "sym_data_event", CREATE_SYM_DATA_EVENT);
    create_if_missing(super, "sym_incoming_batch", CREATE_SYM_INCOMING_BATCH);
    create_if_missing(super, "sym_node", CREATE_SYM_NODE);
    create_if_missing(super, "sym_node_security", CREATE_SYM_NODE_SECURITY);
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

int SymSqliteDialect_dropTables(SymDialect *super) {
    return 0;
}

int SymSqliteDialect_createTrigger(SymDialect *super) {
    return 0;
}

int SymSqliteDialect_removeTrigger(SymDialect *super) {
    return 0;
}

int SymSqliteDialect_getInitialLoadSql(SymDialect *super) {
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
    super->initTables = (void *) &SymSqliteDialect_initTables;
    super->dropTables = (void *) &SymSqliteDialect_dropTables;
    super->createTrigger = (void *) &SymSqliteDialect_createTrigger;
    super->removeTrigger = (void *) &SymSqliteDialect_removeTrigger;
    super->getInitialLoadSql = (void *) &SymSqliteDialect_getInitialLoadSql;
    super->destroy = (void *) &SymSqliteDialect_destroy;

    printf("The DbDialect being used is SymSqliteDialect\n");

    return this;
}
