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
#ifndef SYM_DIALECT_H
#define SYM_DIALECT_H

#include <stdio.h>
#include <stdlib.h>
#include <util/Properties.h>
#include "db/DatabasePlatform.h"

typedef struct {
    SymDatabasePlatform *platform;
    int (*init_tables)(void *this);
    int (*drop_tables)(void *this);
    int (*create_trigger)(void *this);
    int (*remove_trigger)(void *this);
    int (*get_initial_load_sql)(void *this);
    void (*destroy)(void *this);
} SymDialect;

SymDialect * SymDialect_new(SymDialect *this, SymDatabasePlatform *platform);

#endif
