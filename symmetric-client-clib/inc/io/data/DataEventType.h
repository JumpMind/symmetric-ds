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
#ifndef SYM_IO_DATA_DATAEVENTTYPE_H
#define SYM_IO_DATA_DATAEVENTTYPE_H

#include "db/sql/DmlStatement.h"

typedef enum {
    SYM_DATA_EVENT_INSERT,
    SYM_DATA_EVENT_UPDATE,
    SYM_DATA_EVENT_DELETE,
    SYM_DATA_EVENT_RELOAD,
    SYM_DATA_EVENT_SQL,
    SYM_DATA_EVENT_CREATE,
    SYM_DATA_EVENT_BSH,
    SYM_DATA_EVENT_UNKNOWN,

} SymDataEventType;

#define SYM_DATA_EVENT_INSERT_CODE "I"
#define SYM_DATA_EVENT_UPDATE_CODE "U"
#define SYM_DATA_EVENT_DELETE_CODE "D"
#define SYM_DATA_EVENT_RELOAD_CODE "R"
#define SYM_DATA_EVENT_SQL_CODE "S"
#define SYM_DATA_EVENT_CREATE_CODE "C"
#define SYM_DATA_EVENT_BSH_CODE "B"

unsigned short SymDataEvent_isDml(SymDataEventType dataEventType);
SymDmlType SymDataEvent_getDmlType(SymDataEventType dataEventType);
SymDataEventType SymDataEvent_getEventType(char *code);
char * SymDataEvent_getCode(SymDataEventType dataEventType);

#endif /* SYM_IO_DATA_DATAEVENTTYPE_H */
