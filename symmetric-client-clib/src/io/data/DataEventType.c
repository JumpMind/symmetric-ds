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
#include "io/data/DataEventType.h"
#include "common/Log.h"

unsigned short SymDataEvent_isDml(SymDataEventType dataEventType) {
    return dataEventType == SYM_DATA_EVENT_INSERT
            || dataEventType == SYM_DATA_EVENT_DELETE
            || dataEventType == SYM_DATA_EVENT_UPDATE;

}

SymDmlType SymDataEvent_getDmlType(SymDataEventType dataEventType) {
    switch (dataEventType) {
     case SYM_DATA_EVENT_INSERT:
         return SYM_DML_TYPE_INSERT;
     case SYM_DATA_EVENT_UPDATE:
         return SYM_DML_TYPE_UPDATE;
     case SYM_DATA_EVENT_DELETE:
         return SYM_DML_TYPE_DELETE;
     default:
         return SYM_DML_TYPE_UNKNOWN;
     }
}

SymDataEventType SymDataEvent_getEventType(char *code) {
    if (code != NULL) {
        if (SymStringUtils_equals(code, SYM_DATA_EVENT_INSERT_CODE)) {
            return SYM_DATA_EVENT_INSERT;
        } else if (SymStringUtils_equals(code, SYM_DATA_EVENT_UPDATE_CODE)) {
            return SYM_DATA_EVENT_UPDATE;
        } else if (SymStringUtils_equals(code, SYM_DATA_EVENT_DELETE_CODE)) {
            return SYM_DATA_EVENT_DELETE;
        } else if (SymStringUtils_equals(code, SYM_DATA_EVENT_RELOAD_CODE)) {
            return SYM_DATA_EVENT_RELOAD;
        } else if (SymStringUtils_equals(code, SYM_DATA_EVENT_SQL_CODE)) {
            return SYM_DATA_EVENT_SQL;
        } else if (SymStringUtils_equals(code, SYM_DATA_EVENT_CREATE_CODE)) {
            return SYM_DATA_EVENT_CREATE;
        } else if (SymStringUtils_equals(code, SYM_DATA_EVENT_BSH_CODE)) {
            return SYM_DATA_EVENT_BSH;
        }
    }

    SymLog_error("Invalid data event type of %s", code);

    return SYM_DATA_EVENT_UNKNOWN;
}

char * SymDataEvent_getCode(SymDataEventType dataEventType) {
    switch (dataEventType) {
    case SYM_DATA_EVENT_INSERT:
        return SYM_DATA_EVENT_INSERT_CODE;
    case SYM_DATA_EVENT_UPDATE:
        return SYM_DATA_EVENT_UPDATE_CODE;
    case SYM_DATA_EVENT_DELETE:
        return SYM_DATA_EVENT_DELETE_CODE;
    case SYM_DATA_EVENT_RELOAD:
        return SYM_DATA_EVENT_RELOAD_CODE;
    case SYM_DATA_EVENT_SQL:
        return SYM_DATA_EVENT_SQL_CODE;
    case SYM_DATA_EVENT_CREATE:
        return SYM_DATA_EVENT_CREATE_CODE;
    case SYM_DATA_EVENT_BSH:
        return SYM_DATA_EVENT_BSH_CODE;
    default:
        SymLog_error("Invalid data event type of %d", dataEventType);
    }
    return "UNKNOWN";
}
