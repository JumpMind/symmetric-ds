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
#include "common/TableConstants.h"

SymList * SymTableConstants_getConfigTables() {
    SymList *list = SymList_new(NULL);
    list->add(list, SYM_NODE_GROUP);
    list->add(list, SYM_NODE_GROUP_LINK);
    list->add(list, SYM_NODE);
    list->add(list, SYM_NODE_HOST);
    list->add(list, SYM_NODE_SECURITY);
    list->add(list, SYM_PARAMETER);
    list->add(list, SYM_CHANNEL);
    list->add(list, SYM_TRIGGER);
    list->add(list, SYM_ROUTER);
    list->add(list, SYM_TRIGGER_ROUTER);
    list->add(list, SYM_NODE_IDENTITY);
    list->add(list, SYM_FILE_TRIGGER);
    list->add(list, SYM_FILE_TRIGGER_ROUTER);
    list->add(list, SYM_FILE_SNAPSHOT);
    return list;
}

SymList * SymTableConstants_getTablesThatDoNotSync() {
    SymList *list = SymList_new(NULL);
    list->add(list, SYM_NODE_IDENTITY);
    list->add(list, SYM_NODE_CHANNEL_CTL);
    return list;
}
