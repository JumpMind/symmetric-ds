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
#include "model/NodeGroupLinkAction.h"

SymNodeGroupLinkAction SymNodeGroupLinkAction_fromCode(char *code) {
    if (SymStringUtils_equals(code, SYM_NODE_GROUP_LINK_ACTION_PUSH)) {
        return SymNodeGroupLinkAction_P;
    } else if (SymStringUtils_equals(code, SYM_NODE_GROUP_LINK_ACTION_WAIT_FOR_PULL)) {
        return SymNodeGroupLinkAction_W;
    } else if (SymStringUtils_equals(code, SYM_NODE_GROUP_LINK_ACTION_ROUTE)) {
        return SymNodeGroupLinkAction_R;
    }
    return -1;
}

char * SymNodeGroupLinkAction_toString(SymNodeGroupLinkAction nodeGroupLinkAction) {
    switch (nodeGroupLinkAction) {
    case SymNodeGroupLinkAction_P:
        return SYM_NODE_GROUP_LINK_ACTION_PUSH;
    case SymNodeGroupLinkAction_W:
        return SYM_NODE_GROUP_LINK_ACTION_WAIT_FOR_PULL;
    case SymNodeGroupLinkAction_R:
        return SYM_NODE_GROUP_LINK_ACTION_ROUTE;
    }
    return NULL;
}
