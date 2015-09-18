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
#include "service/NodeService.h"

SymNode * SymNodeService_find_identity(SymNodeService *this) {
    printf("SymNodeService_find_identity\n");
    return NULL;
}

SymNodeSecurity * SymNodeService_find_node_security(SymNodeService *this, char *nodeId) {
    printf("SymNodeService_find_node_security\n");
    return NULL;
}

SymNode ** SymNodeService_find_nodes_to_pull(SymNodeService *this) {
    printf("SymNodeService_find_nodes_to_pull\n");
    return NULL;
}

SymNode ** SymNodeService_find_nodes_to_push_to(SymNodeService *this) {
    printf("SymNodeService_find_nodes_to_push_to\n");
    return NULL;
}

int SymNodeService_is_dataload_started(SymNodeService *this) {
    printf("SymNodeService_is_dataload_started\n");
    return 0;
}

void SymNodeService_destroy(SymNodeService *this) {
    free(this);
}

SymNodeService * SymNodeService_new(SymNodeService *this) {
    if (this == NULL) {
        this = (SymNodeService *) calloc(1, sizeof(SymNodeService));
    }
    this->find_identity = (void *) &SymNodeService_find_identity;
    this->find_node_security = (void *) &SymNodeService_find_node_security;
    this->find_nodes_to_pull = (void *) &SymNodeService_find_nodes_to_pull;
    this->find_nodes_to_push_to = (void *) &SymNodeService_find_nodes_to_push_to;
    this->is_dataload_started = (void *) &SymNodeService_is_dataload_started;
    this->destroy = (void *) &SymNodeService_destroy;
    return this;
}
