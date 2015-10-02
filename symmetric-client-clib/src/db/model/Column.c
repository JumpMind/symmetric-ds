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
#include "db/model/Column.h"

char * SymColumn_toString(SymColumn *this) {
    SymStringBuilder *sb = SymStringBuilder_newWithString("Column [name=");
    sb->append(sb, this->name);
    sb->append(sb, "; type=")->appendf(sb, "%d", this->sqlType);
    sb->append(sb, "; required=")->appendf(sb, "%d", this->isRequired);
    sb->append(sb, "; pk=")->appendf(sb, "%d", this->isPrimaryKey);
    sb->append(sb, "]");
    return sb->destroyAndReturn(sb);
}

void SymColumn_destroy(SymColumn *this) {
    free(this->name);
    free(this);
}

SymColumn * SymColumn_new(SymColumn *this, char *name, unsigned short isPrimaryKey) {
    if (this == NULL) {
        this = (SymColumn *) calloc(1, sizeof(SymColumn));
    }
    this->name = SymStringBuilder_copy(name);
    this->isPrimaryKey = isPrimaryKey;
    this->toString = (void *) &SymColumn_toString;
    this->destroy = (void *) &SymColumn_destroy;
    return this;
}
