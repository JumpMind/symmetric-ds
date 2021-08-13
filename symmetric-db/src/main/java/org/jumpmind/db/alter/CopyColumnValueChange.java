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
package org.jumpmind.db.alter;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;

public class CopyColumnValueChange extends TableChangeImplBase {
    private Column sourceColumn;
    private Column targetColumn;

    public CopyColumnValueChange(Table table, Column sourceColumn, Column targetColumn) {
        super(table);
        this.sourceColumn = sourceColumn;
        this.targetColumn = targetColumn;
    }

    public void apply(Database database, boolean caseSensitive) {
        // nothing to do. structure hasn't changed.
    }

    public Column getSourceColumn() {
        return sourceColumn;
    }

    public Column getTargetColumn() {
        return targetColumn;
    }
}
