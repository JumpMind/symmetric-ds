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
package org.jumpmind.db.platform.h2;

import org.jumpmind.db.AbstractDdlTypesTest;
import org.jumpmind.db.platform.DatabaseNamesConstants;

public class H2DdlTypesTest extends AbstractDdlTypesTest {
    @Override
    protected String getName() {
        return DatabaseNamesConstants.H2;
    }

    @Override
    protected String[] getDdlTypes() {
        return new String[] { "VARCHAR(55)", "UUID", "VARCHAR_IGNORECASE(100)", "VARCHAR ARRAY",
                "char(10)", "binary(10)", "varbinary(10)", "blob",
                "boolean", "tinyint", "smallint", "integer", "bigint", "numeric", "real", "double precision", "decfloat",
                "date", "time", "timestamp" };
    }
}
