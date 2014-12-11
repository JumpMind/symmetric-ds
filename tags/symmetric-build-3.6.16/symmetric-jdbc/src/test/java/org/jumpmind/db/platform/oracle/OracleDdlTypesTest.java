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
package org.jumpmind.db.platform.oracle;

import org.jumpmind.db.AbstractDdlTypesTest;
import org.jumpmind.db.platform.DatabaseNamesConstants;

public class OracleDdlTypesTest extends AbstractDdlTypesTest {

    @Override
    protected String getName() {
        return DatabaseNamesConstants.ORACLE;
    }

    @Override
    protected String[] getDdlTypes() {
        return new String[] { "nchar(5)", "nvarchar2(1000)", "varchar2(100)", "number(*,2)",
                "binary_float", "binary_double", "date", "timestamp with time zone",
                "timestamp with local time zone", "nclob", "rowid", "xmltype", "integer" };
    }

}
