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

import java.sql.ResultSet;
import java.sql.SQLException;

import org.jumpmind.db.sql.SymmetricLobHandler;
import org.springframework.jdbc.support.lob.DefaultLobHandler;

public class OracleLobHandler extends SymmetricLobHandler {

    DefaultLobHandler longHandler = new DefaultLobHandler();

    public OracleLobHandler() {
        super(new DefaultLobHandler());
    }

    @Override
    public String getClobAsString(ResultSet rs, int columnIndex, int jdbcTypeCode,
            String jdbcTypeName) throws SQLException {
        if ("LONG".equalsIgnoreCase(jdbcTypeName)) {
            /**
             * Ironically, the Oracle Lob Handler doesn't handle the Oracle
             * specific data type of Long. We should probably swap out the
             * Oracle Lob Handler altogether but I haven't been able to get it
             * to insert Empty Clob Values appropriately yet.
             */
            return longHandler.getClobAsString(rs, columnIndex);
        } else {
            return super.getClobAsString(rs, columnIndex, jdbcTypeCode, jdbcTypeName);
        }
    }

}
