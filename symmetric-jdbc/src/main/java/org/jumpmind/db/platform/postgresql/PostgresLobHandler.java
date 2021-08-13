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
package org.jumpmind.db.platform.postgresql;

import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.jumpmind.db.sql.SymmetricLobHandler;

public class PostgresLobHandler extends SymmetricLobHandler {
    public PostgresLobHandler() {
        super();
    }

    public boolean needsAutoCommitFalseForBlob(int jdbcTypeCode, String jdbcTypeName) {
        return PostgreSqlDatabasePlatform.isBlobStoredByReference(jdbcTypeName);
    }

    public byte[] getBlobAsBytes(ResultSet rs, int columnIndex, int jdbcTypeCode,
            String jdbcTypeName) throws SQLException {
        if (PostgreSqlDatabasePlatform.isBlobStoredByReference(jdbcTypeName)) {
            return getLoColumnAsBytes(rs, columnIndex);
        } else {
            return getDefaultHandler().getBlobAsBytes(rs, columnIndex);
        }
    }

    public static byte[] getLoColumnAsBytes(ResultSet rs, int columnIndex) throws SQLException {
        Blob blob = rs.getBlob(columnIndex);
        if (blob != null) {
            return blob.getBytes(1, (int) blob.length());
        } else {
            return null;
        }
    }
}
