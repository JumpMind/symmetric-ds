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
package org.jumpmind.symmetric.ext;

import java.util.List;

import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.JdbcUtils;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.ext.ISymmetricEngineAware;
import org.jumpmind.symmetric.io.PostgresBulkDatabaseWriter;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.io.data.writer.Conflict;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterErrorHandler;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;
import org.jumpmind.symmetric.io.data.writer.ResolvedData;
import org.jumpmind.symmetric.io.data.writer.TransformWriter;
import org.jumpmind.symmetric.load.IDataLoaderFactory;
import org.springframework.jdbc.support.nativejdbc.NativeJdbcExtractor;

public class PostgresBulkDataLoaderFactory implements IDataLoaderFactory, ISymmetricEngineAware,
        IBuiltInExtensionPoint {

    private int maxRowsBeforeFlush;
    private NativeJdbcExtractor jdbcExtractor;

    public PostgresBulkDataLoaderFactory() {
        this.jdbcExtractor = JdbcUtils.getNativeJdbcExtractory();
    }

    public String getTypeName() {
        return "postgres_bulk";
    }

    public IDataWriter getDataWriter(String sourceNodeId, ISymmetricDialect symmetricDialect,
            TransformWriter transformWriter, List<IDatabaseWriterFilter> filters,
            List<IDatabaseWriterErrorHandler> errorHandlers,
            List<? extends Conflict> conflictSettings, List<ResolvedData> resolvedData) {
        return new PostgresBulkDatabaseWriter(symmetricDialect.getPlatform(), jdbcExtractor,
                maxRowsBeforeFlush);
    }

    public void setSymmetricEngine(ISymmetricEngine engine) {
        this.maxRowsBeforeFlush = engine.getParameterService().getInt(
                "postgres.bulk.load.max.rows.before.flush", 10000);
    }

    public boolean isPlatformSupported(IDatabasePlatform platform) {
        return DatabaseNamesConstants.POSTGRESQL.equals(platform.getName()) || 
        		DatabaseNamesConstants.GREENPLUM.equals(platform.getName());
    }

}
