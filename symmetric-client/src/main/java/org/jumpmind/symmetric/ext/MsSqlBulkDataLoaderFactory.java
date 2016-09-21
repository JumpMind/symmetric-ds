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

import org.apache.commons.lang.StringEscapeUtils;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.JdbcUtils;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.io.MsSqlBulkDatabaseWriter;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.io.data.writer.Conflict;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterErrorHandler;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;
import org.jumpmind.symmetric.io.data.writer.ResolvedData;
import org.jumpmind.symmetric.io.data.writer.TransformWriter;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.load.IDataLoaderFactory;
import org.jumpmind.symmetric.service.IParameterService;
import org.springframework.jdbc.support.nativejdbc.NativeJdbcExtractor;

public class MsSqlBulkDataLoaderFactory implements IDataLoaderFactory {

    private NativeJdbcExtractor jdbcExtractor;
    private IStagingManager stagingManager;
    private IParameterService parameterService;

    public MsSqlBulkDataLoaderFactory(ISymmetricEngine engine) {
        this.jdbcExtractor = JdbcUtils.getNativeJdbcExtractory();
        this.stagingManager = engine.getStagingManager();
        this.parameterService = engine.getParameterService();
    }

    public String getTypeName() {
        return "mssql_bulk";
    }

    public IDataWriter getDataWriter(String sourceNodeId, ISymmetricDialect symmetricDialect, TransformWriter transformWriter,
            List<IDatabaseWriterFilter> filters, List<IDatabaseWriterErrorHandler> errorHandlers,
            List<? extends Conflict> conflictSettings, List<ResolvedData> resolvedData) {

        int maxRowsBeforeFlush = parameterService.getInt("mssql.bulk.load.max.rows.before.flush", 100000);
        boolean fireTriggers = parameterService.is("mssql.bulk.load.fire.triggers", false);
        String uncPath = parameterService.getString("mssql.bulk.load.unc.path");
        String rowTerminator = StringEscapeUtils.unescapeJava(parameterService.getString("mssql.bulk.load.row.terminator",
                "\\r\\n"));
        String fieldTerminator = StringEscapeUtils.unescapeJava(parameterService.getString("mssql.bulk.load.field.terminator",
                "||"));

        return new MsSqlBulkDatabaseWriter(symmetricDialect.getPlatform(), stagingManager, jdbcExtractor, maxRowsBeforeFlush,
                fireTriggers, uncPath, fieldTerminator, rowTerminator);
    }

    public boolean isPlatformSupported(IDatabasePlatform platform) {
        return (DatabaseNamesConstants.MSSQL2000.equals(platform.getName())
                || DatabaseNamesConstants.MSSQL2005.equals(platform.getName()) || DatabaseNamesConstants.MSSQL2008
                    .equals(platform.getName()));
    }

}
