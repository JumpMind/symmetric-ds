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
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.io.MySqlBulkDatabaseWriter;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.io.data.writer.Conflict;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterErrorHandler;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;
import org.jumpmind.symmetric.io.data.writer.ResolvedData;
import org.jumpmind.symmetric.io.data.writer.TransformWriter;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.load.AbstractDataLoaderFactory;
import org.jumpmind.symmetric.load.IDataLoaderFactory;

public class MySqlBulkDataLoaderFactory extends AbstractDataLoaderFactory implements IDataLoaderFactory {
    private IStagingManager stagingManager;

    public MySqlBulkDataLoaderFactory(ISymmetricEngine engine) {
        this.stagingManager = engine.getStagingManager();
        this.parameterService = engine.getParameterService();
    }

    public String getTypeName() {
        return "mysql_bulk";
    }

    public IDataWriter getDataWriter(String sourceNodeId, ISymmetricDialect symmetricDialect,
            TransformWriter transformWriter,
            List<IDatabaseWriterFilter> filters, List<IDatabaseWriterErrorHandler> errorHandlers,
            List<? extends Conflict> conflictSettings, List<ResolvedData> resolvedData) {
        int maxRowsBeforeFlush = parameterService.getInt(ParameterConstants.MYSQL_BULK_LOAD_MAX_ROWS_BEFORE_FLUSH, 100000);
        long maxBytesBeforeFlush = parameterService.getLong(ParameterConstants.MYSQL_BULK_LOAD_MAX_BYTES_BEFORE_FLUSH, 1000000000);
        boolean isLocal = Boolean.parseBoolean(parameterService.getString(ParameterConstants.MYSQL_BULK_LOAD_LOCAL, "true"));
        boolean isReplace = Boolean.parseBoolean(parameterService.getString(ParameterConstants.MYSQL_BULK_LOAD_REPLACE, "false"));
        return new MySqlBulkDatabaseWriter(symmetricDialect.getPlatform(), symmetricDialect.getTargetPlatform(), symmetricDialect.getTablePrefix(),
                stagingManager,
                maxRowsBeforeFlush,
                maxBytesBeforeFlush, isLocal, isReplace, buildParameterDatabaseWritterSettings());
    }

    public boolean isPlatformSupported(IDatabasePlatform platform) {
        return DatabaseNamesConstants.MYSQL.equals(platform.getName());
    }
}
